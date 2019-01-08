// Copyright 2017 Istio Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"context"
	"errors"
	"reflect"
	"time"

	"golang.org/x/time/rate"

	"istio.io/istio/pkg/log"
)

// Agent manages the restarts and the life cycle of a proxy binary.  Agent
// keeps track of all running proxy epochs and their configurations.  Hot
// restarts are performed by launching a new proxy process with a strictly
// incremented restart epoch. It is up to the proxy to ensure that older epochs
// gracefully shutdown and carry over all the necessary state to the latest
// epoch.  The agent does not terminate older epochs. The initial epoch is 0.
// Agent管理proxy的重启以及生命周期，Agent跟踪所有正在运行的proxy的epochs和配置
// hot restart用重启一个新的proxy进程实现，该进程有着严格增加的restart epoch
// 这取决于proxy来确保老的epochs会逐渐关闭并且将所有必要的状态转移到最新的epoch
// agent不会关闭老的epochs，初始的epoch为0
//
// 这个重启协议和Envoy的语义是相符的
// The restart protocol matches Envoy semantics for restart epochs: to
// successfully launch a new Envoy process that will replace the running Envoy
// processes, the restart epoch of the new process must be exactly 1 greater
// than the highest restart epoch of the currently running Envoy processes.
// See https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/hot_restart.html
// for more information about the Envoy hot restart protocol.
//
// Agent requires two functions "run" and "cleanup". Run function is a call to
// start the proxy and must block until the proxy exits. Cleanup function is
// executed immediately after the proxy exits and must be non-blocking since it
// is executed synchronously in the main agent control loop. Both functions
// take the proxy epoch as an argument. A typical scenario would involve epoch
// 0 followed by a failed epoch 1 start. The agent then attempts to start epoch
// 1 again.
// Agent需要"run"和"cleanup"两个函数，Run用于启动proxy并且阻塞直到proxy退出
// Cleanup函数在proxy退出之后立即执行并且必须是非阻塞的，因为它在main agent的控制循环中同步执行
// 两个函数都将proxy epoch作为参数。一个典型的场景是epoch 0之后启动了一个失败的epoch 1
// agent会尝试再次启动epoch 1
//
// Whenever the run function returns an error, the agent assumes that the proxy
// failed to start and attempts to restart the proxy several times with an
// exponential back-off. The subsequent restart attempts may reuse the epoch
// from the failed attempt. Retry budgets are allocated whenever the desired
// configuration changes.
// 当run函数返回一个error时，agent假设proxy启动失败，因此会以指数回退的方式多次尝试重启proxy
// 之后的重启可能重用failed attempt的epoch，当期望的配置改变时，retry budgets会重新分配
//
// Agent executes a single control loop that receives notifications about
// scheduled configuration updates, exits from older proxy epochs, and retry
// attempt timers. The call to schedule a configuration update will block until
// the control loop is ready to accept and process the configuration update.
// Agent执行单个的控制循环，等待接受配置更新的通知，从老的proxy epochs退出，并且多次重试
// 对于一个配置更新的调度会阻塞直到控制循环准备好接受并且处理配置更新
type Agent interface {
	// ScheduleConfigUpdate sets the desired configuration for the proxy.  Agent
	// compares the current active configuration to the desired state and
	// initiates a restart if necessary. If the restart fails, the agent attempts
	// to retry with an exponential back-off.
	// ScheduleConfigUpdate将proxy设置为指定配置
	// Agent将当前的配置和期望的配置进行对比并且有必要的话重启
	// 如果重启失败，则agent以指数回退的方式重试
	ScheduleConfigUpdate(config interface{})

	// Run starts the agent control loop and awaits for a signal on the input
	// channel to exit the loop.
	// Run启动agent的控制循环并且在input channel等待signal用以推出循环
	Run(ctx context.Context)
}

var (
	errAbort = errors.New("epoch aborted")

	// DefaultRetry configuration for proxies
	DefaultRetry = Retry{
		MaxRetries:      10,
		InitialInterval: 200 * time.Millisecond,
	}
)

const (
	// MaxAborts is the maximum number of cascading abort messages to buffer.
	// This should be the upper bound on the number of proxies available at any point in time.
	MaxAborts = 10
)

// NewAgent creates a new proxy agent for the proxy start-up and clean-up functions.
// NewAgent场景一个新的proxy agent用于proxy的start-up和clean-up功能
func NewAgent(proxy Proxy, retry Retry) Agent {
	return &agent{
		proxy:    proxy,
		retry:    retry,
		epochs:   make(map[int]interface{}),
		configCh: make(chan interface{}),
		statusCh: make(chan exitStatus),
		abortCh:  make(map[int]chan error),
	}
}

// Retry configuration for the proxy
type Retry struct {
	// restart is the timestamp of the next scheduled restart attempt
	restart *time.Time

	// number of times to attempts left to retry applying the latest desired configuration
	// 应用最新的目标配置所剩的重试次数
	budget int

	// MaxRetries is the maximum number of retries
	MaxRetries int

	// InitialInterval is the delay between the first restart, from then on it is
	// multiplied by a factor of 2 for each subsequent retry
	// 第一次重试的间隔时间，从此之后，每次后续的重试都将乘以二
	InitialInterval time.Duration
}

// Proxy defines command interface for a proxy
type Proxy interface {
	// Run command for a config, epoch, and abort channel
	Run(interface{}, int, <-chan error) error

	// Cleanup command for an epoch
	Cleanup(int)

	// Panic command is invoked with the desired config when all retries to
	// start the proxy fail just before the agent terminating
	// Panic命令会在所有对于启动proxy的尝试都失败之后被调用，在agent终止之前
	Panic(interface{})
}

type agent struct {
	// proxy commands
	// proxy的接口
	proxy Proxy

	// retry configuration
	retry Retry

	// desired configuration state
	// 期望的配置状态
	desiredConfig interface{}

	// active epochs and their configurations
	// 活跃的epochs以及它们的配置
	epochs map[int]interface{}

	// current configuration is the highest epoch configuration
	// current是最高epoch的配置
	currentConfig interface{}

	// channel for posting desired configurations
	// 用于发布目标配置的channel
	configCh chan interface{}

	// channel for proxy exit notifications
	// 用于发布proxy终止通知的channel
	statusCh chan exitStatus

	// channel for aborting running instances
	// 用于终止运行实例的channel
	abortCh map[int]chan error
}

type exitStatus struct {
	epoch int
	err   error
}

func (a *agent) ScheduleConfigUpdate(config interface{}) {
	// 将config通过agent.configCh发布
	a.configCh <- config
}

func (a *agent) Run(ctx context.Context) {
	log.Info("Starting proxy agent")

	// Throttle processing up to smoothed 1 qps with bursts up to 10 qps.
	// High QPS is needed to process messages on all channels.
	rateLimiter := rate.NewLimiter(1, 10)

	for {
		err := rateLimiter.Wait(ctx)
		if err != nil {
			a.terminate()
			return
		}

		// maximum duration or duration till next restart
		var delay time.Duration = 1<<63 - 1
		if a.retry.restart != nil {
			delay = time.Until(*a.retry.restart)
		}

		select {
		case config := <-a.configCh:
			if !reflect.DeepEqual(a.desiredConfig, config) {
				log.Infof("Received new config, resetting budget")
				// 将agent.desiredConfig设置为最新的config
				a.desiredConfig = config

				// reset retry budget if and only if the desired config changes
				// 当目标配置改变时，重置retry budget
				a.retry.budget = a.retry.MaxRetries
				// 用新的配置重启proxy
				a.reconcile()
			}

		case status := <-a.statusCh:
			// delete epoch record and update current config
			// avoid self-aborting on non-abort error
			// 删除epoch record并且更新当前的config
			delete(a.epochs, status.epoch)
			delete(a.abortCh, status.epoch)
			// 将当前的config设置为最新的epoch对应的config
			a.currentConfig = a.epochs[a.latestEpoch()]

			if status.err == errAbort {
				log.Infof("Epoch %d aborted", status.epoch)
			} else if status.err != nil {
				log.Warnf("Epoch %d terminated with an error: %v", status.epoch, status.err)

				// NOTE: due to Envoy hot restart race conditions, an error from the
				// process requires aggressive non-graceful restarts by killing all
				// existing proxy instances
				// NOTE: 因为Envoy热重启的race conditions，当从proxy进程返回一个error时
				// 就需要采取将所有已经存在的proxy进程都杀死的激进而不优雅的方式
				a.abortAll()
			} else {
				log.Infof("Epoch %d exited normally", status.epoch)
			}

			// cleanup for the epoch
			// 为相应的epoch调用cleanup函数
			a.proxy.Cleanup(status.epoch)

			// schedule a retry for an error.
			// the current config might be out of date from here since its proxy might have been aborted.
			// the current config will change on abort, hence retrying prior to abort will not progress.
			// that means that aborted envoy might need to re-schedule a retry if it was not already scheduled.
			if status.err != nil {
				// skip retrying twice by checking retry restart delay
				if a.retry.restart == nil {
					if a.retry.budget > 0 {
						delayDuration := a.retry.InitialInterval * (1 << uint(a.retry.MaxRetries-a.retry.budget))
						restart := time.Now().Add(delayDuration)
						a.retry.restart = &restart
						a.retry.budget = a.retry.budget - 1
						log.Infof("Epoch %d: set retry delay to %v, budget to %d", status.epoch, delayDuration, a.retry.budget)
					} else {
						log.Error("Permanent error: budget exhausted trying to fulfill the desired configuration")
						a.proxy.Panic(status.epoch)
						return
					}
				} else {
					log.Debugf("Epoch %d: restart already scheduled", status.epoch)
				}
			}

		case <-time.After(delay):
			// 到达重试时间后，调用reconcile()进行重试
			a.reconcile()

		case _, more := <-ctx.Done():
			if !more {
				a.terminate()
				return
			}
		}
	}
}

func (a *agent) terminate() {
	log.Infof("Agent terminating")
	a.abortAll()
}

func (a *agent) reconcile() {
	// cancel any scheduled restart
	a.retry.restart = nil

	log.Infof("Reconciling configuration (budget %d)", a.retry.budget)

	// check that the config is current
	if reflect.DeepEqual(a.desiredConfig, a.currentConfig) {
		log.Infof("Desired configuration is already applied")
		return
	}

	// discover and increment the latest running epoch
	// 发现并且增加最新的running epoch
	epoch := a.latestEpoch() + 1
	// buffer aborts to prevent blocking on failing proxy
	// 对aborts进行缓存从而防止阻塞在failing proxy
	abortCh := make(chan error, MaxAborts)
	a.epochs[epoch] = a.desiredConfig
	a.abortCh[epoch] = abortCh
	a.currentConfig = a.desiredConfig
	// 调用a.proxy.Run，启动proxy
	go a.waitForExit(a.desiredConfig, epoch, abortCh)
}

// waitForExit runs the start-up command as a go routine and waits for it to finish
// waitForExit以一个新的goroutine运行start-up命令并且等待它结束
func (a *agent) waitForExit(config interface{}, epoch int, abortCh <-chan error) {
	log.Infof("Epoch %d starting", epoch)
	err := a.proxy.Run(config, epoch, abortCh)
	a.statusCh <- exitStatus{epoch: epoch, err: err}
}

// latestEpoch returns the latest epoch, or -1 if no epoch is running
// latestEpoch返回最新的epoch，或者返回-1，如果没有epoch处于running状态
func (a *agent) latestEpoch() int {
	epoch := -1
	for active := range a.epochs {
		if active > epoch {
			epoch = active
		}
	}
	return epoch
}

// abortAll sends abort error to all proxies
// abortAll将abort error发送给所有的proxies
func (a *agent) abortAll() {
	for epoch, abortCh := range a.abortCh {
		log.Warnf("Aborting epoch %d...", epoch)
		abortCh <- errAbort
	}
	log.Warnf("Aborted all epochs")
}
