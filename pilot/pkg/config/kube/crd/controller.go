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

package crd

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"

	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pilot/pkg/serviceregistry/kube"
	"istio.io/istio/pkg/log"
)

// controller is a collection of synchronized resource watchers.
// Caches are thread-safe
// controller是一个同步的resource watchers的集合
// Caches是线程安全的
type controller struct {
	client *Client
	queue  kube.Queue
	kinds  map[string]cacheHandler
}

type cacheHandler struct {
	informer cache.SharedIndexInformer
	// ChainHandler线性调用注册的handler
	handler  *kube.ChainHandler
}

var (
	// experiment on getting some monitoring on config errors.
	k8sEvents = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pilot_k8s_cfg_events",
		Help: "Events from k8s config.",
	}, []string{"type", "event"})

	k8sErrors = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pilot_k8s_object_errors",
		Help: "Errors converting k8s CRDs",
	}, []string{"name"})

	// InvalidCRDs contains a sync.Map keyed by the namespace/name of the entry, and has the error as value.
	// It can be used by tools like ctrlz to display the errors.
	InvalidCRDs atomic.Value
)

func init() {
	prometheus.MustRegister(k8sEvents)
}

// NewController creates a new Kubernetes controller for CRDs
// Use "" for namespace to listen for all namespace changes
// NewController创建一个新的Kubernetes controller
// 如果namespace为""则监听所有的namespace更新
func NewController(client *Client, options kube.ControllerOptions) model.ConfigStoreCache {
	log.Infof("CRD controller watching namespaces %q", options.WatchedNamespace)

	// Queue requires a time duration for a retry delay after a handler error
	// Queue需要一个时间间隔用于在一个handler错误之后进行的retry delay
	out := &controller{
		client: client,
		queue:  kube.NewQueue(1 * time.Second),
		kinds:  make(map[string]cacheHandler),
	}

	// add stores for CRD kinds
	// 为各种CRD创建stores
	for _, schema := range client.ConfigDescriptor() {
		out.addInformer(schema, options.WatchedNamespace, options.ResyncPeriod)
	}

	return out
}

func (c *controller) addInformer(schema model.ProtoSchema, namespace string, resyncPeriod time.Duration) {
	// 为各个类型创建informer
	// 倒数第二个参数为List函数，倒数第一个函数为Watch函数
	c.kinds[schema.Type] = c.createInformer(knownTypes[schema.Type].object.DeepCopyObject(), schema.Type, resyncPeriod,
		func(opts meta_v1.ListOptions) (result runtime.Object, err error) {
			result = knownTypes[schema.Type].collection.DeepCopyObject()
			rc, ok := c.client.clientset[apiVersion(&schema)]
			if !ok {
				return nil, fmt.Errorf("client not initialized %s", schema.Type)
			}
			req := rc.dynamic.Get().
				Resource(ResourceName(schema.Plural)).
				VersionedParams(&opts, meta_v1.ParameterCodec)

			if !schema.ClusterScoped {
				req = req.Namespace(namespace)
			}
			err = req.Do().Into(result)
			return
		},
		func(opts meta_v1.ListOptions) (watch.Interface, error) {
			rc, ok := c.client.clientset[apiVersion(&schema)]
			if !ok {
				return nil, fmt.Errorf("client not initialized %s", schema.Type)
			}
			req := rc.dynamic.Get().
				Prefix("watch").
				Resource(ResourceName(schema.Plural)).
				VersionedParams(&opts, meta_v1.ParameterCodec)
			if !schema.ClusterScoped {
				req = req.Namespace(namespace)
			}
			return req.Watch()
		})
}

// notify is the first handler in the handler chain.
// Returning an error causes repeated execution of the entire chain.
// notify是handler chain中的第一个handler
// 返回error会导致整个handler chain的重新执行
func (c *controller) notify(obj interface{}, event model.Event) error {
	if !c.HasSynced() {
		// 等待直到全部完成
		return errors.New("waiting till full synchronization")
	}
	_, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		log.Infof("Error retrieving key: %v", err)
	}
	return nil
}

func (c *controller) createInformer(
	o runtime.Object,
	otype string,
	resyncPeriod time.Duration,
	lf cache.ListFunc,
	wf cache.WatchFunc) cacheHandler {
	handler := &kube.ChainHandler{}
	// 在hanler中增加c.notify函数
	handler.Append(c.notify)

	// TODO: finer-grained index (perf)
	// 调用k8s库，创建informer
	informer := cache.NewSharedIndexInformer(
		&cache.ListWatch{ListFunc: lf, WatchFunc: wf}, o,
		resyncPeriod, cache.Indexers{})

	informer.AddEventHandler(
		// 创建事件处理函数
		cache.ResourceEventHandlerFuncs{
			// TODO: filtering functions to skip over un-referenced resources (perf)
			// 增加过滤函数用于跳过无关的资源
			AddFunc: func(obj interface{}) {
				k8sEvents.With(prometheus.Labels{"type": otype, "event": "add"}).Add(1)
				// 将从k8s获取的事件推送到c.queue中
				// 此处的handler就是各个类型对应的handler
				c.queue.Push(kube.NewTask(handler.Apply, obj, model.EventAdd))
			},
			UpdateFunc: func(old, cur interface{}) {
				if !reflect.DeepEqual(old, cur) {
					k8sEvents.With(prometheus.Labels{"type": otype, "event": "update"}).Add(1)
					c.queue.Push(kube.NewTask(handler.Apply, cur, model.EventUpdate))
				} else {
					k8sEvents.With(prometheus.Labels{"type": otype, "event": "updateSame"}).Add(1)
				}
			},
			DeleteFunc: func(obj interface{}) {
				k8sEvents.With(prometheus.Labels{"type": otype, "event": "add"}).Add(1)
				c.queue.Push(kube.NewTask(handler.Apply, obj, model.EventDelete))
			},
		})

	return cacheHandler{informer: informer, handler: handler}
}

func (c *controller) RegisterEventHandler(typ string, f func(model.Config, model.Event)) {
	schema, exists := c.ConfigDescriptor().GetByType(typ)
	if !exists {
		return
	}
	c.kinds[typ].handler.Append(func(object interface{}, ev model.Event) error {
		item, ok := object.(IstioObject)
		if ok {
			config, err := ConvertObject(schema, item, c.client.domainSuffix)
			if err != nil {
				log.Warnf("error translating object for schema %#v : %v\n Object:\n%#v", schema, err, object)
			} else {
				// 调用处理函数进行处理
				f(*config, ev)
			}
		}
		return nil
	})
}

func (c *controller) HasSynced() bool {
	// 遍历各个类型，检测是否同步完成
	for kind, ctl := range c.kinds {
		if !ctl.informer.HasSynced() {
			log.Infof("controller %q is syncing...", kind)
			return false
		}
	}
	return true
}

func (c *controller) Run(stop <-chan struct{}) {
	// 运行queue
	go c.queue.Run(stop)

	// 运行各个handler
	for _, ctl := range c.kinds {
		go ctl.informer.Run(stop)
	}

	<-stop
	log.Info("controller terminated")
}

func (c *controller) ConfigDescriptor() model.ConfigDescriptor {
	return c.client.ConfigDescriptor()
}

func (c *controller) Get(typ, name, namespace string) (*model.Config, bool) {
	schema, exists := c.client.ConfigDescriptor().GetByType(typ)
	if !exists {
		return nil, false
	}

	// 找到对应类型的informer的store
	store := c.kinds[typ].informer.GetStore()
	data, exists, err := store.GetByKey(kube.KeyFunc(name, namespace))
	if !exists {
		return nil, false
	}
	if err != nil {
		log.Warna(err)
		return nil, false
	}

	obj, ok := data.(IstioObject)
	if !ok {
		log.Warn("Cannot convert to config from store")
		return nil, false
	}

	// 将IstioObject类型的对象转换为model.Config
	config, err := ConvertObject(schema, obj, c.client.domainSuffix)
	if err != nil {
		return nil, false
	}

	return config, true
}

func (c *controller) Create(config model.Config) (string, error) {
	return c.client.Create(config)
}

func (c *controller) Update(config model.Config) (string, error) {
	return c.client.Update(config)
}

func (c *controller) Delete(typ, name, namespace string) error {
	return c.client.Delete(typ, name, namespace)
}

func (c *controller) List(typ, namespace string) ([]model.Config, error) {
	schema, ok := c.client.ConfigDescriptor().GetByType(typ)
	if !ok {
		return nil, fmt.Errorf("missing type %q", typ)
	}

	var newErrors sync.Map
	var errs error
	out := make([]model.Config, 0)
	oldMap := InvalidCRDs.Load()
	if oldMap != nil {
		oldMap.(*sync.Map).Range(func(key, value interface{}) bool {
			k8sErrors.With(prometheus.Labels{"name": key.(string)}).Set(1)
			return true
		})
	}
	for _, data := range c.kinds[typ].informer.GetStore().List() {
		item, ok := data.(IstioObject)
		if !ok {
			continue
		}

		if namespace != "" && namespace != item.GetObjectMeta().Namespace {
			continue
		}

		config, err := ConvertObject(schema, item, c.client.domainSuffix)
		if err != nil {
			key := item.GetObjectMeta().Namespace + "/" + item.GetObjectMeta().Name
			log.Errorf("Failed to convert %s object, ignoring: %s %v %v", typ, key, err, item.GetSpec())
			// DO NOT RETURN ERROR: if a single object is bad, it'll be ignored (with a log message), but
			// the rest should still be processed.
			// TODO: find a way to reset and represent the error !!
			newErrors.Store(key, err)
			k8sErrors.With(prometheus.Labels{"name": key}).Set(1)
		} else {
			out = append(out, *config)
		}
	}
	InvalidCRDs.Store(&newErrors)
	return out, errs
}
