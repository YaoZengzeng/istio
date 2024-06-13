// Copyright Istio Authors
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

package ambient

import (
	"errors"
	"fmt"
	"io"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"istio.io/istio/pkg/config/constants"
	istioKube "istio.io/istio/pkg/kube"
	"istio.io/istio/pkg/maps"
	"istio.io/istio/pkg/test/framework"
	"istio.io/istio/pkg/test/framework/components/crd"
	"istio.io/istio/pkg/test/framework/components/istioctl"
	"istio.io/istio/pkg/test/framework/components/namespace"
	"istio.io/istio/pkg/test/framework/resource"
	testKube "istio.io/istio/pkg/test/kube"
	"istio.io/istio/pkg/test/scopes"
	"istio.io/istio/pkg/test/util/retry"
)

var _ io.Closer = &kubeComponent{}

type kubeComponent struct {
	id resource.ID

	ns       namespace.Instance
	inbound  istioKube.PortForwarder
	outbound istioKube.PortForwarder
	pod      v1.Pod
}

func (k kubeComponent) Namespace() namespace.Instance {
	return k.ns
}

func (k kubeComponent) PodIP() string {
	return k.pod.Status.PodIP
}

func (k kubeComponent) Inbound() string {
	// 返回inbound地址
	return k.inbound.Address()
}

func (k kubeComponent) Outbound() string {
	// 返回outbound地址
	return k.outbound.Address()
}

func (k kubeComponent) ID() resource.ID {
	return k.id
}

func (k kubeComponent) Close() error {
	if k.inbound != nil {
		k.inbound.Close()
	}
	if k.outbound != nil {
		k.outbound.Close()
	}
	return nil
}

// WaypointProxy describes a waypoint proxy deployment
// WaypointProxy描述了一个waypoint proxy deployment
type WaypointProxy interface {
	Namespace() namespace.Instance
	Inbound() string
	Outbound() string
	PodIP() string
}

// NewWaypointProxy creates a new WaypointProxy.
// NewWaypointProxy创建一个新的WaypointProxy
func NewWaypointProxy(ctx resource.Context, ns namespace.Instance, name string) (WaypointProxy, error) {
	server := &kubeComponent{
		ns: ns,
	}
	server.id = ctx.TrackResource(server)
	// 部署GatewayAPI
	if err := crd.DeployGatewayAPI(ctx); err != nil {
		return nil, err
	}

	// TODO support multicluster
	// 创建一个新的isitoctl
	ik, err := istioctl.New(ctx, istioctl.Config{})
	if err != nil {
		return nil, err
	}
	// TODO: detect from UseWaypointProxy in echo.Config
	// 创建waypoint
	_, _, err = ik.Invoke([]string{
		"x",
		"waypoint",
		"apply",
		"--namespace",
		ns.Name(),
		"--name",
		name,
		"--for",
		constants.AllTraffic,
	})
	if err != nil {
		return nil, err
	}

	cls := ctx.Clusters().Kube().Default()
	// Find the Waypoint pod and service, and start forwarding a local port.
	// 找到waypoint pod和service，并且开始转发一个local port
	fetchFn := testKube.NewSinglePodFetch(cls, ns.Name(), fmt.Sprintf("%s=%s", constants.GatewayNameLabel, name))
	// 等待直到Pods处于Ready状态
	pods, err := testKube.WaitUntilPodsAreReady(fetchFn)
	if err != nil {
		return nil, err
	}
	pod := pods[0]
	inbound, err := cls.NewPortForwarder(pod.Name, pod.Namespace, "", 0, 15008)
	if err != nil {
		return nil, err
	}

	if err := inbound.Start(); err != nil {
		return nil, err
	}
	outbound, err := cls.NewPortForwarder(pod.Name, pod.Namespace, "", 0, 15001)
	if err != nil {
		return nil, err
	}

	if err := outbound.Start(); err != nil {
		return nil, err
	}
	server.inbound = inbound
	server.outbound = outbound
	server.pod = pod

	return server, nil
}

// NewWaypointProxyOrFail calls NewWaypointProxy and fails if an error occurs.
func NewWaypointProxyOrFail(t framework.TestContext, ns namespace.Instance, name string) WaypointProxy {
	t.Helper()
	s, err := NewWaypointProxy(t, ns, name)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func SetWaypointForService(t framework.TestContext, ns namespace.Instance, service, waypoint string) {
	if service == "" {
		return
	}

	cs := t.AllClusters().Kube()
	// 遍历所有的clusters
	for _, c := range cs {
		// 获取svc
		oldSvc, err := c.Kube().CoreV1().Services(ns.Name()).Get(t.Context(), service, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("error getting svc %s, err %v", service, err)
		}
		// 获取svc的labels
		oldLabels := oldSvc.ObjectMeta.GetLabels()
		if oldLabels == nil {
			oldLabels = make(map[string]string, 1)
		}
		// 复制old labels，构建新的labels
		newLabels := maps.Clone(oldLabels)
		if waypoint != "" {
			// 添加waypoint proxy
			newLabels[constants.AmbientUseWaypointLabel] = waypoint
		} else {
			// waypoint为""，则从labels中删除
			delete(newLabels, constants.AmbientUseWaypointLabel)
		}

		doLabel := func(labels map[string]string) error {
			// update needs the latest version
			svc, err := c.Kube().CoreV1().Services(ns.Name()).Get(t.Context(), service, metav1.GetOptions{})
			if err != nil {
				return err
			}
			// 设置对象的labels
			svc.ObjectMeta.SetLabels(labels)
			_, err = c.Kube().CoreV1().Services(ns.Name()).Update(t.Context(), svc, metav1.UpdateOptions{})
			return err
		}

		if err = doLabel(newLabels); err != nil {
			// 更新svc
			t.Fatalf("error updating svc %s, err %v", service, err)
		}
		t.Cleanup(func() {
			if err := doLabel(oldLabels); err != nil {
				// 重新设置waypoint失败，因为它可能会破坏其他的测试
				scopes.Framework.Errorf("failed resetting waypoint for %s/%s; this will likely break other tests", ns.Name(), service)
			}
		})

	}
}

func DeleteWaypoint(t framework.TestContext, ns namespace.Instance, waypoint string) {
	istioctl.NewOrFail(t, t, istioctl.Config{}).InvokeOrFail(t, []string{
		"x",
		"waypoint",
		"delete",
		"--namespace",
		ns.Name(),
		waypoint,
	})
	waypointError := retry.UntilSuccess(func() error {
		fetch := testKube.NewPodFetch(t.AllClusters()[0], ns.Name(), constants.GatewayNameLabel+"="+waypoint)
		pods, err := testKube.CheckPodsAreReady(fetch)
		if err != nil && !errors.Is(err, testKube.ErrNoPodsFetched) {
			return fmt.Errorf("cannot fetch pod: %v", err)
		} else if len(pods) != 0 {
			return fmt.Errorf("waypoint pod is not deleted")
		}
		return nil
	}, retry.Timeout(time.Minute), retry.BackoffDelay(time.Millisecond*100))
	if waypointError != nil {
		t.Fatal(waypointError)
	}
}

func RemoveWaypointFromService(t framework.TestContext, ns namespace.Instance, service, waypoint string) {
	if service != "" {
		cs := t.AllClusters().Configs()
		for _, c := range cs {
			oldSvc, err := c.Kube().CoreV1().Services(ns.Name()).Get(t.Context(), service, metav1.GetOptions{})
			if err != nil {
				t.Fatalf("error getting svc %s, err %v", service, err)
			}
			labels := oldSvc.ObjectMeta.GetLabels()
			if labels != nil {
				delete(labels, constants.AmbientUseWaypointLabel)
				oldSvc.ObjectMeta.SetLabels(labels)
			}
			_, err = c.Kube().CoreV1().Services(ns.Name()).Update(t.Context(), oldSvc, metav1.UpdateOptions{})
			if err != nil {
				t.Fatalf("error updating svc %s, err %v", service, err)
			}
		}
	}
}
