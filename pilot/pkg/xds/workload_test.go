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

package xds

import (
	"context"
	"testing"
	"time"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"istio.io/api/security/v1beta1"
	metav1beta1 "istio.io/api/type/v1beta1"
	"istio.io/istio/pilot/pkg/features"
	"istio.io/istio/pilot/pkg/model"
	v3 "istio.io/istio/pilot/pkg/xds/v3"
	"istio.io/istio/pkg/config"
	"istio.io/istio/pkg/config/constants"
	"istio.io/istio/pkg/config/schema/gvk"
	"istio.io/istio/pkg/kube/kclient/clienttest"
	"istio.io/istio/pkg/test"
	"istio.io/istio/pkg/test/util/assert"
	"istio.io/istio/pkg/util/sets"
)

func buildExpect(t *testing.T) func(resp *discovery.DeltaDiscoveryResponse, names ...string) {
	return func(resp *discovery.DeltaDiscoveryResponse, names ...string) {
		t.Helper()
		want := sets.New(names...)
		have := sets.New[string]()
		for _, r := range resp.Resources {
			have.Insert(r.Name)
		}
		if len(resp.RemovedResources) > 0 {
			t.Fatalf("unexpected removals: %v", resp.RemovedResources)
		}
		assert.Equal(t, sets.SortedList(have), sets.SortedList(want))
	}
}

func buildExpectExpectRemoved(t *testing.T) func(resp *discovery.DeltaDiscoveryResponse, names ...string) {
	return func(resp *discovery.DeltaDiscoveryResponse, names ...string) {
		t.Helper()
		want := sets.New(names...)
		have := sets.New[string]()
		for _, r := range resp.RemovedResources {
			have.Insert(r)
		}
		if len(resp.Resources) > 0 {
			t.Fatalf("unexpected resources: %v", resp.Resources)
		}
		assert.Equal(t, sets.SortedList(have), sets.SortedList(want))
	}
}

func buildExpectAddedAndRemoved(t *testing.T) func(resp *discovery.DeltaDiscoveryResponse, added []string, removed []string) {
	return func(resp *discovery.DeltaDiscoveryResponse, added []string, removed []string) {
		t.Helper()
		wantAdded := sets.New(added...)
		wantRemoved := sets.New(removed...)
		have := sets.New[string]()
		haveRemoved := sets.New[string]()
		for _, r := range resp.Resources {
			have.Insert(r.Name)
		}
		for _, r := range resp.RemovedResources {
			haveRemoved.Insert(r)
		}
		assert.Equal(t, sets.SortedList(have), sets.SortedList(wantAdded))
		assert.Equal(t, sets.SortedList(haveRemoved), sets.SortedList(wantRemoved))
	}
}

func TestWorkloadReconnect(t *testing.T) {
	test.SetForTest(t, &features.EnableAmbientControllers, true)
	expect := buildExpect(t)
	s := NewFakeDiscoveryServer(t, FakeOptions{
		KubernetesObjects: []runtime.Object{mkPod("pod", "sa", "127.0.0.1", "not-node")},
	})
	ads := s.ConnectDeltaADS().WithType(v3.AddressType).WithMetadata(model.NodeMetadata{NodeName: "node"})
	ads.Request(&discovery.DeltaDiscoveryRequest{
		ResourceNamesSubscribe:   []string{"*"},
		ResourceNamesUnsubscribe: []string{"*"},
	})
	ads.ExpectEmptyResponse()

	// Now subscribe to the pod, should get it back
	resp := ads.RequestResponseAck(&discovery.DeltaDiscoveryRequest{
		ResourceNamesSubscribe: []string{"/127.0.0.1"},
	})
	expect(resp, "Kubernetes//Pod/default/pod")
	ads.Cleanup()

	// Reconnect
	ads = s.ConnectDeltaADS().WithType(v3.AddressType)
	ads.Request(&discovery.DeltaDiscoveryRequest{
		ResourceNamesSubscribe:   []string{"*"},
		ResourceNamesUnsubscribe: []string{"*"},
		InitialResourceVersions: map[string]string{
			"/127.0.0.1": "",
		},
	})
	expect(ads.ExpectResponse(), "Kubernetes//Pod/default/pod")
}

func TestWorkload(t *testing.T) {
	test.SetForTest(t, &features.EnableAmbientControllers, true)
	t.Run("ondemand", func(t *testing.T) {
		expect := buildExpect(t)
		expectRemoved := buildExpectExpectRemoved(t)
		s := NewFakeDiscoveryServer(t, FakeOptions{})
		ads := s.ConnectDeltaADS().WithType(v3.AddressType).WithMetadata(model.NodeMetadata{NodeName: "node"})

		ads.Request(&discovery.DeltaDiscoveryRequest{
			ResourceNamesSubscribe:   []string{"*"},
			ResourceNamesUnsubscribe: []string{"*"},
		})
		ads.ExpectEmptyResponse()

		// Create pod we are not subscribe to; should be a NOP
		// 创建pod，我们没有订阅，应该为NOP
		createPod(s, "pod", "sa", "127.0.0.1", "not-node")
		ads.ExpectNoResponse()

		// Now subscribe to it, should get it back
		// 现在订阅它
		resp := ads.RequestResponseAck(&discovery.DeltaDiscoveryRequest{
			ResourceNamesSubscribe: []string{"/127.0.0.1"},
		})
		expect(resp, "Kubernetes//Pod/default/pod")

		// Subscribe to unknown pod
		// 订阅一个未知的pod
		ads.Request(&discovery.DeltaDiscoveryRequest{
			ResourceNamesSubscribe: []string{"/127.0.0.2"},
		})
		// "Removed" is a misnomer, but per the spec this is how we report "not found"
		// "Removed"是一个误称，但是这是我们如何报告"not found"
		expectRemoved(ads.ExpectResponse(), "/127.0.0.2")

		// Once we create it, we should get a push
		// 一旦创建，我们应该获取一个push
		createPod(s, "pod2", "sa", "127.0.0.2", "node")
		expect(ads.ExpectResponse(), "Kubernetes//Pod/default/pod2")

		// TODO: implement pod update; this actually cannot really be done without waypoints or VIPs
		deletePod(s, "pod")
		expectRemoved(ads.ExpectResponse(), "Kubernetes//Pod/default/pod")

		// Create pod we are not subscribed to; due to same-node optimization this will push
		// 创建我们不订阅的pod，因为same-node optimization，会push
		createPod(s, "pod-same-node", "sa", "127.0.0.3", "node")
		expect(ads.ExpectResponse(), "Kubernetes//Pod/default/pod-same-node")
		deletePod(s, "pod-same-node")
		expectRemoved(ads.ExpectResponse(), "Kubernetes//Pod/default/pod-same-node")

		// Add service: we should not get any new resources, but updates to existing ones
		// 添加service：我们不应该获取任何新的resources，但是更新已经存在的
		// Note: we are not subscribed to svc1 explicitly, but it impacts pods we are subscribed to
		// 注意：我们没有显式订阅svc1，但是它影响我们订阅的pods
		createService(s, "svc1", "default", map[string]string{"app": "sa"})
		expect(ads.ExpectResponse(), "Kubernetes//Pod/default/pod2")
		// Creating a pod in the service should send an update as usual
		// 创建一个Pod，在service，应该发送一个update，像往常
		createPod(s, "pod", "sa", "127.0.0.1", "node")
		expect(ads.ExpectResponse(), "Kubernetes//Pod/default/pod")
		// Make service not select workload should also update things
		// 当service不选择workload也应该要更新
		createService(s, "svc1", "default", map[string]string{"app": "not-sa"})
		expect(ads.ExpectResponse(), "Kubernetes//Pod/default/pod", "Kubernetes//Pod/default/pod2")

		// Now create pods in the service...
		// 现在创建service的pods
		createPod(s, "pod4", "not-sa", "127.0.0.4", "not-node")
		// Not subscribed, no response
		// 不订阅，没有response
		ads.ExpectNoResponse()

		// Now we subscribe to the service explicitly
		// 现在我们显式订阅service
		ads.Request(&discovery.DeltaDiscoveryRequest{
			ResourceNamesSubscribe: []string{"/10.0.0.1"},
		})
		// Should get updates for all pods in the service
		// 应该获取service中所有pods的更新
		expect(ads.ExpectResponse(), "Kubernetes//Pod/default/pod4", "default/svc1.default.svc.cluster.local")
		// Adding a pod in the service should not trigger an update for that pod - we didn't explicitly subscribe
		// 添加一个pod到service不应触发对于这个pod的更新 - 我们不显式订阅
		createPod(s, "pod5", "not-sa", "127.0.0.5", "not-node")
		ads.ExpectNoResponse()

		// And if the service changes to no longer select them, we should see them *removed* (not updated)
		// 并且如何service的变更没有选择它们，我们应该看到它们被移除
		createService(s, "svc1", "default", map[string]string{"app": "nothing"})
		expect(ads.ExpectResponse(), "Kubernetes//Pod/default/pod4", "default/svc1.default.svc.cluster.local")
	})
	t.Run("wildcard", func(t *testing.T) {
		expect := buildExpect(t)
		expectRemoved := buildExpectExpectRemoved(t)
		s := NewFakeDiscoveryServer(t, FakeOptions{})
		ads := s.ConnectDeltaADS().WithType(v3.AddressType).WithMetadata(model.NodeMetadata{NodeName: "node"})

		ads.Request(&discovery.DeltaDiscoveryRequest{
			ResourceNamesSubscribe: []string{"*"},
		})
		ads.ExpectEmptyResponse()

		// Create pod, due to wildcard subscribe we should receive it
		// 创建pod，因为wildcard订阅，我们应该接收到它
		createPod(s, "pod", "sa", "127.0.0.1", "not-node")
		expect(ads.ExpectResponse(), "Kubernetes//Pod/default/pod")

		// A new pod should push only that one
		// 一个新的pod应该只push一次
		createPod(s, "pod2", "sa", "127.0.0.2", "node")
		expect(ads.ExpectResponse(), "Kubernetes//Pod/default/pod2")

		// TODO: implement pod update; this actually cannot really be done without waypoints or VIPs
		deletePod(s, "pod")
		expectRemoved(ads.ExpectResponse(), "Kubernetes//Pod/default/pod")

		// Add service: we should not get any new resources, but updates to existing ones
		// 添加service：我们应该不获取任何新的resources，但是更新已经存在的
		createService(s, "svc1", "default", map[string]string{"app": "sa"})
		expect(ads.ExpectResponse(), "Kubernetes//Pod/default/pod2", "default/svc1.default.svc.cluster.local")
		// Creating a pod in the service should send an update as usual
		// 在service里创建一个pod应该像往常一样发送一个update
		createPod(s, "pod", "sa", "127.0.0.3", "node")
		expect(ads.ExpectResponse(), "Kubernetes//Pod/default/pod")

		// Make service not select workload should also update things
		// 让service不选择workload也应该更新
		createService(s, "svc1", "default", map[string]string{"app": "not-sa"})
		expect(ads.ExpectResponse(), "Kubernetes//Pod/default/pod", "Kubernetes//Pod/default/pod2", "default/svc1.default.svc.cluster.local")
	})
}

func deletePod(s *FakeDiscoveryServer, name string) {
	err := s.kubeClient.Kube().CoreV1().Pods("default").Delete(context.Background(), name, metav1.DeleteOptions{})
	if err != nil {
		s.t.Fatal(err)
	}
}

func createAuthorizationPolicy(s *FakeDiscoveryServer, name string, ns string) {
	_, err := s.Env().Create(config.Config{
		Meta: config.Meta{
			GroupVersionKind: gvk.AuthorizationPolicy,
			Name:             name,
			Namespace:        ns,
		},
		Spec: &v1beta1.AuthorizationPolicy{},
	})
	if err != nil {
		s.t.Fatal(err)
	}
}

func createPeerAuthentication(s *FakeDiscoveryServer, name string, ns string, f func(*config.Config)) {
	c := config.Config{
		Meta: config.Meta{
			GroupVersionKind: gvk.PeerAuthentication,
			Name:             name,
			Namespace:        ns,
		},
		Spec: &v1beta1.PeerAuthentication{},
	}
	if f != nil {
		f(&c)
	}
	_, err := s.Env().Create(c)
	if err != nil {
		s.t.Fatal(err)
	}
}

func mkPod(name string, sa string, ip string, node string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Annotations: map[string]string{
				constants.AmbientRedirection: constants.AmbientRedirectionEnabled,
			},
			Labels: map[string]string{
				"app": sa,
			},
		},
		Spec: corev1.PodSpec{
			ServiceAccountName: sa,
			NodeName:           node,
		},
		Status: corev1.PodStatus{
			PodIP: ip,
			PodIPs: []corev1.PodIP{
				{
					IP: ip,
				},
			},
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{
					Type:               corev1.PodReady,
					Status:             corev1.ConditionTrue,
					LastTransitionTime: metav1.Now(),
				},
			},
		},
	}
}

func createPod(s *FakeDiscoveryServer, name string, sa string, ip string, node string) {
	pod := mkPod(name, sa, ip, node)
	pods := clienttest.NewWriter[*corev1.Pod](s.t, s.kubeClient)
	pods.CreateOrUpdate(pod)
	pods.UpdateStatus(pod)
}

// nolint: unparam
func createService(s *FakeDiscoveryServer, name, namespace string, selector map[string]string) {
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "10.0.0.1",
			Ports: []corev1.ServicePort{{
				Name:     "tcp",
				Port:     80,
				Protocol: "TCP",
			}},
			Selector: selector,
			Type:     corev1.ServiceTypeClusterIP,
		},
	}

	svcs := clienttest.NewWriter[*corev1.Service](s.t, s.kubeClient)
	svcs.CreateOrUpdate(service)
}

func TestWorkloadAuthorizationPolicy(t *testing.T) {
	test.SetForTest(t, &features.EnableAmbientControllers, true)
	expect := buildExpect(t)
	expectRemoved := buildExpectExpectRemoved(t)
	// 构建fake discovery server
	s := NewFakeDiscoveryServer(t, FakeOptions{})
	// 连接delta ADS
	ads := s.ConnectDeltaADS().WithType(v3.WorkloadAuthorizationType).WithTimeout(time.Second * 10).WithNodeType(model.Ztunnel)

	ads.Request(&discovery.DeltaDiscoveryRequest{
		ResourceNamesSubscribe: []string{"*"},
	})
	ads.ExpectEmptyResponse()

	// Create policy, due to wildcard subscribe we should receive it
	// 创建policy，因为wildcard订阅，我们应该接收它
	createAuthorizationPolicy(s, "policy1", "ns")
	expect(ads.ExpectResponse(), "ns/policy1")

	// A new policy should push only that one
	// 一个新的policy应该push一个
	createAuthorizationPolicy(s, "policy2", "ns")
	expect(ads.ExpectResponse(), "ns/policy2")

	s.Env().Delete(gvk.AuthorizationPolicy, "policy2", "ns", nil)
	expectRemoved(ads.ExpectResponse(), "ns/policy2")

	// Irrelevant update shouldn't push
	// 无关的update不应该被push
	createPod(s, "pod", "sa", "127.0.0.1", "node")
	ads.ExpectNoResponse()
}

func TestWorkloadPeerAuthentication(t *testing.T) {
	test.SetForTest(t, &features.EnableAmbientControllers, true)
	expect := buildExpect(t)
	expectAddedAndRemoved := buildExpectAddedAndRemoved(t)
	s := NewFakeDiscoveryServer(t, FakeOptions{})
	ads := s.ConnectDeltaADS().WithType(v3.WorkloadAuthorizationType).WithTimeout(time.Second * 10).WithNodeType(model.Ztunnel)

	ads.Request(&discovery.DeltaDiscoveryRequest{
		ResourceNamesSubscribe: []string{"*"},
	})
	ads.ExpectEmptyResponse()

	// Create policy; it should push only the static strict policy
	// 创建policy，它应该只推送静态的strict policy
	// We expect a removal because the policy exists in the cluster but is not sent to the proxy (because it's not port-specific, STRICT, etc.)
	createPeerAuthentication(s, "policy1", "ns", nil)
	expectAddedAndRemoved(ads.ExpectResponse(), []string{"istio-system/istio_converted_static_strict"}, []string{"ns/converted_peer_authentication_policy1"})

	createPeerAuthentication(s, "policy2", "ns", func(c *config.Config) {
		c.Spec = &v1beta1.PeerAuthentication{
			Mtls: &v1beta1.PeerAuthentication_MutualTLS{
				Mode: v1beta1.PeerAuthentication_MutualTLS_PERMISSIVE,
			},
			PortLevelMtls: map[uint32]*v1beta1.PeerAuthentication_MutualTLS{
				9080: {
					Mode: v1beta1.PeerAuthentication_MutualTLS_STRICT,
				},
			},
			Selector: &metav1beta1.WorkloadSelector{
				MatchLabels: map[string]string{
					"app": "sa", // This patches the pod we will create
				},
			},
		}
	})
	expect(ads.ExpectResponse(), "ns/converted_peer_authentication_policy2", "istio-system/istio_converted_static_strict")

	// We expect a removal because the policy was deleted
	// Note that policy1 was not removed because its config was not updated (i.e. this is a partial push)
	s.Env().Delete(gvk.PeerAuthentication, "policy2", "ns", nil)
	expectAddedAndRemoved(ads.ExpectResponse(), []string{"istio-system/istio_converted_static_strict"}, []string{"ns/converted_peer_authentication_policy2"})

	// Irrelevant update (pod is in the default namespace and not "ns") shouldn't push
	createPod(s, "pod", "sa", "127.0.0.1", "node")
	ads.ExpectNoResponse()
}
