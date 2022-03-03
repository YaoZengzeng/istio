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
	endpoint "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/golang/protobuf/ptypes/any"

	networkingapi "istio.io/api/networking/v1alpha3"
	"istio.io/istio/pilot/pkg/model"
	networking "istio.io/istio/pilot/pkg/networking/core/v1alpha3"
	"istio.io/istio/pilot/pkg/networking/core/v1alpha3/loadbalancer"
	"istio.io/istio/pilot/pkg/networking/util"
	"istio.io/istio/pilot/pkg/util/sets"
	v3 "istio.io/istio/pilot/pkg/xds/v3"
	"istio.io/istio/pkg/config"
	"istio.io/istio/pkg/config/labels"
	"istio.io/istio/pkg/config/protocol"
	"istio.io/istio/pkg/config/schema/gvk"
)

// UpdateServiceShards will list the endpoints and create the shards.
// This is used to reconcile and to support non-k8s registries (until they migrate).
// Note that aggregated list is expensive (for large numbers) - we want to replace
// it with a model where DiscoveryServer keeps track of all endpoint registries
// directly, and calls them one by one.
func (s *DiscoveryServer) UpdateServiceShards(push *model.PushContext) error {
	registries := s.getNonK8sRegistries()
	// Short circuit now to avoid the call to Services
	if len(registries) == 0 {
		return nil
	}
	// Each registry acts as a shard - we don't want to combine them because some
	// may individually update their endpoints incrementally
	for _, svc := range push.Services(nil) {
		for _, registry := range registries {
			// skip the service in case this svc does not belong to the registry.
			if svc.Attributes.ServiceRegistry != string(registry.Provider()) {
				continue
			}
			endpoints := make([]*model.IstioEndpoint, 0)
			for _, port := range svc.Ports {
				if port.Protocol == protocol.UDP {
					continue
				}

				// This loses track of grouping (shards)
				for _, inst := range registry.InstancesByPort(svc, port.Port, labels.Collection{}) {
					endpoints = append(endpoints, inst.Endpoint)
				}
			}

			s.edsCacheUpdate(registry.Cluster(), string(svc.Hostname), svc.Attributes.Namespace, endpoints)
		}
	}

	return nil
}

// SvcUpdate is a callback from service discovery when service info changes.
func (s *DiscoveryServer) SvcUpdate(cluster, hostname string, namespace string, event model.Event) {
	// When a service deleted, we should cleanup the endpoint shards and also remove keys from EndpointShardsByService to
	// prevent memory leaks.
	if event == model.EventDelete {
		inboundServiceDeletes.Increment()
		s.deleteService(cluster, hostname, namespace)
	} else {
		inboundServiceUpdates.Increment()
	}
}

// EDSUpdate computes destination address membership across all clusters and networks.
// EDSUpdate跨越所有的clusters以及networks计算destination address membership
// This is the main method implementing EDS.
// 这是实现EDS的主要方法
// It replaces InstancesByPort in model - instead of iterating over all endpoints it uses
// the hostname-keyed map. And it avoids the conversion from Endpoint to ServiceEntry to envoy
// on each step: instead the conversion happens once, when an endpoint is first discovered.
func (s *DiscoveryServer) EDSUpdate(clusterID, serviceName string, namespace string,
	istioEndpoints []*model.IstioEndpoint) {
	inboundEDSUpdates.Increment()
	// Update the endpoint shards
	// 更新endpoint shards
	fp := s.edsCacheUpdate(clusterID, serviceName, namespace, istioEndpoints)
	// Trigger a push
	// 触发一个push
	s.ConfigUpdate(&model.PushRequest{
		Full: fp,
		ConfigsUpdated: map[model.ConfigKey]struct{}{{
			Kind:      gvk.ServiceEntry,
			Name:      serviceName,
			Namespace: namespace,
		}: {}},
		Reason: []model.TriggerReason{model.EndpointUpdate},
	})
}

// EDSCacheUpdate computes destination address membership across all clusters and networks.
// This is the main method implementing EDS.
// It replaces InstancesByPort in model - instead of iterating over all endpoints it uses
// the hostname-keyed map. And it avoids the conversion from Endpoint to ServiceEntry to envoy
// on each step: instead the conversion happens once, when an endpoint is first discovered.
//
// Note: the difference with `EDSUpdate` is that it only update the cache rather than requesting a push
func (s *DiscoveryServer) EDSCacheUpdate(clusterID, serviceName string, namespace string,
	istioEndpoints []*model.IstioEndpoint) {
	inboundEDSUpdates.Increment()
	// Update the endpoint shards
	s.edsCacheUpdate(clusterID, serviceName, namespace, istioEndpoints)
}

// edsCacheUpdate updates EndpointShards data by clusterID, hostname, IstioEndpoints.
// It also tracks the changes to ServiceAccounts. It returns whether a full push
// is needed or incremental push is sufficient.
// edsCacheUpdate更新EndpointShards数据，通过clusterID，hostname，IstioEndpoints
// 它同时追踪ServiceAccounts的变更，它返回是进行一个full push还是incremental push就足够了
func (s *DiscoveryServer) edsCacheUpdate(clusterID, hostname string, namespace string,
	istioEndpoints []*model.IstioEndpoint) bool {
	if len(istioEndpoints) == 0 {
		// Should delete the service EndpointShards when endpoints become zero to prevent memory leak,
		// but we should not do not delete the keys from EndpointShardsByService map - that will trigger
		// unnecessary full push which can become a real problem if a pod is in crashloop and thus endpoints
		// flip flopping between 1 and 0.
		// 应该删除service EndpointShards，当endpoints变为0，来防止memory leak
		// 但是我们不应该从EndpointShardsByService map中删除keys，这会触发一次不必要的full push，这会变成一个真正的问题
		// 如果pod是crashloop，因此endpoints在1和0之间跳变
		s.deleteEndpointShards(clusterID, hostname, namespace)
		adsLog.Infof("Incremental push, service %s has no endpoints", hostname)
		return false
	}

	fullPush := false

	// Find endpoint shard for this service, if it is available - otherwise create a new one.
	// 找到这个service的endpoint shard，如果它可用的话，否则创建一个新的
	ep, created := s.getOrCreateEndpointShard(hostname, namespace)
	// If we create a new endpoint shard, that means we have not seen the service earlier. We should do a full push.
	// 如果我们新建了一个endpoint shard，这意味着我们之前没看到过这个service，我们应该做一个full push
	if created {
		adsLog.Infof("Full push, new service %s", hostname)
		fullPush = true
	}

	// Check if ServiceAccounts have changed. We should do a full push if they have changed.
	// 检查ServiceAccounts是否发生了改变，我们应该做一次full push，如果他们发生了变更
	serviceAccounts := sets.Set{}
	for _, e := range istioEndpoints {
		if e.ServiceAccount != "" {
			serviceAccounts.Insert(e.ServiceAccount)
		}
	}

	ep.mutex.Lock()
	// For existing endpoints, we need to do full push if service accounts change.
	// 对于已经存在的endpoints，我们应该做full push，如果service accounts变更
	if !fullPush && !serviceAccounts.Equals(ep.ServiceAccounts) {
		adsLog.Debugf("Updating service accounts now, svc %v, before service account %v, after %v",
			hostname, ep.ServiceAccounts, serviceAccounts)
		adsLog.Infof("Full push, service accounts changed, %v", hostname)
		fullPush = true
	}
	// cluster id到istio endpoints之间的映射
	ep.Shards[clusterID] = istioEndpoints
	ep.ServiceAccounts = serviceAccounts
	ep.mutex.Unlock()

	return fullPush
}

func (s *DiscoveryServer) getOrCreateEndpointShard(serviceName, namespace string) (*EndpointShards, bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, exists := s.EndpointShardsByService[serviceName]; !exists {
		// 创建endpoint shard
		s.EndpointShardsByService[serviceName] = map[string]*EndpointShards{}
	}
	if ep, exists := s.EndpointShardsByService[serviceName][namespace]; exists {
		return ep, false
	}
	// This endpoint is for a service that was not previously loaded.
	// 这是一个之前未加载过的service的endpoint
	ep := &EndpointShards{
		Shards:          map[string][]*model.IstioEndpoint{},
		ServiceAccounts: sets.Set{},
	}
	s.EndpointShardsByService[serviceName][namespace] = ep

	return ep, true
}

// deleteEndpointShards deletes matching endpoint shards from EndpointShardsByService map. This is called when
// endpoints are deleted.
func (s *DiscoveryServer) deleteEndpointShards(cluster, serviceName, namespace string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.EndpointShardsByService[serviceName] != nil &&
		s.EndpointShardsByService[serviceName][namespace] != nil {
		s.EndpointShardsByService[serviceName][namespace].mutex.Lock()
		delete(s.EndpointShardsByService[serviceName][namespace].Shards, cluster)
		s.EndpointShardsByService[serviceName][namespace].mutex.Unlock()
	}
}

// deleteService deletes all service related references from EndpointShardsByService. This is called
// when a service is deleted.
func (s *DiscoveryServer) deleteService(cluster, serviceName, namespace string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.EndpointShardsByService[serviceName] != nil &&
		s.EndpointShardsByService[serviceName][namespace] != nil {

		s.EndpointShardsByService[serviceName][namespace].mutex.Lock()
		delete(s.EndpointShardsByService[serviceName][namespace].Shards, cluster)
		shards := len(s.EndpointShardsByService[serviceName][namespace].Shards)
		s.EndpointShardsByService[serviceName][namespace].mutex.Unlock()

		if shards == 0 {
			delete(s.EndpointShardsByService[serviceName], namespace)
		}
		if len(s.EndpointShardsByService[serviceName]) == 0 {
			delete(s.EndpointShardsByService, serviceName)
		}
	}
}

// loadAssignmentsForCluster return the endpoints for a cluster
// loadAssignmentsForCluster返回一个cluster的endpoints
// Initial implementation is computing the endpoints on the flight - caching will be added as needed, based on
// perf tests.
// 初始的实现是直接计算endpoints - 缓存会按需添加，基于perf tests
func (s *DiscoveryServer) loadAssignmentsForCluster(b EndpointBuilder) *endpoint.ClusterLoadAssignment {
	if b.service == nil {
		// Shouldn't happen here
		adsLog.Debugf("can not find the service for cluster %s", b.clusterName)
		return buildEmptyClusterLoadAssignment(b.clusterName)
	}

	// Service resolution type might have changed and Cluster may be still in the EDS cluster list of "Connection.Clusters".
	// This can happen if a ServiceEntry's resolution is changed from STATIC to DNS which changes the Envoy cluster type from
	// EDS to STRICT_DNS. When pushEds is called before Envoy sends the updated cluster list via Endpoint request which in turn
	// will update "Connection.Clusters", we might accidentally send EDS updates for STRICT_DNS cluster. This check guards
	// against such behavior and returns nil. When the updated cluster warms up in Envoy, it would update with new endpoints
	// automatically.
	// Gateways use EDS for Passthrough cluster. So we should allow Passthrough here.
	if b.service.Resolution == model.DNSLB {
		adsLog.Infof("cluster %s in eds cluster, but its resolution now is updated to %v, skipping it.", b.clusterName, b.service.Resolution)
		return nil
	}

	svcPort, f := b.service.Ports.GetByPort(b.port)
	if !f {
		// Shouldn't happen here
		adsLog.Debugf("can not find the service port %d for cluster %s", b.port, b.clusterName)
		return buildEmptyClusterLoadAssignment(b.clusterName)
	}

	s.mutex.RLock()
	// 获取endpoints shard
	epShards, f := s.EndpointShardsByService[string(b.hostname)][b.service.Attributes.Namespace]
	s.mutex.RUnlock()
	if !f {
		// Shouldn't happen here
		adsLog.Debugf("can not find the endpointShards for cluster %s", b.clusterName)
		return buildEmptyClusterLoadAssignment(b.clusterName)
	}

	locEps := b.buildLocalityLbEndpointsFromShards(epShards, svcPort)

	return &endpoint.ClusterLoadAssignment{
		ClusterName: b.clusterName,
		Endpoints:   locEps,
	}
}

func (s *DiscoveryServer) generateEndpoints(b EndpointBuilder) *endpoint.ClusterLoadAssignment {
	l := s.loadAssignmentsForCluster(b)
	if l == nil {
		return nil
	}

	// If networks are set (by default they aren't) apply the Split Horizon
	// EDS filter on the endpoints
	// 如果设置了networks（默认不设置），在endpoints中设置Split Horizon EDS filter
	if b.MultiNetworkConfigured() {
		l.Endpoints = b.EndpointsByNetworkFilter(l.Endpoints)
	}

	// If locality aware routing is enabled, prioritize endpoints or set their lb weight.
	// Failover should only be enabled when there is an outlier detection, otherwise Envoy
	// will never detect the hosts are unhealthy and redirect traffic.
	enableFailover, lb := getOutlierDetectionAndLoadBalancerSettings(b.DestinationRule(), b.port, b.subsetName)
	lbSetting := loadbalancer.GetLocalityLbSetting(b.push.Mesh.GetLocalityLbSetting(), lb.GetLocalityLbSetting())
	if lbSetting != nil {
		// Make a shallow copy of the cla as we are mutating the endpoints with priorities/weights relative to the calling proxy
		l = util.CloneClusterLoadAssignment(l)
		loadbalancer.ApplyLocalityLBSetting(b.locality, l, lbSetting, enableFailover)
	}
	return l
}

// EdsGenerator implements the new Generate method for EDS, using the in-memory, optimized endpoint
// storage in DiscoveryServer.
// EdsGenerator为EDS实现了新的Generate方法，在DiscoveryServer中使用内存中的，缓存的endpoint存储
type EdsGenerator struct {
	Server *DiscoveryServer
}

var _ model.XdsResourceGenerator = &EdsGenerator{}

// Map of all configs that do not impact EDS
var skippedEdsConfigs = map[config.GroupVersionKind]struct{}{
	gvk.Gateway:               {},
	gvk.VirtualService:        {},
	gvk.WorkloadGroup:         {},
	gvk.AuthorizationPolicy:   {},
	gvk.RequestAuthentication: {},
	gvk.Secret:                {},
}

func edsNeedsPush(updates model.XdsUpdates) bool {
	// If none set, we will always push
	if len(updates) == 0 {
		return true
	}
	for config := range updates {
		if _, f := skippedEdsConfigs[config.Kind]; !f {
			return true
		}
	}
	return false
}

func (eds *EdsGenerator) Generate(proxy *model.Proxy, push *model.PushContext, w *model.WatchedResource, req *model.PushRequest) model.Resources {
	if !edsNeedsPush(req.ConfigsUpdated) {
		return nil
	}
	var edsUpdatedServices map[string]struct{}
	if !req.Full {
		edsUpdatedServices = model.ConfigNamesOfKind(req.ConfigsUpdated, gvk.ServiceEntry)
	}
	resources := make([]*any.Any, 0)
	empty := 0

	cached := 0
	regenerated := 0
	for _, clusterName := range w.ResourceNames {
		if edsUpdatedServices != nil {
			_, _, hostname, _ := model.ParseSubsetKey(clusterName)
			if _, ok := edsUpdatedServices[string(hostname)]; !ok {
				// Cluster was not updated, skip recomputing. This happens when we get an incremental update for a
				// specific Hostname. On connect or for full push edsUpdatedServices will be empty.
				// Cluster没有更新，跳过recomputing，这会在我们获取一个incremental update用于一个特定的Hostname
				// 刚连接，或者full push，edsUpdatedServices会为空
				continue
			}
		}
		builder := NewEndpointBuilder(clusterName, proxy, push)
		if marshalledEndpoint, f := eds.Server.Cache.Get(builder); f {
			resources = append(resources, marshalledEndpoint)
			cached++
		} else {
			l := eds.Server.generateEndpoints(builder)
			if l == nil {
				continue
			}
			regenerated++

			if len(l.Endpoints) == 0 {
				empty++
			}
			resource := util.MessageToAny(l)
			resources = append(resources, resource)
			// 加入到cache中
			eds.Server.Cache.Add(builder, resource)
		}
	}
	if len(edsUpdatedServices) == 0 {
		adsLog.Infof("EDS: PUSH for node:%s resources:%d empty:%v cached:%v/%v",
			proxy.ID, len(resources), empty, cached, cached+regenerated)
	} else {
		adsLog.Debugf("EDS: PUSH INC for node:%s clusters:%d empty:%v cached:%v/%v",
			proxy.ID, len(resources), empty, cached, cached+regenerated)
	}
	return resources
}

func getOutlierDetectionAndLoadBalancerSettings(
	destinationRule *networkingapi.DestinationRule,
	portNumber int,
	subsetName string) (bool, *networkingapi.LoadBalancerSettings) {
	if destinationRule == nil {
		return false, nil
	}
	var outlierDetectionEnabled = false
	var lbSettings *networkingapi.LoadBalancerSettings

	port := &model.Port{Port: portNumber}
	policy := networking.MergeTrafficPolicy(nil, destinationRule.TrafficPolicy, port)

	for _, subset := range destinationRule.Subsets {
		if subset.Name == subsetName {
			policy = networking.MergeTrafficPolicy(policy, subset.TrafficPolicy, port)
			break
		}
	}

	if policy != nil {
		lbSettings = policy.LoadBalancer
		if policy.OutlierDetection != nil {
			outlierDetectionEnabled = true
		}
	}

	return outlierDetectionEnabled, lbSettings
}

func endpointDiscoveryResponse(loadAssignments []*any.Any, version, noncePrefix string) *discovery.DiscoveryResponse {
	out := &discovery.DiscoveryResponse{
		TypeUrl: v3.EndpointType,
		// Pilot does not really care for versioning. It always supplies what's currently
		// available to it, irrespective of whether Envoy chooses to accept or reject EDS
		// responses. Pilot believes in eventual consistency and that at some point, Envoy
		// will begin seeing results it deems to be good.
		VersionInfo: version,
		Nonce:       nonce(noncePrefix),
		Resources:   loadAssignments,
	}

	return out
}

// cluster with no endpoints
func buildEmptyClusterLoadAssignment(clusterName string) *endpoint.ClusterLoadAssignment {
	return &endpoint.ClusterLoadAssignment{
		ClusterName: clusterName,
	}
}
