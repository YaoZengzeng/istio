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

// packages used for load balancer setting
// 用于负载均衡设置的包
package loadbalancer

import (
	"math"
	"sort"

	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpoint "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	wrappers "google.golang.org/protobuf/types/known/wrapperspb"

	"istio.io/api/networking/v1alpha3"
	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pilot/pkg/networking/util"
)

func GetLocalityLbSetting(
	mesh *v1alpha3.LocalityLoadBalancerSetting,
	destrule *v1alpha3.LocalityLoadBalancerSetting,
) *v1alpha3.LocalityLoadBalancerSetting {
	var enabled bool
	// Locality lb is enabled if its not explicitly disabled in mesh global config
	if mesh != nil && (mesh.Enabled == nil || mesh.Enabled.Value) {
		enabled = true
	}
	// Unless we explicitly override this in destination rule
	if destrule != nil {
		if destrule.Enabled != nil && !destrule.Enabled.Value {
			enabled = false
		} else {
			enabled = true
		}
	}
	if !enabled {
		return nil
	}

	// Destination Rule overrides mesh config. If its defined, use that
	if destrule != nil {
		return destrule
	}
	// Otherwise fall back to mesh default
	return mesh
}

func ApplyLocalityLBSetting(
	loadAssignment *endpoint.ClusterLoadAssignment,
	wrappedLocalityLbEndpoints []*WrappedLocalityLbEndpoints,
	locality *core.Locality,
	proxyLabels map[string]string,
	localityLB *v1alpha3.LocalityLoadBalancerSetting,
	enableFailover bool,
) {
	if localityLB == nil || loadAssignment == nil {
		return
	}

	// one of Distribute or Failover settings can be applied.
	// 一个Distribute或Failover设置可以应用。
	if localityLB.GetDistribute() != nil {
		// 应用locality weight
		applyLocalityWeight(locality, loadAssignment, localityLB.GetDistribute())
		// Failover needs outlier detection, otherwise Envoy will never drop down to a lower priority.
		// Do not apply default failover when locality LB is disabled.
		// Failover需要outlier detection，否则Envoy不会降低到较低的优先级。
		// 不要应用默认的failover，当locality LB被禁用时。
	} else if enableFailover && (localityLB.Enabled == nil || localityLB.Enabled.Value) {
		// 使能了failover
		if len(localityLB.FailoverPriority) > 0 {
			applyPriorityFailover(loadAssignment, wrappedLocalityLbEndpoints, proxyLabels, localityLB.FailoverPriority)
			return
		}
		applyLocalityFailover(locality, loadAssignment, localityLB.Failover)
	}
}

// set locality loadbalancing weight
// 设置locality权重
func applyLocalityWeight(
	locality *core.Locality,
	loadAssignment *endpoint.ClusterLoadAssignment,
	distribute []*v1alpha3.LocalityLoadBalancerSetting_Distribute,
) {
	if distribute == nil {
		return
	}

	// Support Locality weighted load balancing
	// (https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/upstream/load_balancing/locality_weight#locality-weighted-load-balancing)
	// by providing weights in LocalityLbEndpoints via load_balancing_weight.
	// 通过在LocalityLbEndpoints中提供权重来支持Locality加权负载平衡。
	// By setting weights across different localities, it can allow
	// Envoy to weight assignments across different zones and geographical locations.
	// 通过指定不同locality的权重，它可以允许Envoy在不同的区域和地理位置之间分配权重。
	for _, localityWeightSetting := range distribute {
		if localityWeightSetting != nil &&
			util.LocalityMatch(locality, localityWeightSetting.From) {
			misMatched := map[int]struct{}{}
			for i := range loadAssignment.Endpoints {
				misMatched[i] = struct{}{}
			}
			for locality, weight := range localityWeightSetting.To {
				// index -> original weight
				destLocMap := map[int]uint32{}
				totalWeight := uint32(0)
				for i, ep := range loadAssignment.Endpoints {
					if _, exist := misMatched[i]; exist {
						if util.LocalityMatch(ep.Locality, locality) {
							delete(misMatched, i)
							if ep.LoadBalancingWeight != nil {
								destLocMap[i] = ep.LoadBalancingWeight.Value
							} else {
								destLocMap[i] = 1
							}
							totalWeight += destLocMap[i]
						}
					}
				}
				// in case wildcard dest matching multi groups of endpoints
				// the load balancing weight for a locality is divided by the sum of the weights of all localities
				// 万一通配符dest匹配多组端点，locality的负载平衡权重被所有locality的权重总和除以
				for index, originalWeight := range destLocMap {
					destWeight := float64(originalWeight*weight) / float64(totalWeight)
					if destWeight > 0 {
						// 对weight进行赋值
						loadAssignment.Endpoints[index].LoadBalancingWeight = &wrappers.UInt32Value{
							Value: uint32(math.Ceil(destWeight)),
						}
					}
				}
			}

			// remove groups of endpoints in a locality that miss matched
			// 从locality中删除不匹配的endpoint组
			for i := range misMatched {
				loadAssignment.Endpoints[i].LbEndpoints = nil
			}
			break
		}
	}
}

// set locality loadbalancing priority
// 设置locality优先级
func applyLocalityFailover(
	locality *core.Locality,
	loadAssignment *endpoint.ClusterLoadAssignment,
	failover []*v1alpha3.LocalityLoadBalancerSetting_Failover,
) {
	// key is priority, value is the index of the LocalityLbEndpoints in ClusterLoadAssignment
	// key是priority，value是ClusterLoadAssignment中LocalityLbEndpoints的索引
	priorityMap := map[int][]int{}

	// 1. calculate the LocalityLbEndpoints.Priority compared with proxy locality
	// 1. 计算与代理locality相比的LocalityLbEndpoints.Priority
	for i, localityEndpoint := range loadAssignment.Endpoints {
		// if region/zone/subZone all match, the priority is 0.
		// 如果region/zone/subZone都匹配，则优先级为0
		// if region/zone match, the priority is 1.
		// 如果region/zone匹配，则优先级为1
		// if region matches, the priority is 2.
		// 如果region匹配，则优先级为2
		// if locality not match, the priority is 3.
		// 如果locality不匹配，则优先级为3
		priority := util.LbPriority(locality, localityEndpoint.Locality)
		// region not match, apply failover settings when specified
		// region不匹配，当指定时应用failover设置
		// update localityLbEndpoints' priority to 4 if failover not match
		// 更新localityLbEndpoints的优先级为4，如果failover不匹配
		if priority == 3 {
			for _, failoverSetting := range failover {
				if failoverSetting.From == locality.Region {
					if localityEndpoint.Locality == nil || localityEndpoint.Locality.Region != failoverSetting.To {
						priority = 4
					}
					break
				}
			}
		}
		// 指定优先级
		loadAssignment.Endpoints[i].Priority = uint32(priority)
		priorityMap[priority] = append(priorityMap[priority], i)
	}

	// since Priorities should range from 0 (highest) to N (lowest) without skipping.
	// 因为Priorities应该从0（最高）到N（最低）而不跳过。
	// 2. adjust the priorities in order
	// 2. 按顺序调整优先级
	// 2.1 sort all priorities in increasing order.
	// 2.1 按升序排序所有优先级。
	priorities := []int{}
	for priority := range priorityMap {
		priorities = append(priorities, priority)
	}
	sort.Ints(priorities)
	// 2.2 adjust LocalityLbEndpoints priority
	// if the index and value of priorities array is not equal.
	// 2.2 调整LocalityLbEndpoints优先级，如果优先级数组的索引和值不相等。
	for i, priority := range priorities {
		if i != priority {
			// the LocalityLbEndpoints index in ClusterLoadAssignment.Endpoints
			for _, index := range priorityMap[priority] {
				loadAssignment.Endpoints[index].Priority = uint32(i)
			}
		}
	}
}

// WrappedLocalityLbEndpoints contain an envoy LocalityLbEndpoints
// and the original IstioEndpoints used to generate it.
// WrappedLocalityLbEndpoints包含一个envoy LocalityLbEndpoints和用于生成它的原始IstioEndpoints。
// It is used to do failover priority label match with proxy labels.
// 它用于使用代理标签进行故障转移优先级标签匹配。
type WrappedLocalityLbEndpoints struct {
	IstioEndpoints      []*model.IstioEndpoint
	LocalityLbEndpoints *endpoint.LocalityLbEndpoints
}

// set loadbalancing priority by failover priority label
// 设置故障转移优先级标签的负载平衡优先级
func applyPriorityFailover(
	loadAssignment *endpoint.ClusterLoadAssignment,
	wrappedLocalityLbEndpoints []*WrappedLocalityLbEndpoints,
	proxyLabels map[string]string,
	failoverPriorities []string,
) {
	if len(proxyLabels) == 0 || len(wrappedLocalityLbEndpoints) == 0 {
		return
	}
	priorityMap := make(map[int][]int, len(failoverPriorities))
	localityLbEndpoints := []*endpoint.LocalityLbEndpoints{}
	for _, wrappedLbEndpoint := range wrappedLocalityLbEndpoints {
		localityLbEndpointsPerLocality := applyPriorityFailoverPerLocality(proxyLabels, wrappedLbEndpoint, failoverPriorities)
		localityLbEndpoints = append(localityLbEndpoints, localityLbEndpointsPerLocality...)
	}
	for i, ep := range localityLbEndpoints {
		priorityMap[int(ep.Priority)] = append(priorityMap[int(ep.Priority)], i)
	}
	// since Priorities should range from 0 (highest) to N (lowest) without skipping.
	// adjust the priorities in order
	// 因为Priorities必须从0（最高）到N（最低）而不跳过。
	// 1. sort all priorities in increasing order.
	// 1. 对所有优先级按升序排序。
	priorities := []int{}
	for priority := range priorityMap {
		priorities = append(priorities, priority)
	}
	sort.Ints(priorities)
	// 2. adjust LocalityLbEndpoints priority
	// if the index and value of priorities array is not equal.
	// 2. 调整LocalityLbEndpoints优先级，如果优先级数组的索引和值不相等。
	for i, priority := range priorities {
		if i != priority {
			// the LocalityLbEndpoints index in ClusterLoadAssignment.Endpoints
			for _, index := range priorityMap[priority] {
				localityLbEndpoints[index].Priority = uint32(i)
			}
		}
	}
	loadAssignment.Endpoints = localityLbEndpoints
}

// set loadbalancing priority by failover priority label.
// split one LocalityLbEndpoints to multiple LocalityLbEndpoints based on failover priorities.
func applyPriorityFailoverPerLocality(
	proxyLabels map[string]string,
	ep *WrappedLocalityLbEndpoints,
	failoverPriorities []string,
) []*endpoint.LocalityLbEndpoints {
	lowestPriority := len(failoverPriorities)
	// key is priority, value is the index of LocalityLbEndpoints.LbEndpoints
	priorityMap := map[int][]int{}
	for i, istioEndpoint := range ep.IstioEndpoints {
		var priority int
		// failoverPriority labels match
		for j, label := range failoverPriorities {
			if proxyLabels[label] != istioEndpoint.Labels[label] {
				priority = lowestPriority - j
				break
			}
		}
		priorityMap[priority] = append(priorityMap[priority], i)
	}

	// sort all priorities in increasing order.
	priorities := []int{}
	for priority := range priorityMap {
		priorities = append(priorities, priority)
	}
	sort.Ints(priorities)

	out := make([]*endpoint.LocalityLbEndpoints, len(priorityMap))
	for i, priority := range priorities {
		out[i] = util.CloneLocalityLbEndpoint(ep.LocalityLbEndpoints)
		out[i].LbEndpoints = nil
		out[i].Priority = uint32(priority)
		var weight uint32
		for _, index := range priorityMap[priority] {
			out[i].LbEndpoints = append(out[i].LbEndpoints, ep.LocalityLbEndpoints.LbEndpoints[index])
			weight += ep.LocalityLbEndpoints.LbEndpoints[index].GetLoadBalancingWeight().GetValue()
		}
		// reset weight
		out[i].LoadBalancingWeight = &wrappers.UInt32Value{
			Value: weight,
		}
	}

	return out
}
