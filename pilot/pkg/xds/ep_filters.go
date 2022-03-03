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
	"net"

	endpoint "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/wrappers"

	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pilot/pkg/networking/util"
	"istio.io/istio/pkg/config/labels"
)

// EndpointsByNetworkFilter is a network filter function to support Split Horizon EDS - filter the endpoints based on the network
// of the connected sidecar. The filter will filter out all endpoints which are not present within the
// sidecar network and add a gateway endpoint to remote networks that have endpoints
// (if gateway exists and its IP is an IP and not a dns name).
// EndpointsByNetworkFilter是一个network filter函数，用来支持Split Horizion EDS - 基于连接的sidecar所在的网络对endpoints进行过滤
// filter会过滤掉所有不在sidecar network的endpoints，并且增加一个gateway endpoint到有endpoints的remote networks
// （如果gateways存在并且它的IP是一个IP而不是一个dns name）
// Information for the mesh networks is provided as a MeshNetwork config map.
func (b *EndpointBuilder) EndpointsByNetworkFilter(endpoints []*endpoint.LocalityLbEndpoints) []*endpoint.LocalityLbEndpoints {
	// calculate the multiples of weight.
	// It is needed to normalize the LB Weight across different networks.
	multiples := 1
	for _, gateways := range b.push.NetworkGateways() {
		if num := len(gateways); num > 0 {
			multiples *= num
		}
	}

	// A new array of endpoints to be returned that will have both local and
	// remote gateways (if any)
	// 一个新的endpoints的array，既有local的，也有remote gateways的，如果有的话
	filtered := make([]*endpoint.LocalityLbEndpoints, 0)

	// Go through all cluster endpoints and add those with the same network as the sidecar
	// to the result. Also count the number of endpoints per each remote network while
	// iterating so that it can be used as the weight for the gateway endpoint
	for _, ep := range endpoints {
		lbEndpoints := make([]*endpoint.LbEndpoint, 0)

		// Weight (number of endpoints) for the EDS cluster for each remote networks
		remoteEps := map[string]uint32{}
		// calculate remote network endpoints
		for _, lbEp := range ep.LbEndpoints {
			epNetwork := istioMetadata(lbEp, "network")
			// This is a local endpoint or remote network endpoint
			// but can be accessed directly from local network.
			if epNetwork == b.network || len(b.push.NetworkGatewaysByNetwork(epNetwork)) == 0 {
				// Clone the endpoint so subsequent updates to the shared cache of
				// service endpoints doesn't overwrite endpoints already in-flight.
				// 这是一个本地的endpoint
				clonedLbEp := proto.Clone(lbEp).(*endpoint.LbEndpoint)
				clonedLbEp.LoadBalancingWeight = &wrappers.UInt32Value{
					Value: uint32(multiples),
				}
				lbEndpoints = append(lbEndpoints, clonedLbEp)
			} else {
				if !b.canViewNetwork(epNetwork) {
					continue
				}
				if tlsMode := envoytransportSocketMetadata(lbEp, "tlsMode"); tlsMode == model.DisabledTLSModeLabel {
					// dont allow cross-network endpoints for uninjected traffic
					continue
				}

				// Remote network endpoint which can not be accessed directly from local network.
				// Increase the weight counter
				// Remote network endpoint，不能直接从local network访问
				remoteEps[epNetwork]++
			}
		}

		// Add remote networks' gateways to endpoints if the gateway is a valid IP
		// 添加remote networks的gateways到endpoints，如果gateway是一个合法的IP
		// If its a dns name (like AWS ELB), skip adding all endpoints from this network.
		// 如果它是一个dns name（就像AWS ELB），跳过这个network中所有的endpoints

		// Iterate over all networks that have the cluster endpoint (weight>0) and
		// for each one of those add a new endpoint that points to the network's
		// gateway with the relevant weight. For each gateway endpoint, set the tlsMode metadata so that
		// we initiate mTLS automatically to this remote gateway. Split horizon to remote gateway cannot
		// work with plaintext
		for network, w := range remoteEps {
			// 获取network的gateways
			gateways := b.push.NetworkGatewaysByNetwork(network)

			gatewayNum := len(gateways)
			weight := w * uint32(multiples/gatewayNum)

			// There may be multiples gateways for one network. Add each gateway as an endpoint.
			// 一个network有多个gateways，添加每个gateway作为一个endpoint
			for _, gw := range gateways {
				if net.ParseIP(gw.Addr) == nil {
					// this is a gateway with hostname in it. skip this gateway as EDS can't take hostnames
					continue
				}
				// 构建基于gateway的LbEndpoint
				epAddr := util.BuildAddress(gw.Addr, gw.Port)
				gwEp := &endpoint.LbEndpoint{
					HostIdentifier: &endpoint.LbEndpoint_Endpoint{
						Endpoint: &endpoint.Endpoint{
							Address: epAddr,
						},
					},
					LoadBalancingWeight: &wrappers.UInt32Value{
						Value: weight,
					},
				}
				// TODO: figure out a way to extract locality data from the gateway public endpoints in meshNetworks
				gwEp.Metadata = util.BuildLbEndpointMetadata(network, model.IstioMutualTLSModeLabel, "", "", labels.Instance{})
				lbEndpoints = append(lbEndpoints, gwEp)
			}
		}

		// Found endpoint(s) that can be accessed from local network
		// and then build a new LocalityLbEndpoints with them.
		// 找到可以直接从local network访问的endpoint(s)，之后再用他们构建一个
		// 新的LocalityLbEndpoints
		newEp := createLocalityLbEndpoints(ep, lbEndpoints)
		filtered = append(filtered, newEp)
	}

	return filtered
}

// TODO: remove this, filtering should be done before generating the config, and
// network metadata should not be included in output. A node only receives endpoints
// in the same network as itself - so passing an network meta, with exactly
// same value that the node itself had, on each endpoint is a bit absurd.

// Checks whether there is an istio metadata string value for the provided key
// within the endpoint metadata. If exists, it will return the value.
func istioMetadata(ep *endpoint.LbEndpoint, key string) string {
	if ep.Metadata != nil &&
		ep.Metadata.FilterMetadata[util.IstioMetadataKey] != nil &&
		ep.Metadata.FilterMetadata[util.IstioMetadataKey].Fields != nil &&
		ep.Metadata.FilterMetadata[util.IstioMetadataKey].Fields[key] != nil {
		return ep.Metadata.FilterMetadata[util.IstioMetadataKey].Fields[key].GetStringValue()
	}
	return ""
}

func envoytransportSocketMetadata(ep *endpoint.LbEndpoint, key string) string {
	if ep.Metadata != nil &&
		ep.Metadata.FilterMetadata[util.EnvoyTransportSocketMetadataKey] != nil &&
		ep.Metadata.FilterMetadata[util.EnvoyTransportSocketMetadataKey].Fields != nil &&
		ep.Metadata.FilterMetadata[util.EnvoyTransportSocketMetadataKey].Fields[key] != nil {
		return ep.Metadata.FilterMetadata[util.EnvoyTransportSocketMetadataKey].Fields[key].GetStringValue()
	}
	return ""
}

func createLocalityLbEndpoints(base *endpoint.LocalityLbEndpoints, lbEndpoints []*endpoint.LbEndpoint) *endpoint.LocalityLbEndpoints {
	var weight *wrappers.UInt32Value
	if len(lbEndpoints) == 0 {
		weight = nil
	} else {
		weight = &wrappers.UInt32Value{}
		for _, lbEp := range lbEndpoints {
			weight.Value += lbEp.GetLoadBalancingWeight().Value
		}
	}
	ep := &endpoint.LocalityLbEndpoints{
		Locality:            base.Locality,
		LbEndpoints:         lbEndpoints,
		LoadBalancingWeight: weight,
		Priority:            base.Priority,
	}
	return ep
}
