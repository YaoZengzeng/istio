// Copyright 2020 Istio Authors
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

// Package label contains common definitions for workload labels used by Istio.
package label

const (
	// TLSMode is the name of label given to service instances to determine whether to use mTLS or
	// fallback to plaintext/tls
	TLSMode = "security.istio.io/tlsMode"

	// IstioCanonicalServiceName is the name of label for the Istio Canonical Service for a workload instance.
	IstioCanonicalServiceName = "service.istio.io/canonical-name"

	// IstioCanonicalServiceRevision is the name of label for the Istio Canonical Service revision for a workload instance.
	IstioCanonicalServiceRevision = "service.istio.io/canonical-revision"

	// IstioRev is the Istio control plane revision associated with the resource; e.g. "canary"
	IstioRev = "istio.io/rev"

	// IstioOperatorComponent is the Istio operator component name of the resource, e.g. "Pilot"
	IstioOperatorComponent = "operator.istio.io/component"

	// IstioOperatorManaged is "Reconcile" if the Istio operator will reconcile the resource.
	IstioOperatorManaged = "operator.istio.io/managed"

	// IstioOperatorVersion is the Istio operator version that installed the resource, e.g. "1.6.0"
	IstioOperatorVersion = "operator.istio.io/version"

	// IstioSubZone identifies the locality subzone of a workload.
	IstioSubZone = "topology.istio.io/subzone"

	// IstioNetwork enables Istio to group endpoints resident in the same L3 domain/network.
	// All endpoints in the same network are assumed to be directly reachable from one another.
	// When endpoints in different networks cannot reach each other directly, an Istio Gateway can
	// be used to establish connectivity (usually using the AUTO_PASSTHROUGH mode in a  Gateway Server).
	// This is an advanced configuration used typically for spanning an Istio mesh over multiple clusters.
	// IstioNetwork让Istio能够聚合在同一个L3 domain/network之内的endpoints，所有在同一个network之内的endpoint
	// 都假设能够互相访问，当endpoints处于不同的networks，它们不能互相访问，需要一个Istio Gateway来建立连接
	// （通常在Gateway Server中使用AUTO_PASSTHROUGH模式），这是一个高级的配置，通过用于将Istio跨越多个clusters
	IstioNetwork = "topology.istio.io/network"

	// IstioCluster is a workload label that indicates the name of the cluster that contains the
	// workload. The label is added internally within the control plane to enable workload selection
	// based on cluster.
	IstioCluster = "topology.istio.io/cluster"
)
