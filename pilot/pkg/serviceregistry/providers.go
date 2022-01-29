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

package serviceregistry

// ProviderID defines underlying platform supporting service registry
// ProviderID定义了支持service registry的底层平台
type ProviderID string

const (
	// Mock is a service registry that contains 2 hard-coded test services
	// Mock是一个service registry，包含2个硬编码的test services
	Mock ProviderID = "Mock"
	// Kubernetes is a service registry backed by k8s API server
	// Kubernetes是一个service registry，后端是K8S API server
	Kubernetes ProviderID = "Kubernetes"
	// MCP is a service registry backed by MCP ServiceEntries
	// MCP是一个service registry，后端是MCP ServiceEntries
	MCP ProviderID = "MCP"
	// External is a service registry for externally provided ServiceEntries
	// External是一个service registry用于外部提供的ServiceEntries
	External = "External"
)
