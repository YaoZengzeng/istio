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

package echo

import (
	"istio.io/istio/pkg/test/framework/resource"
)

// Instance is a component that provides access to a deployed echo service.
// Instance是一个component，提供对于部署的echo service的访问
type Instance interface {
	Caller
	Target
	resource.Resource

	// Address of the service (e.g. Kubernetes cluster IP). May be "" if headless.
	// service的地址（例如，Kubernetes cluster IP），可能为""，如果是headless
	Address() string

	// Addresses of service in dualmode
	// 在dualmode中的service的地址
	Addresses() []string

	// Restart restarts the workloads associated with this echo instance
	// Restart重启和这个echo instance相关的workloads
	Restart() error

	// UpdateWorkloadLabel update pod labels of this echo instance
	// UpdateWorkloadLabel更新这个echo instance的pod labels
	UpdateWorkloadLabel(add map[string]string, remove []string) error

	// WithWorkloads returns a target with only the specified subset of workloads
	// WithWorkloads返回一个target，只有特定的workloads的子集
	WithWorkloads(wl ...Workload) Instance
}
