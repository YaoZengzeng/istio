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

package zipkin

import (
	"testing"

	"istio.io/istio/pkg/test/framework/components/cluster"
	"istio.io/istio/pkg/test/framework/resource"
)

// Instance represents a zipkin deployment on kube
// Instance代表在kube中的一个zipkin deployment
type Instance interface {
	resource.Resource

	// QueryTraces gets at most number of limit most recent available traces from zipkin.
	// QueryTraces获取最大数目的limit，大多数从zipkin中可用
	// spanName filters that only trace with the given span name will be included.
	// spanName进行过滤，只有给定的span name的会被包含
	QueryTraces(limit int, spanName, annotationQuery string) ([]Trace, error)
}

type Config struct {
	// Cluster to be used in a multicluster environment
	Cluster cluster.Cluster

	// HTTP Address of ingress gateway of the cluster to be used to install zipkin in.
	IngressAddr string
}

// Span represents a single span, which includes span attributes for verification
// TODO(bianpengyuan) consider using zipkin proto api https://github.com/istio/istio/issues/13926
type Span struct {
	SpanID       string
	ParentSpanID string
	ServiceName  string
	Name         string
	ChildSpans   []*Span
}

// Trace represents a trace by a collection of spans which all belong to that trace
type Trace struct {
	Spans []Span
}

// New returns a new instance of zipkin.
// New返回zipkin的一个新实例
func New(ctx resource.Context, c Config) (i Instance, err error) {
	return newKube(ctx, c)
}

// NewOrFail returns a new zipkin instance or fails test.
// NewOrFail返回一个新的zipkin实例或者测试失败
func NewOrFail(t *testing.T, ctx resource.Context, c Config) Instance {
	t.Helper()
	i, err := New(ctx, c)
	if err != nil {
		t.Fatalf("zipkin.NewOrFail: %v", err)
	}

	return i
}
