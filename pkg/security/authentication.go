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

package security

import (
	"context"
	"errors"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"

	"istio.io/istio/pilot/pkg/features"
	"istio.io/istio/pkg/env"
)

var AuthPlaintext = env.Register("XDS_AUTH_PLAINTEXT", false,
	"authenticate plain text requests - used if Istiod is running on a secure/trusted network").Get()

// Authenticate authenticates the ADS request using the configured authenticators.
// Authenticate认证ADS request，使用提供的authenticators
// Returns the validated principals or an error.
// 返回合法的principals或者一个ewrror
// If no authenticators are configured, or if the request is on a non-secure
// stream ( 15010 ) - returns amn empty caller and no errors.
// 如果没有配置authenticators，或者请求是一个非安全的stream (15010) = 返回一个amn empty caller以及没有errors
func Authenticate(ctx context.Context, authenticators []Authenticator) (*Caller, error) {
	if !features.XDSAuth {
		return nil, nil
	}

	// authenticate - currently just checks that request has a certificate signed with the our key.
	// 认证 - 当前只检查请求有一个证书，用我们的key签名
	// Protected by flag to avoid breaking upgrades - should be enabled in multi-cluster/meshexpansion where
	// XDS is exposed.
	// 通过flag来保护来避免breaking upgrades - 应该在多集群/meshexpansion使能，当XDS暴露
	peerInfo, ok := peer.FromContext(ctx)
	if !ok {
		return nil, errors.New("invalid context")
	}
	// Not a TLS connection, we will not perform authentication
	// 不是一个TLS连接，不会执行认证
	// TODO: add a flag to prevent unauthenticated requests ( 15010 )
	// request not over TLS on the insecure port
	if _, ok := peerInfo.AuthInfo.(credentials.TLSInfo); !ok && !AuthPlaintext {
		return nil, nil
	}

	am := authenticationManager{
		Authenticators: authenticators,
	}
	if u := am.authenticate(ctx); u != nil {
		return u, nil
	}
	securityLog.Errorf("Failed to authenticate client from %s: %s", peerInfo.Addr.String(), am.FailedMessages())
	return nil, errors.New("authentication failure")
}
