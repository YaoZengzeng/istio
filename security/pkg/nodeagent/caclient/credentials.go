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

package caclient

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"google.golang.org/grpc/credentials"

	"istio.io/istio/pkg/security"
	"istio.io/istio/security/pkg/nodeagent/plugin/providers/google/stsclient"
	"istio.io/istio/security/pkg/stsservice"
	"istio.io/istio/security/pkg/stsservice/server"
	"istio.io/istio/security/pkg/stsservice/tokenmanager/google"
	"istio.io/pkg/log"
)

// TokenProvider is a grpc PerRPCCredentials that can be used to attach a JWT token to each gRPC call.
// TokenProvider can be used for XDS, which may involve token exchange through STS.
// TokenProvider是一个grpc PerRPCCredentials，可以用于关联一个JWT token到每个gRPC调用
// TokenProvider可以用于XDS，可能会包括通过STS的token exchange
type TokenProvider struct {
	opts *security.Options
	// TokenProvider can be used for XDS. Because CA is often used with
	// external systems and XDS is not often (yet?), many of the security options only apply to CA
	// communication. A more proper solution would be to have separate options for CA and XDS, but
	// this requires API changes.
	forCA bool
}

var _ credentials.PerRPCCredentials = &TokenProvider{}

// TODO add metrics
// TODO change package
func NewCATokenProvider(opts *security.Options) *TokenProvider {
	return &TokenProvider{opts, true}
}

func NewXDSTokenProvider(opts *security.Options) *TokenProvider {
	return &TokenProvider{opts, false}
}

func (t *TokenProvider) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	if t == nil {
		return nil, nil
	}
	// 首先从token provider中获取token
	token, err := t.GetToken()
	if err != nil {
		return nil, err
	}
	if token == "" {
		return nil, nil
	}
	if t.opts.TokenManager == nil {
		return map[string]string{
			"authorization": "Bearer " + token,
		}, nil
	}
	// 从TokenManager中获取map
	return t.opts.TokenManager.GetMetadata(t.forCA, t.opts.XdsAuthProvider, token)
}

// Allow the token provider to be used regardless of transport security; callers can determine whether
// this is safe themselves.
func (t *TokenProvider) RequireTransportSecurity() bool {
	return false
}

// GetToken fetches a token to attach to a request. Returning "", nil will cause no header to be
// added; while a non-nil error will block the request If the token selected is not found, no error
// will be returned, causing no authorization header to be set. This ensures that even if the JWT
// token is missing (for example, on a VM that has rebooted, causing the token to be removed from
// volatile memory), we can still proceed and allow other authentication methods to potentially
// handle the request, such as mTLS.
// GetToken获取一个token来关联到一个request，返回""，nil会导致没有header被添加，而非nil的错误会阻塞请求，如果
// 没有找到选中的token，不会有error返回，导致没有authorization header被设置，这确保及时没有JWT token，我们也可以
// 处理并且允许其他的authentication methods能潜在地处理请求，例如mTLS
func (t *TokenProvider) GetToken() (string, error) {
	if !t.forCA {
		return t.GetTokenForXDS()
	}
	// For CA, we have two modes, using the newer CredentialFetcher or just reading directly from file
	// 对于CA，我们有两种模式，使用更新的CredentialFetcher或者直接从文件中读取
	var token string
	if t.opts.CredFetcher != nil {
		var err error
		// 获取平台的credential，如果t.opts.CredFetcher不为空的话
		token, err = t.opts.CredFetcher.GetPlatformCredential()
		if err != nil {
			return "", fmt.Errorf("fetch platform credential: %v", err)
		}
	} else {
		if t.opts.JWTPath == "" {
			return "", nil
		}
		// 从JWTPath中读取
		tok, err := os.ReadFile(t.opts.JWTPath)
		if err != nil {
			log.Warnf("failed to fetch token from file: %v", err)
			return "", nil
		}
		token = strings.TrimSpace(string(tok))
	}

	// Regardless of where the token came from, we (optionally) can exchange the token for a different
	// one using the configured TokenExchanger.
	// 不管token来自哪里，我们（可选地）可以使用配置的TokenExchanger来交换一个不同的token
	return t.exchangeToken(token)
}

// GetTokenForXDS gets the token for the XDS flow.
func (t *TokenProvider) GetTokenForXDS() (string, error) {
	if t.opts.XdsAuthProvider == google.GCPAuthProvider {
		return t.getTokenForGCP()
	}
	// For XDS flow, when no token provider is specified, we only support reading from file.
	if t.opts.JWTPath == "" {
		return "", nil
	}
	tok, err := os.ReadFile(t.opts.JWTPath)
	if err != nil {
		log.Warnf("failed to fetch token from file: %v", err)
		return "", nil
	}
	return strings.TrimSpace(string(tok)), nil
}

func (t *TokenProvider) getTokenForGCP() (string, error) {
	var tok string
	var err error
	if t.opts.CredFetcher != nil {
		// When running at a non-k8s platform, use CredFetcher to get credential.
		tok, err = t.opts.CredFetcher.GetPlatformCredential()
		if err != nil {
			return "", fmt.Errorf("failed to fetch platform credential: %v", err)
		}
	} else {
		// When XDS auth provider is GCP, token is always required. We should return
		// err when failed to get a token.
		if t.opts.JWTPath == "" {
			return "", fmt.Errorf("the JWTPath is not set")
		}
		tokBytes, err := os.ReadFile(t.opts.JWTPath)
		if err != nil {
			return "", fmt.Errorf("failed to fetch token from file: %v", err)
		}
		tok = string(tokBytes)
	}
	// For XDS flow, the token exchange is different from that of the CA flow.
	if t.opts.TokenManager == nil {
		return "", fmt.Errorf("XDS token exchange is enabled but token manager is nil")
	}
	if strings.TrimSpace(tok) == "" {
		return "", fmt.Errorf("the token for XDS token exchange is empty")
	}
	params := security.StsRequestParameters{
		Scope:            stsclient.Scope,
		GrantType:        server.TokenExchangeGrantType,
		SubjectToken:     strings.TrimSpace(tok),
		SubjectTokenType: server.SubjectTokenType,
	}
	body, err := t.opts.TokenManager.GenerateToken(params)
	if err != nil {
		return "", fmt.Errorf("token manager failed to generate access token: %v", err)
	}
	respData := &stsservice.StsResponseParameters{}
	if err := json.Unmarshal(body, respData); err != nil {
		return "", fmt.Errorf("failed to unmarshal access token response data: %v", err)
	}
	return respData.AccessToken, nil
}

// exchangeToken exchanges the provided token using TokenExchanger, if configured. If not, the
// original token is returned.
// exchangeToken使用TokenExchange交换提供的token，如果配置的话，如果没有，则返回original token
func (t *TokenProvider) exchangeToken(token string) (string, error) {
	if t.opts.TokenExchanger == nil {
		return token, nil
	}
	return t.opts.TokenExchanger.ExchangeToken(token)
}
