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
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	"istio.io/istio/pkg/env"
	istiolog "istio.io/istio/pkg/log"
)

var securityLog = istiolog.RegisterScope("security", "security debugging")

const (
	// etc/certs files are used with external CA managing the certs,
	// i.e. mounted Secret or external plugin.
	// If present, FileMountedCerts should be true.

	// DefaultCertChainFilePath is the well-known path for an existing certificate chain file
	DefaultCertChainFilePath = "./etc/certs/cert-chain.pem"

	// DefaultKeyFilePath is the well-known path for an existing key file
	DefaultKeyFilePath = "./etc/certs/key.pem"

	// DefaultRootCertFilePath is the well-known path for an existing root certificate file
	// DefaultRootCertFilePath是已知的路径，对于一个已经存在的root证书文件
	DefaultRootCertFilePath = "./etc/certs/root-cert.pem"

	// WorkloadIdentitySocketPath is the well-known path to the Unix Domain Socket for SDS.
	WorkloadIdentitySocketPath = "./var/run/secrets/workload-spiffe-uds/socket"

	// CredentialNameSocketPath is the well-known path to the Unix Domain Socket for Credential Name.
	// CredentialNameSocketPath是well-known的UDS路径，对于Credentail Name
	CredentialNameSocketPath = "./var/run/secrets/credential-uds/socket"

	// CredentialMetaDataName is the name in node meta data.
	CredentialMetaDataName = "credential"

	// SDSExternalClusterName is the name of the cluster for external SDS connections which is defined via CredentialNameSocketPath
	SDSExternalClusterName = "sds-external"

	// SDSExternalCredentialPrefix is the prefix for the credentialName which will utilize external SDS connections defined via CredentialNameSocketPath
	SDSExternalCredentialPrefix = "sds://"

	// WorkloadIdentityCredentialsPath is the well-known path to a folder with workload certificate files.
	WorkloadIdentityCredentialsPath = "./var/run/secrets/workload-spiffe-credentials"

	// WorkloadIdentityCertChainPath is the well-known path to a workload certificate chain file.
	WorkloadIdentityCertChainPath = WorkloadIdentityCredentialsPath + "/cert-chain.pem"

	// WorkloadIdentityKeyPath is the well-known path to a workload key file.
	WorkloadIdentityKeyPath = WorkloadIdentityCredentialsPath + "/key.pem"

	// WorkloadIdentityRootCertPath is the well-known path to a workload root certificate file.
	// WorkloadIdentityRootCertPath是已知的路径，对于workload root证书文件
	WorkloadIdentityRootCertPath = WorkloadIdentityCredentialsPath + "/root-cert.pem"

	// GkeWorkloadCertChainFilePath is the well-known path for the GKE workload certificate chain file.
	// Quoted from https://cloud.google.com/traffic-director/docs/security-proxyless-setup#create-service:
	// "On creation, each Pod gets a volume at /var/run/secrets/workload-spiffe-credentials."
	GkeWorkloadCertChainFilePath = WorkloadIdentityCredentialsPath + "/certificates.pem"

	// GkeWorkloadKeyFilePath is the well-known path for the GKE workload certificate key file
	GkeWorkloadKeyFilePath = WorkloadIdentityCredentialsPath + "/private_key.pem"

	// GkeWorkloadRootCertFilePath is the well-known path for the GKE workload root certificate file
	GkeWorkloadRootCertFilePath = WorkloadIdentityCredentialsPath + "/ca_certificates.pem"

	// SystemRootCerts is special case input for root cert configuration to use system root certificates.
	SystemRootCerts = "SYSTEM"

	// RootCertReqResourceName is resource name of discovery request for root certificate.
	// RootCertReqResourceName是resource name。对于root ca的discovery req
	RootCertReqResourceName = "ROOTCA"

	// WorkloadKeyCertResourceName is the resource name of the discovery request for workload
	// identity.
	// WorkloadKeyCertResourceName是resource name，对于workload id的discovery req
	WorkloadKeyCertResourceName = "default"

	// GCE is Credential fetcher type of Google plugin
	GCE = "GoogleComputeEngine"

	// JWT is a Credential fetcher type that reads from a JWT token file
	// JWT是一个Credential fetcher的类型，从一个JWT token文件读取
	JWT = "JWT"

	// Mock is Credential fetcher type of mock plugin
	Mock = "Mock" // testing only

	// GoogleCAProvider uses the Google CA for workload certificate signing
	// GoogleCAProvider使用google CA用于workload证书签名
	GoogleCAProvider = "GoogleCA"

	// GoogleCASProvider uses the Google certificate Authority Service to sign workload certificates
	// GoogleCASProvider使用Google certificate Authority Service用于签名workload证书
	GoogleCASProvider = "GoogleCAS"

	// GkeWorkloadCertificateProvider uses the GKE workload certificates
	GkeWorkloadCertificateProvider = "GkeWorkloadCertificate"

	// FileRootSystemCACert is a unique resource name signaling that the system CA certificate should be used
	FileRootSystemCACert = "file-root:system"
)

// TODO: For 1.8, make sure MeshConfig is updated with those settings,
// they should be dynamic to allow migrations without restart.
// Both are critical.
var (
	// Require3PToken disables the use of K8S 1P tokens. Note that 1P tokens can be used to request
	// 3P TOKENS. A 1P token is the token automatically mounted by Kubelet and used for authentication with
	// the Apiserver.
	Require3PToken = env.Register("REQUIRE_3P_TOKEN", false,
		"Reject k8s default tokens, without audience. If false, default K8S token will be accepted")

	// TokenAudiences specifies a list of audiences for SDS trustworthy JWT. This is to make sure that the CSR requests
	// contain the JWTs intended for Citadel.
	TokenAudiences = strings.Split(env.Register("TOKEN_AUDIENCES", "istio-ca",
		"A list of comma separated audiences to check in the JWT token before issuing a certificate. "+
			"The token is accepted if it matches with one of the audiences").Get(), ",")
)

const (
	BearerTokenPrefix = "Bearer "

	K8sTokenPrefix = "Istio "

	// CertSigner info
	CertSigner = "CertSigner"

	// ImpersonatedIdentity declares the identity we are requesting a certificate on behalf of.
	// This is constrained to only allow identities in CATrustedNodeAccounts, and only to impersonate identities
	// on their node.
	ImpersonatedIdentity = "ImpersonatedIdentity"
)

type ImpersonatedIdentityContextKey struct{}

// Options provides all of the configuration parameters for secret discovery service
// and CA configuration. Used in both Istiod and Agent.
// Options提供了所有的配置参数，对于secret discovery service以及CA的配置，同时在Istiod和Agent中使用
// TODO: ProxyConfig should have most of those, and be passed to all components
// (as source of truth)
type Options struct {
	// CAEndpoint is the CA endpoint to which node agent sends CSR request.
	// CAEndpoint是CA endpoint，node agents发送CSR请求
	CAEndpoint string

	// CAEndpointSAN overrides the ServerName extracted from CAEndpoint.
	CAEndpointSAN string

	// The CA provider name.
	// CA provider的名字
	CAProviderName string

	// TrustDomain corresponds to the trust root of a system.
	// https://github.com/spiffe/spiffe/blob/master/standards/SPIFFE-ID.md#21-trust-domain
	TrustDomain string

	// WorkloadRSAKeySize is the size of a private key for a workload certificate.
	WorkloadRSAKeySize int

	// Whether to generate PKCS#8 private keys.
	Pkcs8Keys bool

	// OutputKeyCertToDir is the directory for output the key and certificate
	OutputKeyCertToDir string

	// ProvCert is the directory for client to provide the key and certificate to CA server when authenticating
	// with mTLS. This is not used for workload mTLS communication, and is
	// ProvCert是一个给client提供的目录，提供key以及证书到CA server，当用mTLS认证，这不是用于workload mTLS交互
	ProvCert string

	// ClusterID is the cluster where the agent resides.
	// Normally initialized from ISTIO_META_CLUSTER_ID - after a tortuous journey it
	// makes its way into the ClusterID metadata of Citadel gRPC request to create the cert.
	// Didn't find much doc - but I suspect used for 'central cluster' use cases - so should
	// match the cluster name set in the MC setup.
	ClusterID string

	// The type of Elliptical Signature algorithm to use
	// when generating private keys. Currently only ECDSA is supported.
	ECCSigAlg string

	// The type of curve to use when generating private keys with ECC. Currently only ECDSA is supported.
	ECCCurve string

	// FileMountedCerts indicates whether the proxy is using file
	// mounted certs created by a foreign CA. Refresh is managed by the external
	// CA, by updating the Secret or VM file. We will watch the file for changes
	// or check before the cert expires. This assumes the certs are in the
	// well-known ./etc/certs location.
	// FileMountedCerts表明是proxy使用文件挂载的certs，被一个foreign CA创建
	FileMountedCerts bool

	// PilotCertProvider is the provider of the Pilot certificate (PILOT_CERT_PROVIDER env)
	// Determines the root CA file to use for connecting to CA gRPC:
	// 决定用于连接CA gRPC的root CA文件
	// - istiod
	// - kubernetes
	// - custom
	// - none
	PilotCertProvider string

	// secret TTL.
	SecretTTL time.Duration

	// The ratio of cert lifetime to refresh a cert. For example, at 0.10 and 1 hour TTL,
	// we would refresh 6 minutes before expiration.
	SecretRotationGracePeriodRatio float64

	// STS port
	STSPort int

	// authentication provider specific plugins, will exchange the token
	// For example exchange long lived refresh with access tokens.
	// authentication provider特定的plugins，会交换token，例如交换long lived refresh的access tokens
	// Used by the secret fetcher when signing CSRs.
	// 被secret fetcher使用，当签名CSRs时
	// Optional; if not present the token will be used directly
	// 可选地，如果不存在，token会被直接使用
	TokenExchanger TokenExchanger

	// credential fetcher.
	CredFetcher CredFetcher

	// credential identity provider
	CredIdentityProvider string

	// Namespace corresponding to workload
	WorkloadNamespace string

	// Name of the Service Account
	ServiceAccount string

	// XDS auth provider
	XdsAuthProvider string

	// Token manager for the token exchange of XDS
	// Token manager用于XDS的token exchange
	TokenManager TokenManager

	// Cert signer info
	CertSigner string

	// Delay in reading certificates from file after the change is detected. This is useful in cases
	// where the write operation of key and cert take longer.
	FileDebounceDuration time.Duration

	// Root Cert read from the OS
	CARootPath string

	// The path for an existing certificate chain file
	CertChainFilePath string
	// The path for an existing key file
	KeyFilePath string
	// The path for an existing root certificate bundle
	RootCertFilePath string
}

// TokenManager contains methods for generating token.
// TokenManager包含方法用于生成token
type TokenManager interface {
	// GenerateToken takes STS request parameters and generates token. Returns
	// StsResponseParameters in JSON.
	// GenerateToken获取STS请求参数并且生成token，返回JSON格式的StsResponseParameters
	GenerateToken(parameters StsRequestParameters) ([]byte, error)
	// DumpTokenStatus dumps status of all generated tokens and returns status in JSON.
	// DumpTokenStatus dump所有生成的token并且返回status，以JSON的格式
	DumpTokenStatus() ([]byte, error)
	// GetMetadata returns the metadata headers related to the token
	GetMetadata(forCA bool, xdsAuthProvider, token string) (map[string]string, error)
}

// StsRequestParameters stores all STS request attributes defined in
// https://tools.ietf.org/html/draft-ietf-oauth-token-exchange-16#section-2.1
type StsRequestParameters struct {
	// REQUIRED. The value "urn:ietf:params:oauth:grant-type:token- exchange"
	// indicates that a token exchange is being performed.
	GrantType string
	// OPTIONAL. Indicates the location of the target service or resource where
	// the client intends to use the requested security token.
	Resource string
	// OPTIONAL. The logical name of the target service where the client intends
	// to use the requested security token.
	Audience string
	// OPTIONAL. A list of space-delimited, case-sensitive strings, that allow
	// the client to specify the desired Scope of the requested security token in the
	// context of the service or Resource where the token will be used.
	Scope string
	// OPTIONAL. An identifier, for the type of the requested security token.
	RequestedTokenType string
	// REQUIRED. A security token that represents the identity of the party on
	// behalf of whom the request is being made.
	SubjectToken string
	// REQUIRED. An identifier, that indicates the type of the security token in
	// the "subject_token" parameter.
	SubjectTokenType string
	// OPTIONAL. A security token that represents the identity of the acting party.
	ActorToken string
	// An identifier, that indicates the type of the security token in the
	// "actor_token" parameter.
	ActorTokenType string
}

// Client interface defines the clients need to implement to talk to CA for CSR.
// The Agent will create a key pair and a CSR, and use an implementation of this
// interface to get back a signed certificate. There is no guarantee that the SAN
// in the request will be returned - server may replace it.
type Client interface {
	CSRSign(csrPEM []byte, certValidTTLInSec int64) ([]string, error)
	Close()
	// Retrieve CA root certs If CA publishes API endpoint for this
	GetRootCertBundle() ([]string, error)
}

// SecretManager defines secrets management interface which is used by SDS.
type SecretManager interface {
	// GenerateSecret generates new secret for the given resource.
	//
	// The current implementation also watched the generated secret and trigger a callback when it is
	// near expiry. It will constructs the SAN based on the token's 'sub' claim, expected to be in
	// the K8S format. No other JWTs are currently supported due to client logic. If JWT is
	// missing/invalid, the resourceName is used.
	GenerateSecret(resourceName string) (*SecretItem, error)
}

// TokenExchanger provides common interfaces so that authentication providers could choose to implement their specific logic.
// TokenExchanger提供公共的接口，这样authentication providers会选择实现特定的逻辑
type TokenExchanger interface {
	// ExchangeToken provides a common interface to exchange an existing token for a new one.
	// ExchangeToken提供了一个公共的接口用于将一个已经存在的token交换为新的
	ExchangeToken(serviceAccountToken string) (string, error)
}

// SecretItem is the cached item in in-memory secret store.
type SecretItem struct {
	CertificateChain []byte
	PrivateKey       []byte

	RootCert []byte

	// ResourceName passed from envoy SDS discovery request.
	// "ROOTCA" for root cert request, "default" for key/cert request.
	ResourceName string

	CreatedTime time.Time

	ExpireTime time.Time
}

type CredFetcher interface {
	// GetPlatformCredential fetches workload credential provided by the platform.
	// GetPlatformCredential获取platform提供的workload credential
	GetPlatformCredential() (string, error)

	// GetIdentityProvider returns the name of the IdentityProvider that can authenticate the workload credential.
	// GetIdentityProvider返回IdentityProvider的名字，可以认证workload credential
	GetIdentityProvider() string

	// Stop releases resources and cleans up.
	// 停止释放资源并且清理
	Stop()
}

// AuthSource represents where authentication result is derived from.
type AuthSource int

const (
	AuthSourceClientCertificate AuthSource = iota
	AuthSourceIDToken
)

const (
	authorizationMeta = "authorization"
)

type AuthContext struct {
	// grpc context
	GrpcContext context.Context
	// http request
	Request *http.Request
}

// RemoteAddress returns the authenticated remote address from AuthContext.
func (ac *AuthContext) RemoteAddress() string {
	if ac.GrpcContext != nil {
		return GetConnectionAddress(ac.GrpcContext)
	} else if ac.Request != nil {
		return ac.Request.RemoteAddr
	}
	return ""
}

// Header returns the authenticated remote address from AuthContext.
func (ac *AuthContext) Header(header string) []string {
	if ac.GrpcContext != nil {
		if meta, ok := metadata.FromIncomingContext(ac.GrpcContext); ok {
			return meta.Get(header)
		}
	} else if ac.Request != nil {
		return ac.Request.Header.Values(header)
	}
	return nil
}

// Caller carries the identity and authentication source of a caller.
// Caller携带id以及一个caller的authentication source
type Caller struct {
	AuthSource AuthSource
	Identities []string

	KubernetesInfo KubernetesInfo
}

// KubernetesInfo defines Kubernetes specific information extracted from the caller.
// This involves additional metadata about the caller beyond just its SPIFFE identity.
type KubernetesInfo struct {
	PodName           string
	PodNamespace      string
	PodUID            string
	PodServiceAccount string
}

func (k KubernetesInfo) String() string {
	return fmt.Sprintf("Pod{Name: %s, Namespace: %s, UID: %s, ServiceAccount: %s}", k.PodName, k.PodNamespace, k.PodUID, k.PodServiceAccount)
}

// Authenticator determines the caller identity based on request context.
// Authenticator决定caller的id，基于请求的上下文
type Authenticator interface {
	Authenticate(ctx AuthContext) (*Caller, error)
	AuthenticatorType() string
}

// authenticationManager orchestrates all authenticators to perform authentication.
// authenticationManager编排所有的authenticators来执行认证
type authenticationManager struct {
	Authenticators []Authenticator
	// authFailMsgs contains list of messages that authenticator wants to record - mainly used for logging.
	// authFailMsgs保存一系列的messages，authenticators想要记录
	authFailMsgs []string
}

// Authenticate loops through all the configured Authenticators and returns if one of the authenticator succeeds.
// Authenticate遍历所有配置的Authenticators，并且返回如果一个authenticator成功
func (am *authenticationManager) authenticate(ctx context.Context) *Caller {
	req := AuthContext{GrpcContext: ctx}
	for _, authn := range am.Authenticators {
		u, err := authn.Authenticate(req)
		if u != nil && len(u.Identities) > 0 && err == nil {
			securityLog.Debugf("Authentication successful through auth source %v", u.AuthSource)
			return u
		}
		am.authFailMsgs = append(am.authFailMsgs, fmt.Sprintf("Authenticator %s: %v", authn.AuthenticatorType(), err))
	}
	return nil
}

func GetConnectionAddress(ctx context.Context) string {
	peerInfo, ok := peer.FromContext(ctx)
	peerAddr := "unknown"
	if ok {
		peerAddr = peerInfo.Addr.String()
	}
	return peerAddr
}

func (am *authenticationManager) FailedMessages() string {
	return strings.Join(am.authFailMsgs, "; ")
}

func ExtractBearerToken(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", fmt.Errorf("no metadata is attached")
	}

	authHeader, exists := md[authorizationMeta]
	if !exists {
		return "", fmt.Errorf("no HTTP authorization header exists")
	}

	for _, value := range authHeader {
		if strings.HasPrefix(value, BearerTokenPrefix) {
			return strings.TrimPrefix(value, BearerTokenPrefix), nil
		}
	}

	return "", fmt.Errorf("no bearer token exists in HTTP authorization header")
}

func ExtractRequestToken(req *http.Request) (string, error) {
	value := req.Header.Get(authorizationMeta)
	if value == "" {
		return "", fmt.Errorf("no HTTP authorization header exists")
	}

	if strings.HasPrefix(value, BearerTokenPrefix) {
		return strings.TrimPrefix(value, BearerTokenPrefix), nil
	}
	if strings.HasPrefix(value, K8sTokenPrefix) {
		return strings.TrimPrefix(value, K8sTokenPrefix), nil
	}

	return "", fmt.Errorf("no bearer token exists in HTTP authorization header")
}

// GetOSRootFilePath returns the first file path detected from a list of known CA certificate file paths.
// If none of the known CA certificate files are found, a warning in printed and an empty string is returned.
func GetOSRootFilePath() string {
	// Get and store the OS CA certificate path for Linux systems
	// Source of CA File Paths: https://golang.org/src/crypto/x509/root_linux.go
	certFiles := []string{
		"/etc/ssl/certs/ca-certificates.crt",                // Debian/Ubuntu/Gentoo etc.
		"/etc/pki/tls/certs/ca-bundle.crt",                  // Fedora/RHEL 6
		"/etc/ssl/ca-bundle.pem",                            // OpenSUSE
		"/etc/pki/tls/cacert.pem",                           // OpenELEC
		"/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem", // CentOS/RHEL 7
		"/etc/ssl/cert.pem",                                 // Alpine Linux
		"/usr/local/etc/ssl/cert.pem",                       // FreeBSD
		"/etc/ssl/certs/ca-certificates",                    // Talos Linux
	}

	for _, cert := range certFiles {
		if _, err := os.Stat(cert); err == nil {
			istiolog.Debugf("Using OS CA certificate for proxy: %s", cert)
			return cert
		}
	}
	istiolog.Warn("OS CA Cert could not be found for agent")
	return ""
}

// CheckWorkloadCertificate returns true when the workload certificate
// files are present under the provided paths. Otherwise, return false.
// CheckWorkloadCertificate返回true，当workload证书文件在给定的路径存在
func CheckWorkloadCertificate(certChainFilePath, keyFilePath, rootCertFilePath string) bool {
	if _, err := os.Stat(certChainFilePath); err != nil {
		return false
	}
	if _, err := os.Stat(keyFilePath); err != nil {
		return false
	}
	if _, err := os.Stat(rootCertFilePath); err != nil {
		return false
	}
	return true
}

type SdsCertificateConfig struct {
	CertificatePath   string
	PrivateKeyPath    string
	CaCertificatePath string
}

const (
	ResourceSeparator = "~"
)

// GetResourceName converts a SdsCertificateConfig to a string to be used as an SDS resource name
func (s SdsCertificateConfig) GetResourceName() string {
	if s.IsKeyCertificate() {
		return "file-cert:" + s.CertificatePath + ResourceSeparator + s.PrivateKeyPath // Format: file-cert:%s~%s
	}
	return ""
}

// GetRootResourceName converts a SdsCertificateConfig to a string to be used as an SDS resource name for the root
func (s SdsCertificateConfig) GetRootResourceName() string {
	if s.IsRootCertificate() {
		return "file-root:" + s.CaCertificatePath // Format: file-root:%s
	}
	return ""
}

// IsRootCertificate returns true if this config represents a root certificate config.
func (s SdsCertificateConfig) IsRootCertificate() bool {
	return s.CaCertificatePath != ""
}

// IsKeyCertificate returns true if this config represents key certificate config.
func (s SdsCertificateConfig) IsKeyCertificate() bool {
	return s.CertificatePath != "" && s.PrivateKeyPath != ""
}

// SdsCertificateConfigFromResourceName converts the provided resource name into a SdsCertificateConfig
// If the resource name is not valid, false is returned.
func SdsCertificateConfigFromResourceName(resource string) (SdsCertificateConfig, bool) {
	if strings.HasPrefix(resource, "file-cert:") {
		filesString := strings.TrimPrefix(resource, "file-cert:")
		split := strings.Split(filesString, ResourceSeparator)
		if len(split) != 2 {
			return SdsCertificateConfig{}, false
		}
		return SdsCertificateConfig{split[0], split[1], ""}, true
	} else if strings.HasPrefix(resource, "file-root:") {
		filesString := strings.TrimPrefix(resource, "file-root:")
		split := strings.Split(filesString, ResourceSeparator)

		if len(split) != 1 {
			return SdsCertificateConfig{}, false
		}
		return SdsCertificateConfig{"", "", split[0]}, true
	} else {
		return SdsCertificateConfig{}, false
	}
}

// SdsCertificateConfigFromResourceNameForOSCACert converts the OS resource name into a SdsCertificateConfig
func SdsCertificateConfigFromResourceNameForOSCACert(resource string) (SdsCertificateConfig, bool) {
	if resource == "" {
		return SdsCertificateConfig{}, false
	}
	return SdsCertificateConfig{"", "", resource}, true
}
