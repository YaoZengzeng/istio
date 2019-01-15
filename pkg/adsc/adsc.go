// Copyright 2018 Istio Authors
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

package adsc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"sync"
	"time"

	xdsapi "github.com/envoyproxy/go-control-plane/envoy/api/v2"
	"github.com/envoyproxy/go-control-plane/envoy/api/v2/core"
	ads "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v2"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	types "github.com/gogo/protobuf/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Config for the ADS connection.
type Config struct {
	// Namespace defaults to 'default'
	Namespace string

	// Workload defaults to 'test'
	Workload string

	// Meta includes additional metadata for the node
	// Meta包含了node额外的元数据
	Meta map[string]string

	// NodeType defaults to sidecar. "ingress" and "router" are also supported.
	// NodeType默认为sidecar，"ingress"和"router"也同时支持
	NodeType string
	IP       string
}

// ADSC implements a basic client for ADS, for use in stress tests and tools
// or libraries that need to connect to Istio pilot or other ADS servers.
// ADSC实现了一个基本的ADS client，用于压力测试以及那些需要连接Istio pilot或者其他ADS servers的工具或库的测试
type ADSC struct {
	// Stream is the GRPC connection stream, allowing direct GRPC send operations.
	// Set after Dial is called.
	// Stream时GRPC connection stream，允许直接的GRPC发送服务
	// 在Dial被调用之后设置
	stream ads.AggregatedDiscoveryService_StreamAggregatedResourcesClient

	// NodeID is the node identity sent to Pilot.
	nodeID string

	done chan error

	certDir string
	url     string

	watchTime time.Time

	InitialLoad time.Duration

	TCPListeners  map[string]*xdsapi.Listener
	HTTPListeners map[string]*xdsapi.Listener
	Clusters      map[string]*xdsapi.Cluster
	Routes        map[string]*xdsapi.RouteConfiguration
	EDS           map[string]*xdsapi.ClusterLoadAssignment

	// Metadata has the node metadata to send to pilot.
	// If nil, the defaults will be used.
	// Metadata是发送给pilot的node的元数据
	// 如果为nil，则会使用默认的
	Metadata map[string]string

	rdsNames     []string
	clusterNames []string

	// Updates includes the type of the last update received from the server.
	// Updates包含从server接收到的最新更新的type
	Updates     chan string
	// 记录各个资源类型的版本信息
	VersionInfo map[string]string

	mutex sync.Mutex
}

const (
	typePrefix = "type.googleapis.com/envoy.api.v2."

	// Constants used for XDS

	// ClusterType is used for cluster discovery. Typically first request received
	clusterType = typePrefix + "Cluster"
	// EndpointType is used for EDS and ADS endpoint discovery. Typically second request.
	endpointType = typePrefix + "ClusterLoadAssignment"
	// ListenerType is sent after clusters and endpoints.
	listenerType = typePrefix + "Listener"
	// RouteType is sent after listeners.
	routeType = typePrefix + "RouteConfiguration"
)

var (
	// ErrTimeout is returned by Wait if no update is received in the given time.
	ErrTimeout = errors.New("timeout")
)

// Dial connects to a ADS server, with optional MTLS authentication if a cert dir is specified.
// Dial用于和一个ADS server连接，如果指定了cert dir的话，则增加可选的MTLS authentication
func Dial(url string, certDir string, opts *Config) (*ADSC, error) {
	adsc := &ADSC{
		done:        make(chan error),
		Updates:     make(chan string, 100),
		VersionInfo: map[string]string{},
		certDir:     certDir,
		url:         url,
	}
	if opts.Namespace == "" {
		opts.Namespace = "default"
	}
	if opts.NodeType == "" {
		opts.NodeType = "sidecar"
	}
	if opts.IP == "" {
		opts.IP = getPrivateIPIfAvailable().String()
	}
	if opts.Workload == "" {
		opts.Workload = "test-1"
	}

	// 根据Config构建nodeID
	adsc.nodeID = fmt.Sprintf("sidecar~%s~%s.%s~%s.svc.cluster.local", opts.IP,
		opts.Workload, opts.Namespace, opts.Namespace)

	err := adsc.Reconnect()
	return adsc, err
}

// Returns a private IP address, or unspecified IP (0.0.0.0) if no IP is available
func getPrivateIPIfAvailable() net.IP {
	addrs, _ := net.InterfaceAddrs()
	for _, addr := range addrs {
		var ip net.IP
		switch v := addr.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		default:
			continue
		}
		if !ip.IsLoopback() {
			return ip
		}
	}
	return net.IPv4zero
}

func tlsConfig(certDir string) (*tls.Config, error) {
	clientCert, err := tls.LoadX509KeyPair(certDir+"/cert-chain.pem",
		certDir+"/key.pem")
	if err != nil {
		return nil, err
	}

	serverCABytes, err := ioutil.ReadFile(certDir + "/root-cert.pem")
	if err != nil {
		return nil, err
	}
	serverCAs := x509.NewCertPool()
	if ok := serverCAs.AppendCertsFromPEM(serverCABytes); !ok {
		return nil, err
	}

	return &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      serverCAs,
		ServerName:   "istio-pilot.istio-system",
		VerifyPeerCertificate: func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
			return nil
		},
	}, nil
}

// Close the stream. Reconnect() can be called to restore the connection, to
// simulate envoy restart behavior.
func (a *ADSC) Close() {
	if a.stream != nil {
		a.stream.CloseSend()
	}
}

// Reconnect will reconnect after close.
// Reconnect会在close之后重新进行连接
func (a *ADSC) Reconnect() error {

	// TODO: pass version info, nonce properly
	var conn *grpc.ClientConn
	var err error
	if len(a.certDir) > 0 {
		tlsCfg, err := tlsConfig(a.certDir)
		if err != nil {
			return err
		}
		creds := credentials.NewTLS(tlsCfg)

		opts := []grpc.DialOption{
			// Verify Pilot cert and service account
			grpc.WithTransportCredentials(creds),
		}
		conn, err = grpc.Dial(a.url, opts...)
		if err != nil {
			return err
		}
	} else {
		conn, err = grpc.Dial(a.url, grpc.WithInsecure())
		if err != nil {
			return err
		}
	}
	if err != nil {
		return err
	}

	// 创建ads client
	xds := ads.NewAggregatedDiscoveryServiceClient(conn)
	// 从ads server获取资源
	edsstr, err := xds.StreamAggregatedResources(context.Background())
	if err != nil {
		return err
	}
	a.stream = edsstr
	go a.handleRecv()
	return nil
}

func (a *ADSC) update(m string) {
	select {
	case a.Updates <- m:
	default:
	}
}

func (a *ADSC) handleRecv() {
	for {
		// 从ads server获取资源
		msg, err := a.stream.Recv()
		if err != nil {
			log.Println("Connection closed ", err, a.nodeID)
			a.Close()
			a.WaitClear()
			a.Updates <- "close"
			return
		}

		// 从返回的response中解析出listeners，clusters，routes以及eds
		listeners := []*xdsapi.Listener{}
		clusters := []*xdsapi.Cluster{}
		routes := []*xdsapi.RouteConfiguration{}
		eds := []*xdsapi.ClusterLoadAssignment{}
		for _, rsc := range msg.Resources { // Any
			// 记录某个资源类型的版本
			a.VersionInfo[rsc.TypeUrl] = msg.VersionInfo
			valBytes := rsc.Value
			// 根据资源类型将资源解码
			if rsc.TypeUrl == listenerType {
				ll := &xdsapi.Listener{}
				proto.Unmarshal(valBytes, ll)
				listeners = append(listeners, ll)
			} else if rsc.TypeUrl == clusterType {
				ll := &xdsapi.Cluster{}
				proto.Unmarshal(valBytes, ll)
				clusters = append(clusters, ll)
			} else if rsc.TypeUrl == endpointType {
				ll := &xdsapi.ClusterLoadAssignment{}
				proto.Unmarshal(valBytes, ll)
				eds = append(eds, ll)
			} else if rsc.TypeUrl == routeType {
				ll := &xdsapi.RouteConfiguration{}
				proto.Unmarshal(valBytes, ll)
				routes = append(routes, ll)
			}
		}

		// TODO: add hook to inject nacks
		a.ack(msg)

		// 分别对接受到的listners, clusters, eds以及routes进行处理
		if len(listeners) > 0 {
			a.handleLDS(listeners)
		}
		if len(clusters) > 0 {
			a.handleCDS(clusters)
		}
		if len(eds) > 0 {
			a.handleEDS(eds)
		}
		if len(routes) > 0 {
			a.handleRDS(routes)
		}
	}

}

func (a *ADSC) handleLDS(ll []*xdsapi.Listener) {
	// 包含http proxy的listener
	lh := map[string]*xdsapi.Listener{}
	// 包含tcp proxy的listener
	lt := map[string]*xdsapi.Listener{}

	clusters := []string{}
	routes := []string{}
	ldsSize := 0

	// 遍历listeners
	for _, l := range ll {
		ldsSize += l.Size()
		f0 := l.FilterChains[0].Filters[0]
		// 根据filter的name进行处理
		if f0.Name == "mixer" {
			f0 = l.FilterChains[0].Filters[1]
		}
		if f0.Name == "envoy.tcp_proxy" {
			lt[l.Name] = l
			c := f0.Config.Fields["cluster"].GetStringValue()
			clusters = append(clusters, c)
			//log.Printf("TCP: %s -> %s", l.Name, c)
		} else if f0.Name == "envoy.http_connection_manager" {
			lh[l.Name] = l

			// Getting from config is too painful..
			port := l.Address.GetSocketAddress().GetPortValue()
			// 获取routes的名字
			routes = append(routes, fmt.Sprintf("%d", port))
			//log.Printf("HTTP: %s -> %d", l.Name, port)
		} else if f0.Name == "envoy.mongo_proxy" {
			// ignore for now
		} else {
			tm := &jsonpb.Marshaler{Indent: "  "}
			log.Println(tm.MarshalToString(l))
		}
	}

	log.Println("LDS: http=", len(lh), "tcp=", len(lt), "size=", ldsSize)
	// 如果routes的数目大于零，请求routes的相关信息
	if len(routes) > 0 {
		a.sendRsc(routeType, routes)
	}
	a.mutex.Lock()
	defer a.mutex.Unlock()
	a.HTTPListeners = lh
	a.TCPListeners = lt

	select {
	// 发送给a.Updates
	case a.Updates <- "lds":
	default:
	}
}

func (a *ADSC) handleCDS(ll []*xdsapi.Cluster) {

	cn := []string{}
	cdsSize := 0
	cds := map[string]*xdsapi.Cluster{}
	for _, c := range ll {
		cdsSize += c.Size()
		// 如果返回的Cluster类型为EDS，则保存
		if c.Type != xdsapi.Cluster_EDS {
			// TODO: save them
			continue
		}
		cn = append(cn, c.Name)
		cds[c.Name] = c
	}

	log.Println("CDS: ", len(cn), "size=", cdsSize)

	if len(cn) > 0 {
		// 对于EDS类型的cluster，发送eds请求
		a.sendRsc(endpointType, cn)
	}

	a.mutex.Lock()
	defer a.mutex.Unlock()
	a.Clusters = cds

	select {
	case a.Updates <- "cds":
	default:
	}
}

func (a *ADSC) node() *core.Node {
	n := &core.Node{
		Id: a.nodeID,
	}
	if a.Metadata == nil {
		n.Metadata = &types.Struct{
			Fields: map[string]*types.Value{
				"ISTIO_PROXY_VERSION": &types.Value{Kind: &types.Value_StringValue{StringValue: "1.0"}},
			}}
	} else {
		f := map[string]*types.Value{}

		for k, v := range a.Metadata {
			f[k] = &types.Value{Kind: &types.Value_StringValue{StringValue: v}}
		}
		n.Metadata = &types.Struct{
			Fields: f,
		}
	}
	return n
}

func (a *ADSC) handleEDS(eds []*xdsapi.ClusterLoadAssignment) {
	la := map[string]*xdsapi.ClusterLoadAssignment{}
	edsSize := 0
	for _, cla := range eds {
		edsSize += cla.Size()
		la[cla.ClusterName] = cla
	}

	log.Println("EDS: ", len(eds), "size=", edsSize)

	if a.InitialLoad == 0 {
		// first load - Envoy loads listeners after endpoints
		// Envoy在endpoints之后加载listeners
		a.stream.Send(&xdsapi.DiscoveryRequest{
			ResponseNonce: time.Now().String(),
			Node:          a.node(),
			TypeUrl:       listenerType,
		})
	}

	a.mutex.Lock()
	defer a.mutex.Unlock()
	a.EDS = la

	select {
	case a.Updates <- "eds":
	default:
	}
}

func (a *ADSC) handleRDS(configurations []*xdsapi.RouteConfiguration) {

	vh := 0
	rcount := 0
	size := 0

	httpClusters := []string{}
	rds := map[string]*xdsapi.RouteConfiguration{}

	// 遍历RouteConfiguration
	for _, r := range configurations {
		// 遍历RouteConfiguration的VirtualHosts
		for _, h := range r.VirtualHosts {
			vh++
			// 遍历VirtualHosts的Routes
			for _, rt := range h.Routes {
				rcount++
				// Example: match:<prefix:"/" > route:<cluster:"outbound|9154||load-se-154.local" ...
				//log.Println(rt.String())
				// 从route中获取cluster信息
				httpClusters = append(httpClusters, rt.GetRoute().GetCluster())
			}
		}
		rds[r.Name] = r
		size += r.Size()
	}
	if a.InitialLoad == 0 {
		a.InitialLoad = time.Since(a.watchTime)
		log.Println("RDS: ", len(configurations), "size=", size, "vhosts=", vh, "routes=", rcount, " time=", a.InitialLoad)
	} else {
		log.Println("RDS: ", len(configurations), "size=", size, "vhosts=", vh, "routes=", rcount)
	}

	a.mutex.Lock()
	defer a.mutex.Unlock()
	a.Routes = rds

	select {
	case a.Updates <- "rds":
	default:
	}

}

// WaitClear will clear the waiting events, so next call to Wait will get
// the next push type.
// WaitClear会清除waiting events，因此下一次调用Wait会获得下一个push类型
func (a *ADSC) WaitClear() {
	for {
		select {
		case <-a.Updates:
		default:
			return
		}
	}
}

// Wait for an update of the specified type. If type is empty, wait for next update.
// 等待给定类型的更新，如果类型为空，则等待下一次的更新
func (a *ADSC) Wait(update string, to time.Duration) (string, error) {
	t := time.NewTimer(to)

	for {
		select {
		case t := <-a.Updates:
			if len(update) == 0 || update == t {
				return t, nil
			}
		case <-t.C:
			return "", ErrTimeout
		}
	}
}

// EndpointsJSON returns the endpoints from pilot, formatted as json for debug.
func (a *ADSC) EndpointsJSON() string {
	out, _ := json.MarshalIndent(a.EDS, " ", " ")
	return string(out)
}

// Watch will start watching resources, starting with LDS. Based on the LDS response
// it will start watching RDS and CDS.
// Watch会开始监听resouces，从LDS开始，基于LDS的返回，它会开始监听RDS和CDS
func (a *ADSC) Watch() {
	a.watchTime = time.Now()
	a.stream.Send(&xdsapi.DiscoveryRequest{
		ResponseNonce: time.Now().String(),
		Node:          a.node(),
		TypeUrl:       clusterType,
	})
}

func (a *ADSC) sendRsc(typeurl string, rsc []string) {
	a.stream.Send(&xdsapi.DiscoveryRequest{
		ResponseNonce: time.Now().String(),
		Node:          a.node(),
		TypeUrl:       typeurl,
		ResourceNames: rsc,
	})
}

func (a *ADSC) ack(msg *xdsapi.DiscoveryResponse) {
	// 用msg.Nonce进行ack
	a.stream.Send(&xdsapi.DiscoveryRequest{
		ResponseNonce: msg.Nonce,
		TypeUrl:       msg.TypeUrl,
		Node:          a.node(),
	})
}
