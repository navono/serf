package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-msgpack/codec"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/serf/serf"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	g, err := newGateway("127.0.0.1:9999")
	if err != nil {
		return fmt.Errorf("error creating gateway: %v", err)
	}

	n1, err := consul_consul_setupSerfWAN("node1", "dc1", "127.0.0.11", 8888, "9.9.9.9", 9999, g)
	if err != nil {
		return fmt.Errorf("error creating node1: %v", err)
	}
	defer n1.Shutdown()

	n2, err := consul_consul_setupSerfWAN("node2", "dc2", "127.0.0.12", 8888, "9.9.9.9", 9999, g)
	if err != nil {
		return fmt.Errorf("error creating node2: %v", err)
	}
	defer n2.Shutdown()

	for {
		if _, err := n1.Join([]string{"node2.dc2/127.0.0.12:8888"}, true); err != nil {
			log.Printf("ERROR: having node1 join node2: %v", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		break
	}

	time.Sleep(5 * time.Second)

	log.Printf("NODE1 members: %s", printMembers(n1.Members()))
	log.Printf("NODE2 members: %s", printMembers(n2.Members()))

	return nil
}

func printMembers(members []serf.Member) string {
	var parts []string
	for _, m := range members {
		parts = append(parts, fmt.Sprintf("%s/%s:%d=%s~tags=%s", m.Name, m.Addr, m.Port, m.Status, m.Tags))
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

type ProxyReq struct {
	Type string // "packet" / "stream"
	Dest memberlist.Address
}

type Gateway struct {
	l      net.Listener
	logger *log.Logger

	nodesLock sync.Mutex
	nodes     map[string]*mgwTransport
}

func (g *Gateway) getNode(node string) *mgwTransport {
	g.nodesLock.Lock()
	defer g.nodesLock.Unlock()
	g.logger.Printf("GETNODE: %q", node)
	return g.nodes[node]
}

func (g *Gateway) AddNode(node string, t *mgwTransport) {
	g.nodesLock.Lock()
	defer g.nodesLock.Unlock()
	g.logger.Printf("ADDNODE: %q", node)
	g.nodes[node] = t
}

func (g *Gateway) Addr() string {
	return g.l.Addr().String()
}

func newGateway(addr string) (*Gateway, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	g := &Gateway{
		l:      l,
		logger: log.New(os.Stdout, "[gateway] ", log.LstdFlags),
		nodes:  make(map[string]*mgwTransport),
	}
	go g.mux()
	return g, nil
}

func (g *Gateway) mux() {
	g.logger.Printf("starting on %s", g.l.Addr())
	for {
		conn, err := g.l.Accept()
		if err != nil {
			g.logger.Printf("[ERROR] accept failed: %v", err)
			continue
		}

		go func() {
			headerLen := uint32(0)
			err = binary.Read(conn, binary.BigEndian, &headerLen)
			if err != nil {
				conn.Close()
				g.logger.Printf("[ERROR] accept failed: %v", err)
				return
			}

			var req ProxyReq
			dec := codec.NewDecoder(io.LimitReader(conn, int64(headerLen)), &codec.MsgpackHandle{})
			if err := dec.Decode(&req); err != nil {
				conn.Close()
				g.logger.Printf("[ERROR] accept failed: %v", err)
				return
			}

			dc, err := getDatacenter(req.Dest.Name)
			if err != nil {
				conn.Close()
				g.logger.Printf("[ERROR] accept failed: %v", err)
				return
			}

			g.logger.Printf("got %q request for dc=%q: %+v", req.Type, dc, req.Dest)
			defer g.logger.Printf("done with %q request for dc=%q: %+v", req.Type, dc, req.Dest)
			switch req.Type {
			case "packet":
				err = g.handlePacket(&req, conn)
			case "stream":
				err = g.handleStream(&req, conn)
			default:
				g.logger.Printf("UNKNOWN TYPE: %s", req.Type)
			}

			if err != nil {
				g.logger.Printf("[ERROR] handle failed: %v", err)
			}
		}()
	}
}

func (g *Gateway) handleStream(req *ProxyReq, conn net.Conn) error {
	nt := g.getNode(req.Dest.Name)
	if nt == nil {
		conn.Close()
		return io.EOF
	}
	g.logger.Printf("accepted stream to relay to %q", req.Dest.Name)

	return nt.IngestStream(conn)
}

func (g *Gateway) handlePacket(req *ProxyReq, conn net.Conn) error {
	nt := g.getNode(req.Dest.Name)
	if nt == nil {
		conn.Close()
		return io.EOF
	}
	g.logger.Printf("accepted packet to relay to %q", req.Dest.Name)

	return nt.IngestPacket(conn, conn.RemoteAddr(), time.Now())
}

func writeHeader(conn net.Conn, req *ProxyReq) error {
	var buf bytes.Buffer

	enc := codec.NewEncoder(&buf, &codec.MsgpackHandle{})
	if err := enc.Encode(req); err != nil {
		return err
	}

	header := buf.Bytes()

	err := binary.Write(conn, binary.BigEndian, uint32(len(header)))
	if err != nil {
		return err
	}
	_, err = conn.Write(header)
	if err != nil {
		return err
	}

	return nil
}

func consul_consul_setupSerfWAN(
	node, dc string,
	bindIP string, bindPort int,
	advIP string, advPort int,
	g *Gateway,
) (*serf.Serf, error) {
	conf := consul_consul_config_lib_SerfWANConfig()
	conf.Init()

	conf.MemberlistConfig.BindAddr = bindIP
	conf.MemberlistConfig.BindPort = bindPort

	conf.MemberlistConfig.AdvertiseAddr = advIP
	conf.MemberlistConfig.AdvertisePort = advPort

	conf.NodeName = fmt.Sprintf("%s.%s", node, dc)
	conf.Tags["dc"] = dc

	logger := log.New(os.Stdout, "["+conf.NodeName+"] ", log.LstdFlags)

	conf.MemberlistConfig.Logger = logger
	conf.Logger = logger
	conf.ProtocolVersion = 4
	conf.EnableNameConflictResolution = false

	// conf.SnapshotPath = filepath.Join(s.config.DataDir, path)
	// if err := os.MkdirAll(conf.SnapshotPath, 0755); err != nil {
	// 	return err
	// }

	nt, err := memberlist.NewNetTransport(&memberlist.NetTransportConfig{
		BindAddrs: []string{conf.MemberlistConfig.BindAddr},
		BindPort:  conf.MemberlistConfig.BindPort,
		Logger:    logger,
	})
	if err != nil {
		return nil, err
	}

	mgt := &mgwTransport{
		NodeAwareTransport: nt,
		logger:             logger,
		datacenter:         dc,
		gateway:            g,
	}
	g.AddNode(conf.NodeName, mgt)

	conf.MemberlistConfig.Transport = mgt

	return serf.Create(conf)
}

type mgwTransport struct {
	memberlist.NodeAwareTransport
	logger *log.Logger

	datacenter string
	gateway    *Gateway
}

var _ memberlist.NodeAwareTransport = (*mgwTransport)(nil)

func (t *mgwTransport) WriteToAddress(b []byte, addr memberlist.Address) (time.Time, error) {
	dc, err := getDatacenter(addr.Name)
	if err != nil {
		return time.Time{}, err
	}
	if dc != t.datacenter {
		t.logger.Printf("WriteToAddress seeing dest.dc=%q in src.dc=%q", dc, t.datacenter)

		gwConn, err := net.Dial("tcp", t.gateway.Addr())
		if err != nil {
			return time.Time{}, err
		}
		defer gwConn.Close()

		err = writeHeader(gwConn, &ProxyReq{Type: "packet", Dest: addr})
		if err != nil {
			return time.Time{}, err
		}

		if _, err = gwConn.Write(b); err != nil {
			return time.Time{}, err
		}

		return time.Now(), nil
	}

	return t.NodeAwareTransport.WriteToAddress(b, addr)
}

func (t *mgwTransport) DialAddressTimeout(addr memberlist.Address, timeout time.Duration) (net.Conn, error) {
	dc, err := getDatacenter(addr.Name)
	if err != nil {
		return nil, err
	}
	if dc != t.datacenter {
		t.logger.Printf("DialAddressTimeout seeing dest.dc=%q in src.dc=%q", dc, t.datacenter)

		gwConn, err := net.Dial("tcp", t.gateway.Addr())
		if err != nil {
			return nil, err
		}

		err = writeHeader(gwConn, &ProxyReq{Type: "stream", Dest: addr})
		if err != nil {
			gwConn.Close()
			return nil, err
		}

		return gwConn, nil
	}
	return t.NodeAwareTransport.DialAddressTimeout(addr, timeout)
}

func consul_consul_config_lib_SerfWANConfig() *serf.Config {
	conf := consul_lib_SerfDefaultConfig()
	conf.ReconnectTimeout = 3 * 24 * time.Hour
	conf.MemberlistConfig = memberlist.DefaultWANConfig()
	conf.MemberlistConfig.DeadNodeReclaimTime = 30 * time.Second
	conf.MemberlistConfig.RequireNodeNames = true
	return conf
}

func consul_lib_SerfDefaultConfig() *serf.Config {
	base := serf.DefaultConfig()

	// This effectively disables the annoying queue depth warnings.
	base.QueueDepthWarning = 1000000

	// This enables dynamic sizing of the message queue depth based on the
	// cluster size.
	base.MinQueueDepth = 4096

	// This gives leaves some time to propagate through the cluster before
	// we shut down. The value was chosen to be reasonably short, but to
	// allow a leave to get to over 99.99% of the cluster with 100k nodes
	// (using https://www.serf.io/docs/internals/simulator.html).
	base.LeavePropagateDelay = 3 * time.Second

	return base
}

func getDatacenter(node string) (string, error) {
	parts := strings.Split(node, ".")
	if len(parts) != 2 {
		return "", fmt.Errorf("node name does not encode a datacenter: %s", node)
	}
	return parts[1], nil
}
