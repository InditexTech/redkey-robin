// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package integration_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	goredis "github.com/redis/go-redis/v9"

	"github.com/inditextech/redkey-robin/internal/redis"
)

// Helpers to run Robin's Redis primitives against real redis-server processes, covering Redis
// behaviors that neither miniredis nor envtest emulate (cluster gossip, persisted nodes.conf, AOF
// reloads). The binaries are built into bin/redis-<version>/ by `make redis`.

const (
	redisClusterSlots   = 16384
	redisConvergeWait   = 30 * time.Second
	redisPollInterval   = 200 * time.Millisecond
	redisBusPortOffset  = 10000
	redisStartupTimeout = 10 * time.Second
)

var (
	redisBinariesOnce sync.Once
	errRedisBinaries  error
)

// requireRedisBinaries makes redis-server and redis-cli available to the specs, preferring the ones
// built by `make redis` into bin/ over any found in PATH. redis-cli is also invoked by Robin itself
// (e.g. ClusterReshard), so bin/ is prepended to PATH.
func requireRedisBinaries() {
	GinkgoHelper()
	redisBinariesOnce.Do(func() {
		if dir := getFirstFoundRedisBinaryDir(); dir != "" {
			errRedisBinaries = os.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
			if errRedisBinaries != nil {
				return
			}
		}
		for _, bin := range []string{"redis-server", "redis-cli"} {
			if _, err := exec.LookPath(bin); err != nil {
				errRedisBinaries = fmt.Errorf("%s not found in bin/redis-* nor in PATH, run 'make redis': %w", bin, err)
				return
			}
		}
	})
	Expect(errRedisBinaries).NotTo(HaveOccurred())
}

// getFirstFoundRedisBinaryDir locates the first bin/redis-<version>/ directory holding redis-server.
func getFirstFoundRedisBinaryDir() string {
	matches, _ := filepath.Glob(filepath.Join("..", "..", "bin", "redis-*", "redis-server"))
	if len(matches) == 0 {
		return ""
	}
	dir, err := filepath.Abs(filepath.Dir(matches[0]))
	if err != nil {
		return ""
	}
	return dir
}

// redisServer is a real redis-server process configured like a Redkey persistent cluster node: data
// (AOF + nodes.conf) lives in dir, which plays the role of the pod's PVC and survives restarts.
type redisServer struct {
	name string
	port int
	dir  string
	cmd  *exec.Cmd
}

// freeRedisPort returns a port whose cluster bus port (port+10000) is free as well.
func freeRedisPort() int {
	GinkgoHelper()
	for range 50 {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		Expect(err).NotTo(HaveOccurred())
		port := l.Addr().(*net.TCPAddr).Port
		_ = l.Close()
		if port+redisBusPortOffset > 65535 {
			continue
		}
		bus, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port+redisBusPortOffset))
		if err != nil {
			continue
		}
		_ = bus.Close()
		return port
	}
	Fail("could not find a free port pair for redis-server")
	return 0
}

// startRedisServer creates and starts a node. The process is killed when the spec ends.
func startRedisServer(name string) *redisServer {
	GinkgoHelper()
	s := &redisServer{name: name, port: freeRedisPort(), dir: GinkgoT().TempDir()}
	// Mirrors the cluster directives Robin renders for persistent clusters (buildRedisConf).
	conf := strings.Join([]string{
		fmt.Sprintf("port %d", s.port),
		"bind 127.0.0.1",
		"dir " + s.dir,
		"logfile " + filepath.Join(s.dir, "redis.log"),
		"cluster-enabled yes",
		"cluster-node-timeout 5000",
		"cluster-require-full-coverage no",
		"cluster-allow-reads-when-down yes",
		"cluster-allow-replica-migration no",
		"cluster-config-file " + filepath.Join(s.dir, "nodes.conf"),
		"appendonly yes",
	}, "\n") + "\n"
	Expect(os.WriteFile(filepath.Join(s.dir, "redis.conf"), []byte(conf), 0o600)).To(Succeed())
	s.start()
	DeferCleanup(func() {
		if s.cmd != nil && s.cmd.Process != nil {
			_ = s.cmd.Process.Kill()
			_ = s.cmd.Wait()
		}
	})
	return s
}

func (s *redisServer) addr() string { return fmt.Sprintf("127.0.0.1:%d", s.port) }

func (s *redisServer) client() *redis.Client {
	c := redis.NewClient(s.addr(), "")
	DeferCleanup(c.Close)
	return c
}

func (s *redisServer) start() {
	GinkgoHelper()
	s.cmd = exec.Command("redis-server", filepath.Join(s.dir, "redis.conf"))
	Expect(s.cmd.Start()).To(Succeed(), "starting %s", s.name)
	c := redis.NewClient(s.addr(), "")
	defer func() { _ = c.Close() }()
	Expect(c.CheckConnection(context.Background(), int(redisStartupTimeout/redisPollInterval), redisPollInterval)).
		To(Succeed(), "%s did not start, log:\n%s", s.name, s.log())
}

// restart shuts the node down gracefully (fsyncing the AOF and nodes.conf, like a pod deletion)
// and starts it again on the same data directory, like a pod remounting its PVC.
func (s *redisServer) restart() {
	GinkgoHelper()
	s.stop()
	s.start()
}

func (s *redisServer) stop() {
	GinkgoHelper()
	// MaxRetries -1: SHUTDOWN closes the connection, and a retry would only hit a closed port.
	c := goredis.NewClient(&goredis.Options{Addr: s.addr(), MaxRetries: -1})
	_ = c.Do(context.Background(), "SHUTDOWN", "SAVE").Err()
	_ = c.Close()
	done := make(chan error, 1)
	go func() { done <- s.cmd.Wait() }()
	Eventually(done).WithTimeout(redisStartupTimeout).Should(Receive(), "%s did not shut down, log:\n%s", s.name, s.log())
	s.cmd = nil
}

func (s *redisServer) log() string {
	b, _ := os.ReadFile(filepath.Join(s.dir, "redis.log"))
	return string(b)
}

func (s *redisServer) id(ctx context.Context) string {
	GinkgoHelper()
	id, err := s.client().ClusterMyID(ctx)
	Expect(err).NotTo(HaveOccurred())
	return id
}

func (s *redisServer) state(ctx context.Context) redis.NodeState {
	GinkgoHelper()
	st, err := s.client().GetNodeState(ctx)
	Expect(err).NotTo(HaveOccurred())
	return st
}

// eventuallyRedis polls cond until it succeeds or redisConvergeWait expires.
func eventuallyRedis(cond func(g Gomega)) {
	GinkgoHelper()
	Eventually(cond).WithTimeout(redisConvergeWait).WithPolling(redisPollInterval).Should(Succeed())
}

// formRedisCluster assigns the 16384 slots evenly across primaries and meets every node.
func formRedisCluster(ctx context.Context, primaries ...*redisServer) {
	GinkgoHelper()
	per := redisClusterSlots / len(primaries)
	for i, p := range primaries {
		start := i * per
		end := start + per
		if i == len(primaries)-1 {
			end = redisClusterSlots
		}
		slots := make([]int, 0, end-start)
		for s := start; s < end; s++ {
			slots = append(slots, s)
		}
		Expect(p.client().ClusterAddSlots(ctx, slots...)).To(Succeed())
	}
	for _, p := range primaries[1:] {
		meetRedis(ctx, primaries[0], p)
	}
	waitRedisConverged(ctx, primaries...)
	eventuallyRedis(func(g Gomega) {
		for _, p := range primaries {
			info, err := p.client().GetClusterInfo(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(info.State).To(Equal("ok"), "%s cluster_state", p.name)
		}
	})
}

func meetRedis(ctx context.Context, from, to *redisServer) {
	GinkgoHelper()
	Expect(from.client().ClusterMeet(ctx, "127.0.0.1", to.port)).To(Succeed())
}

// waitRedisConverged waits until every node sees every other node by ID, outside handshake.
func waitRedisConverged(ctx context.Context, nodes ...*redisServer) {
	GinkgoHelper()
	ids := make([]string, 0, len(nodes))
	for _, n := range nodes {
		ids = append(ids, n.id(ctx))
	}
	eventuallyRedis(func(g Gomega) {
		for _, n := range nodes {
			cns, err := n.client().GetClusterNodes(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			seen := make([]string, 0, len(cns))
			for _, cn := range cns {
				if !strings.Contains(cn.Flags, "handshake") {
					seen = append(seen, cn.ID)
				}
			}
			g.Expect(seen).To(ContainElements(ids), "%s does not see every node yet", n.name)
		}
	})
}

// forgetRedisNode makes every node in from forget id.
func forgetRedisNode(ctx context.Context, id string, from ...*redisServer) {
	GinkgoHelper()
	for _, n := range from {
		if err := n.client().ClusterForget(ctx, id); err != nil {
			Expect(err.Error()).To(ContainSubstring("Unknown node"))
		}
	}
}

// reshardAllRedis moves every slot of src to dst with redis-cli --cluster reshard (as Robin does),
// once every node agrees on the configuration (redis-cli refuses to reshard otherwise).
func reshardAllRedis(ctx context.Context, src, dst *redisServer) {
	GinkgoHelper()
	eventuallyRedis(func(g Gomega) {
		res, err := src.client().ClusterCheck(ctx)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(res.Errors).To(BeEmpty())
	})
	srcID, dstID := src.id(ctx), dst.id(ctx)
	n, err := src.client().CountSlotsForNode(ctx, srcID)
	Expect(err).NotTo(HaveOccurred())
	_, err = src.client().ClusterReshard(ctx, srcID, dstID, n)
	Expect(err).NotTo(HaveOccurred(), "resharding %s -> %s", src.name, dst.name)
}

func writeRedisKeys(ctx context.Context, seed *redisServer, n int) {
	GinkgoHelper()
	cc := goredis.NewClusterClient(&goredis.ClusterOptions{Addrs: []string{seed.addr()}})
	defer func() { _ = cc.Close() }()
	for i := range n {
		Expect(cc.Set(ctx, fmt.Sprintf("key-%d", i), i, 0).Err()).To(Succeed())
	}
}

// expectRedisKeysReadable checks every key written by writeRedisKeys is still served.
func expectRedisKeysReadable(ctx context.Context, seed *redisServer, n int) {
	GinkgoHelper()
	eventuallyRedis(func(g Gomega) {
		cc := goredis.NewClusterClient(&goredis.ClusterOptions{Addrs: []string{seed.addr()}})
		defer func() { _ = cc.Close() }()
		for i := range n {
			g.Expect(cc.Get(ctx, fmt.Sprintf("key-%d", i)).Err()).To(Succeed(), "key-%d", i)
		}
	})
}
