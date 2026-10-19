// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package integration_test

import (
	"context"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Stale node state on reused volumes (integration, real Redis)", Label("redis"), func() {
	const (
		testKeys    = 200
		specTimeout = 5 * time.Minute
		// forgetBlacklistTTL is Redis' CLUSTER FORGET blacklist (60s) plus a safety margin.
		forgetBlacklistTTL = 62 * time.Second
	)

	BeforeEach(func() {
		requireRedisBinaries()
	})

	// rollingUpgradeLeavesStaleExtra reproduces the state a Rolling N+1 upgrade leaves behind on
	// the extra node of a persistent cluster:
	//   - the extra node joins and receives every slot of node 0;
	//   - node 0 is recycled (reset to a pristine node, as flushAndPersistNode does) and met back;
	//   - the extra node's slots are moved back to the (empty) node 0, which makes Redis demote the
	//     extra to a replica of node 0 even with cluster-allow-replica-migration no;
	//   - the extra node is forgotten and stopped (StatefulSet scale-down), its data dir (the
	//     retained PVC with deletePVC=false) keeping that stale replica identity.
	//
	// It returns the three primaries and the stopped extra node.
	rollingUpgradeLeavesStaleExtra := func(ctx context.Context) ([]*redisServer, *redisServer) {
		n0, n1, n2 := startRedisServer("node-0"), startRedisServer("node-1"), startRedisServer("node-2")
		formRedisCluster(ctx, n0, n1, n2)
		writeRedisKeys(ctx, n0, testKeys)

		extra := startRedisServer("extra")
		meetRedis(ctx, n0, extra)
		waitRedisConverged(ctx, n0, n1, n2, extra)

		reshardAllRedis(ctx, n0, extra)
		oldN0 := n0.id(ctx)
		_, err := n0.client().ResetToPristine(ctx)
		Expect(err).NotTo(HaveOccurred())
		forgetRedisNode(ctx, oldN0, n1, n2, extra)
		meetRedis(ctx, n1, n0)
		waitRedisConverged(ctx, n0, n1, n2, extra)

		reshardAllRedis(ctx, extra, n0)
		eventuallyRedis(func(g Gomega) {
			g.Expect(extra.client().IsReplica(ctx)).To(BeTrue(), "extra should be demoted to a replica once drained")
		})
		expectRedisKeysReadable(ctx, n0, testKeys)

		forgetRedisNode(ctx, extra.id(ctx), n0, n1, n2)
		extra.stop()
		return []*redisServer{n0, n1, n2}, extra
	}

	// Second Rolling N+1 upgrade: the extra node remounts the retained volume, comes up as a stale
	// replica, and must be reset to a pristine empty master before being met, after which it is a
	// valid reshard destination.
	It("resets an extra node resuming a stale replica identity into a valid reshard target", func(ctx SpecContext) {
		primaries, extra := rollingUpgradeLeavesStaleExtra(ctx)
		n0, n2 := primaries[0], primaries[2]

		extra.start()
		stale := extra.state(ctx)
		Expect(stale.IsReplica).To(BeTrue(), "remounted extra should resume as a replica")
		Expect(stale.KnownNodes).To(BeNumerically(">", 1))
		Expect(stale.Pristine()).To(BeFalse())

		newID, err := extra.client().ResetToPristine(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(newID).NotTo(Equal(stale.ID), "reset should generate a new node ID")
		Expect(extra.state(ctx).Pristine()).To(BeTrue())

		extra.restart()
		st := extra.state(ctx)
		Expect(st.Pristine()).To(BeTrue(), "reset should survive a restart: %+v", st)
		Expect(st.ID).To(Equal(newID))

		meetRedis(ctx, n0, extra)
		waitRedisConverged(ctx, append(primaries, extra)...)
		reshardAllRedis(ctx, n2, extra)
		st = extra.state(ctx)
		Expect(st.IsReplica).To(BeFalse())
		Expect(st.OwnsSlots).To(BeTrue())
		expectRedisKeysReadable(ctx, n0, testKeys)
	}, NodeTimeout(specTimeout))

	// Clusters already stuck by the bug: the stale extra was met under its old identity and the
	// cluster lists it as a replica of node 0. Resetting it and forgetting its old ID cluster-wide
	// lets it rejoin as the expected empty primary.
	It("recovers a stale extra node already met as a replica", func(ctx SpecContext) {
		primaries, extra := rollingUpgradeLeavesStaleExtra(ctx)
		n0, n1, n2 := primaries[0], primaries[1], primaries[2]

		// CLUSTER FORGET blacklists the forgotten ID for 60s. In a real cluster the stale extra is
		// met again by the NEXT upgrade, long after that; wait it out so node-0 accepts it.
		time.Sleep(forgetBlacklistTTL)

		extra.start()
		oldID := extra.id(ctx)
		meetRedis(ctx, n0, extra)
		waitRedisConverged(ctx, n0, n1, n2, extra)
		eventuallyRedis(func(g Gomega) {
			cns, err := n0.client().GetClusterNodes(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			replicaIDs := []string{}
			for _, cn := range cns {
				if strings.Contains(cn.Flags, "slave") {
					replicaIDs = append(replicaIDs, cn.ID)
				}
			}
			g.Expect(replicaIDs).To(ContainElement(oldID), "node-0 should list the stale extra as a replica")
		})

		_, err := extra.client().ResetToPristine(ctx)
		Expect(err).NotTo(HaveOccurred())
		forgetRedisNode(ctx, oldID, n0, n1, n2)
		meetRedis(ctx, n0, extra)
		waitRedisConverged(ctx, n0, n1, n2, extra)

		cns, err := n0.client().GetClusterNodes(ctx)
		Expect(err).NotTo(HaveOccurred())
		newID := extra.id(ctx)
		for _, cn := range cns {
			Expect(cn.ID).NotTo(Equal(oldID), "the old extra ID should be forgotten")
			if cn.ID == newID {
				Expect(cn.Flags).To(ContainSubstring("master"), "the reset extra should rejoin as a master")
			}
		}

		reshardAllRedis(ctx, n2, extra)
		expectRedisKeysReadable(ctx, n0, testKeys)
	}, NodeTimeout(specTimeout))

	// Recycling a drained node that Redis demoted to a replica (flushAndPersistNode). CLUSTER RESET
	// empties a replica's dataset in memory only; without a FLUSHALL recorded in the AOF the
	// restarted node reloads the stale keys and claims their slots.
	It("does not resurrect keys from the AOF after resetting a replica", func(ctx SpecContext) {
		n0, n1, n2 := startRedisServer("node-0"), startRedisServer("node-1"), startRedisServer("node-2")
		formRedisCluster(ctx, n0, n1, n2)
		writeRedisKeys(ctx, n0, testKeys)

		replica := startRedisServer("replica")
		meetRedis(ctx, n0, replica)
		waitRedisConverged(ctx, n0, n1, n2, replica)
		Expect(replica.client().ClusterReplicate(ctx, n0.id(ctx))).To(Succeed())
		eventuallyRedis(func(g Gomega) {
			st, err := replica.client().GetNodeState(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(st.IsReplica).To(BeTrue())
			g.Expect(st.DBSize).To(BeNumerically(">", 0), "replica should be synced with keys")
		})

		forgetRedisNode(ctx, replica.id(ctx), n0, n1, n2)
		_, err := replica.client().ResetToPristine(ctx)
		Expect(err).NotTo(HaveOccurred())
		replica.restart()

		st := replica.state(ctx)
		Expect(st.Pristine()).To(BeTrue(),
			"no keys should be reloaded nor slots claimed after restart: %+v\nlog:\n%s", st, replica.log())
	}, NodeTimeout(specTimeout))
})
