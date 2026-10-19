// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import (
	"context"
	"fmt"
	"strings"

	redisv1 "github.com/inditextech/redkey-operator/api/v1beta1"

	"github.com/inditextech/redkey-robin/internal/kubernetes"
	"github.com/inditextech/redkey-robin/internal/redis"
)

// Stale node state on reused ordinals
//
// On persistent clusters with deletePVC=false the StatefulSet retains the PVC of every ordinal
// removed by a scale-down. When a later operation re-creates a pod at one of those ordinals — the
// extra (N+1) nodes of every Rolling N+1 upgrade, or the new nodes of a scale-up — the pod remounts
// the retained volume and Redis reloads its persisted nodes.conf and dataset. The node then comes up
// under its OLD identity: e.g. the extra primary of a previous upgrade is drained back to an empty
// node 0 at the end of that upgrade, which makes Redis demote it to a replica of node 0
// (sender_slots == migrated_our_slots overrides cluster-allow-replica-migration no), and that role is
// what it resumes with. Integrated as is, such a node joins as a replica instead of the empty primary
// the operation expects, and the operation stalls forever.
//
// The helpers below detect those nodes before they are met into the cluster and reset them to a
// pristine empty master, exactly what a brand-new pod (or an ephemeral cluster) provides for free.

// shouldResetStaleNode decides whether a node about to be integrated into the cluster must be reset
// to a pristine empty master.
//   - isMember: the node's current ID is a member of the live cluster. Members are never reset,
//     which keeps the decision idempotent across reconcile retries once a node has been met, except
//     when mustBeMaster is set and the member is a replica: that is a stale node that was already
//     met under its old identity (e.g. a cluster stuck by a previous Robin version) and must be
//     reset so it can rejoin as the expected empty primary.
//   - non-members are reset unless they are pristine (a genuinely new pod).
func shouldResetStaleNode(state redis.NodeState, isMember, mustBeMaster bool) bool {
	if isMember {
		return mustBeMaster && state.IsReplica
	}
	return !state.Pristine()
}

// clusterMembership is the seed's view of who belongs to the cluster.
type clusterMembership struct {
	// ids holds the IDs of established members (no handshake, with an address).
	ids map[string]struct{}
	// handshakeIPs holds the IPs of nodes currently being met (handshake entries carry a
	// temporary ID, so the joining node can only be matched by IP).
	handshakeIPs map[string]struct{}
}

// contains reports whether the node with the given ID and address is a member of the cluster or
// is already being met into it. A node mid-handshake already knows its peers, so it must not be
// mistaken for a stale node and reset in a loop.
func (m clusterMembership) contains(id, addr string) bool {
	if _, ok := m.ids[id]; ok {
		return true
	}
	_, ok := m.handshakeIPs[extractIP(addr)]
	return ok
}

// getClusterMembership returns the cluster membership as seen by seed.
func getClusterMembership(ctx context.Context, seed *redis.Client) (clusterMembership, error) {
	nodes, err := seed.GetClusterNodes(ctx)
	if err != nil {
		return clusterMembership{}, err
	}
	m := clusterMembership{
		ids:          make(map[string]struct{}, len(nodes)),
		handshakeIPs: make(map[string]struct{}),
	}
	for _, n := range nodes {
		if strings.Contains(n.Flags, "handshake") {
			if n.IP != "" {
				m.handshakeIPs[n.IP] = struct{}{}
			}
			continue
		}
		if n.ID == "" || strings.Contains(n.Flags, "noaddr") {
			continue
		}
		m.ids[n.ID] = struct{}{}
	}
	return m, nil
}

// sanitizeExtraNodes resets the extra (N+1) upgrade pods that came up carrying stale cluster state
// from a retained PVC, before the scale-up phase meets them into the cluster. It must run BEFORE
// healTopology(meetMissing=true), which would otherwise meet a stale pod under its old identity.
//
// If the extra primary needs a reset, every extra pod is reset: the extra replicas hold no data
// that must survive this phase (they are attached to the extra primary right after) and a stale
// replica could otherwise still point at the old extra primary ID. Otherwise each extra replica is
// evaluated on its own. The old IDs of the reset nodes are then forgotten cluster-wide.
//
// It returns reset=true when at least one node was reset, so the caller requeues and re-evaluates
// the topology with the new identities.
func (cr *ClusterReconciler) sanitizeExtraNodes(ctx context.Context, config *redisv1.RedkeyConfig, password string) (bool, error) {
	originalTotal := config.Spec.Primaries + config.Spec.Primaries*config.Spec.ReplicasPerPrimary
	extra := upgradeExtraNodes(config)

	podAddrs, err := kubernetes.GetPodAddresses(ctx, cr.client, cr.clusterName, cr.namespace)
	if err != nil {
		return false, fmt.Errorf("getting pod addresses: %w", err)
	}
	seedAddr, ok := podAddrs[fmt.Sprintf("%s-0", cr.clusterName)]
	if !ok {
		return false, fmt.Errorf("pod %s-0 not found or has no IP", cr.clusterName)
	}
	seed := redis.NewClient(seedAddr, password)
	members, err := getClusterMembership(ctx, seed)
	_ = seed.Close()
	if err != nil {
		return false, fmt.Errorf("getting cluster members: %w", err)
	}

	type extraPod struct {
		name  string
		addr  string
		state redis.NodeState
		reset bool
	}
	pods := make([]extraPod, 0, extra)
	for i := range extra {
		name := fmt.Sprintf("%s-%d", cr.clusterName, originalTotal+i)
		addr, ok := podAddrs[name]
		if !ok {
			return false, fmt.Errorf("pod %s not found or has no IP", name)
		}
		c := redis.NewClient(addr, password)
		state, err := c.GetNodeState(ctx)
		_ = c.Close()
		if err != nil {
			return false, fmt.Errorf("getting state of extra pod %s: %w", name, err)
		}
		pods = append(pods, extraPod{
			name:  name,
			addr:  addr,
			state: state,
			reset: shouldResetStaleNode(state, members.contains(state.ID, addr), i == 0),
		})
	}

	if pods[0].reset {
		for i := range pods {
			pods[i].reset = true
		}
	}

	var staleIDs []string
	for _, p := range pods {
		if !p.reset {
			continue
		}
		cr.logger.Warn("Rolling upgrade: extra pod carries stale cluster state (retained PVC), resetting it to a pristine empty master",
			"pod", p.name, "staleID", p.state.ID, "replica", p.state.IsReplica,
			"knownNodes", p.state.KnownNodes, "ownsSlots", p.state.OwnsSlots, "keys", p.state.DBSize)
		c := redis.NewClient(p.addr, password)
		newID, err := c.ResetToPristine(ctx)
		_ = c.Close()
		if err != nil {
			return false, fmt.Errorf("resetting stale extra pod %s: %w", p.name, err)
		}
		cr.logger.Info("Rolling upgrade: stale extra pod reset", "pod", p.name, "oldID", p.state.ID, "newID", newID)
		staleIDs = append(staleIDs, p.state.ID)
	}

	cr.forgetStaleIDs(ctx, staleIDs, password, int(originalTotal+extra))
	return len(staleIDs) > 0, nil
}

// forgetStaleIDs makes every pod up to nodeCount forget the given (pre-reset) node IDs. Failures
// are only logged: a lingering entry for an old ID is unreachable under that ID and is cleaned up
// by membership healing (noaddr/fail ghost) on a later pass.
func (cr *ClusterReconciler) forgetStaleIDs(ctx context.Context, ids []string, password string, nodeCount int) {
	for _, id := range ids {
		if err := cr.forgetNodeFromAll(ctx, id, password, nodeCount); err != nil {
			cr.logger.Warn("Failed to forget stale node ID cluster-wide, continuing", "nodeID", id, "error", err)
		}
	}
}

// resetExtraNodesForRetention resets every extra (N+1) upgrade pod to a pristine empty master once
// it has been forgotten by the cluster at the end of the upgrade, so the PVC retained after the
// scale-down (deletePVC=false) holds a clean nodes.conf and dataset for the next upgrade. It is
// best-effort: sanitizeExtraNodes is the guarantee at the start of the next upgrade.
func (cr *ClusterReconciler) resetExtraNodesForRetention(ctx context.Context, config *redisv1.RedkeyConfig, password string) {
	if config.Spec.Ephemeral {
		return
	}
	originalTotal := config.Spec.Primaries + config.Spec.Primaries*config.Spec.ReplicasPerPrimary
	for i := range upgradeExtraNodes(config) {
		ordinal := originalTotal + i
		addr, err := cr.getPodAddr(ctx, ordinal)
		if err != nil {
			cr.logger.Warn("Rolling upgrade ending: cannot reach extra pod to reset it before scale down", "ordinal", ordinal, "error", err)
			continue
		}
		c := redis.NewClient(addr, password)
		if _, err := c.ResetToPristine(ctx); err != nil {
			cr.logger.Warn("Rolling upgrade ending: failed to reset extra pod before scale down", "ordinal", ordinal, "error", err)
		}
		_ = c.Close()
	}
}

// sanitizeScaleUpNodes resets the nodes that are not members of the live cluster but come up
// carrying stale cluster state (a scale-up re-creating ordinals whose PVCs were retained by an
// earlier scale-down). It only acts when the seed nodes[0] sees a formed cluster (slots assigned), so
// it never interferes with an initial formation or a restore from persisted state. Reset nodes get
// their new ID refreshed in place. It returns reset=true when at least one node was reset.
func (cr *ClusterReconciler) sanitizeScaleUpNodes(ctx context.Context, nodes []*redis.Node) (bool, error) {
	if len(nodes) == 0 {
		return false, nil
	}
	seed := nodes[0]
	info, err := seed.Client().GetClusterInfo(ctx)
	if err != nil {
		return false, fmt.Errorf("getting cluster info from seed node %s: %w", seed.Name, err)
	}
	if assigned := info.Raw()["cluster_slots_assigned"]; assigned == "" || assigned == "0" {
		return false, nil
	}
	members, err := getClusterMembership(ctx, seed.Client())
	if err != nil {
		return false, fmt.Errorf("getting cluster members: %w", err)
	}

	var staleIDs []string
	for _, n := range nodes {
		if members.contains(n.ID, n.Addr) {
			continue
		}
		state, err := n.Client().GetNodeState(ctx)
		if err != nil {
			return false, fmt.Errorf("getting state of node %s: %w", n.Name, err)
		}
		if !shouldResetStaleNode(state, false, false) {
			continue
		}
		cr.logger.Warn("Scale up: new pod carries stale cluster state (retained PVC), resetting it to a pristine empty master",
			"pod", n.Name, "staleID", state.ID, "replica", state.IsReplica,
			"knownNodes", state.KnownNodes, "ownsSlots", state.OwnsSlots, "keys", state.DBSize)
		newID, err := n.Client().ResetToPristine(ctx)
		if err != nil {
			return false, fmt.Errorf("resetting stale node %s: %w", n.Name, err)
		}
		cr.logger.Info("Scale up: stale pod reset", "pod", n.Name, "oldID", state.ID, "newID", newID)
		staleIDs = append(staleIDs, state.ID)
		n.ID = newID
	}

	for _, id := range staleIDs {
		for _, k := range nodes {
			if err := k.Client().ClusterForget(ctx, id); err != nil && !strings.Contains(err.Error(), "Unknown node") {
				cr.logger.Warn("Failed to forget stale node ID, continuing", "from", k.Name, "nodeID", id, "error", err)
			}
		}
	}
	return len(staleIDs) > 0, nil
}
