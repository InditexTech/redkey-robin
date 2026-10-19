// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"fmt"
	"strings"
)

// NodeState is a snapshot of a node's own view of its cluster identity and dataset. It is used
// to detect nodes that come up carrying a stale identity persisted in nodes.conf (e.g. a pod that
// remounts a retained PVC from a previous incarnation) before they are integrated into a cluster.
type NodeState struct {
	// ID is the node's cluster ID (CLUSTER MYID).
	ID string
	// IsReplica reports whether the node believes it is a replica.
	IsReplica bool
	// KnownNodes is cluster_known_nodes from the node's own CLUSTER INFO (1 = knows only itself).
	KnownNodes int
	// OwnsSlots reports whether the node's own view assigns slots to itself.
	OwnsSlots bool
	// DBSize is the number of keys held by the node.
	DBSize int64
}

// Pristine reports whether the node is a brand-new, empty cluster member: a master that only
// knows itself, owns no slots and holds no keys. That is the state of a freshly created pod.
func (s NodeState) Pristine() bool {
	return !s.IsReplica && s.KnownNodes <= 1 && !s.OwnsSlots && s.DBSize == 0
}

// GetNodeState collects the node's own view of its cluster identity and dataset.
func (c *Client) GetNodeState(ctx context.Context) (NodeState, error) {
	var state NodeState

	nodes, err := c.GetClusterNodes(ctx)
	if err != nil {
		return state, err
	}
	found := false
	for _, n := range nodes {
		if strings.Contains(n.Flags, "myself") {
			state.ID = n.ID
			state.IsReplica = strings.Contains(n.Flags, "slave")
			state.OwnsSlots = CountSlotsFromRanges(n.Slots) > 0
			found = true
			break
		}
	}
	if !found {
		return state, fmt.Errorf("node %s: could not find myself in CLUSTER NODES output", c.addr)
	}

	info, err := c.GetClusterInfo(ctx)
	if err != nil {
		return state, err
	}
	state.KnownNodes = info.KnownNodes

	size, err := c.client.DBSize(ctx).Result()
	if err != nil {
		return state, fmt.Errorf("DBSIZE on %s: %w", c.addr, err)
	}
	state.DBSize = size
	return state, nil
}

// ResetToPristine wipes the node's cluster identity and dataset so it behaves like a brand-new
// empty master, and persists that state so it survives a restart. It returns the node's new ID.
//
// The sequence is:
//   - FLUSHALL when the node is a master: CLUSTER RESET is rejected on a master holding keys
//     (a read-only replica rejects FLUSHALL instead, and is flushed by the reset itself);
//   - CLUSTER RESET HARD: forgets every other node, drops all slots, resets the config epoch and
//     generates a new node ID, rewriting nodes.conf;
//   - FLUSHALL again, now that the node is a writable master. Resetting a replica empties its
//     dataset in memory only, without recording it in the AOF, so with appendonly enabled a
//     restart would reload the stale keys and the node would claim their (now unassigned in its
//     view) slots. The explicit FLUSHALL is written to the AOF and prevents that;
//   - SAVE, so an RDB-only configuration does not reload a stale snapshot either.
//
// It is idempotent: running it on an already pristine node only rotates its ID.
func (c *Client) ResetToPristine(ctx context.Context) (string, error) {
	isReplica, err := c.IsReplica(ctx)
	if err != nil {
		return "", fmt.Errorf("checking role of node %s before reset: %w", c.addr, err)
	}
	if !isReplica {
		if err := c.FlushAll(ctx); err != nil {
			return "", fmt.Errorf("flushing node %s before reset: %w", c.addr, err)
		}
	}
	if err := c.ClusterReset(ctx, true); err != nil {
		return "", fmt.Errorf("resetting cluster state of node %s: %w", c.addr, err)
	}
	if err := c.FlushAll(ctx); err != nil {
		return "", fmt.Errorf("flushing node %s after reset: %w", c.addr, err)
	}
	if err := c.Save(ctx); err != nil {
		return "", fmt.Errorf("persisting empty dataset on node %s: %w", c.addr, err)
	}
	id, err := c.ClusterMyID(ctx)
	if err != nil {
		return "", fmt.Errorf("reading new ID of node %s after reset: %w", c.addr, err)
	}
	return id, nil
}
