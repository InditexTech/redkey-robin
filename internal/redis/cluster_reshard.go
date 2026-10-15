// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
)

// ClusterReshardResult holds the outcome of redis-cli --cluster reshard.
type ClusterReshardResult struct {
	CommandCodeOutput int
	Output            string
}

// ClusterReshard executes redis-cli --cluster reshard to migrate a specified number of
// slots from a source node to a target node. It uses --cluster-yes for non-interactive
// mode and --cluster-pipeline for batched key migration. The addr parameter should be
// any reachable node in the cluster (typically the source or target).
//
// This is the mechanism used during a Rolling N+1 upgrade to move all slots from a victim
// node to the extra (destination) node before the victim is recycled.
func (c *Client) ClusterReshard(ctx context.Context, sourceNodeID, targetNodeID string, numSlots int) (*ClusterReshardResult, error) {
	args := []string{
		"--cluster", "reshard", c.addr,
		"--cluster-from", sourceNodeID,
		"--cluster-to", targetNodeID,
		"--cluster-slots", strconv.Itoa(numSlots),
		"--cluster-pipeline", strconv.Itoa(rebalancePipeline),
		"--cluster-replace",
		"--cluster-yes",
	}

	cmd := newRedisCLICommand(ctx, args, c.redisCLIEnv())
	cmd.Run()

	if cmd.Err != nil {
		if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, fmt.Errorf("redis-cli --cluster reshard canceled or timed out: %w", ctx.Err())
		}
		if cmd.ExitCode == -1 {
			return nil, fmt.Errorf("redis-cli --cluster reshard on %s: %w", c.addr, cmd.Err)
		}
	}

	if cmd.ExitCode != 0 {
		return nil, formatClusterCLIExitCodeError("reshard", c.addr, cmd.ExitCode, cmd.GetCombinedOutput())
	}

	return &ClusterReshardResult{
		CommandCodeOutput: cmd.ExitCode,
		Output:            cmd.GetCombinedOutput(),
	}, nil
}

// CountSlotsForNode returns the number of slots owned by the given node ID by parsing
// CLUSTER NODES output. It counts individual slots from slot range expressions.
func (c *Client) CountSlotsForNode(ctx context.Context, nodeID string) (int, error) {
	nodes, err := c.GetClusterNodes(ctx)
	if err != nil {
		return 0, err
	}
	for _, node := range nodes {
		if node.ID == nodeID {
			return CountSlotsFromRanges(node.Slots), nil
		}
	}
	return 0, fmt.Errorf("node %s not found in cluster", nodeID)
}

// CountSlotsFromRanges parses a slot range string (e.g. "0-5460 10923-16383") and
// returns the total number of individual slots covered. Migrating/importing markers
// and malformed or out-of-range tokens are ignored.
func CountSlotsFromRanges(slotStr string) int {
	total := 0
	for _, part := range splitFields(slotStr) {
		// Skip importing/migrating markers like [123->-nodeID] or [123-<-nodeID].
		if part[0] == '[' {
			continue
		}
		if rng, err := parseSlotToken(part); err == nil {
			total += rng.End - rng.Start + 1
		}
	}
	return total
}

// OwnsSlots reports whether a CLUSTER NODES slot field lists any slot ownership entry,
// ignoring migrating/importing markers ("[...]"). Unlike CountSlotsFromRanges it does not
// validate the entries: anything unparseable is treated as ownership, so guards against
// destructive operations (e.g. CLUSTER FORGET) stay conservative on unexpected input.
func OwnsSlots(slotStr string) bool {
	for _, part := range splitFields(slotStr) {
		if part[0] != '[' {
			return true
		}
	}
	return false
}

// ParseOpenSlots extracts slot numbers that are in a migrating or importing state
// from the Slots field of a ClusterNode. These are in the format [<slot>->-<nodeID>]
// (migrating) or [<slot>-<-<nodeID>] (importing).
func ParseOpenSlots(slotStr string) []int {
	var slots []int
	for _, part := range splitFields(slotStr) {
		if len(part) < 2 || part[0] != '[' {
			continue
		}
		// Extract slot number from [<slot>->-<id>] or [<slot>-<-<id>]
		inner := part[1:] // strip leading '['
		idx := 0
		for idx < len(inner) && inner[idx] >= '0' && inner[idx] <= '9' {
			idx++
		}
		if idx == 0 {
			continue
		}
		slot, err := strconv.Atoi(inner[:idx])
		if err != nil {
			continue
		}
		slots = append(slots, slot)
	}
	return slots
}

// splitFields splits a string by whitespace (same as strings.Fields but avoids import for this file).
func splitFields(s string) []string {
	var fields []string
	start := -1
	for i, c := range s {
		if c == ' ' || c == '\t' {
			if start >= 0 {
				fields = append(fields, s[start:i])
				start = -1
			}
		} else if start < 0 {
			start = i
		}
	}
	if start >= 0 {
		fields = append(fields, s[start:])
	}
	return fields
}

// FlushAll executes FLUSHALL on the Redis node.
func (c *Client) FlushAll(ctx context.Context) error {
	err := c.client.FlushAll(ctx).Err()
	if err != nil {
		return fmt.Errorf("FLUSHALL on %s: %w", c.addr, err)
	}
	return nil
}

// ConfigSet executes CONFIG SET on the Redis node.
func (c *Client) ConfigSet(ctx context.Context, param, value string) error {
	err := c.client.ConfigSet(ctx, param, value).Err()
	if err != nil {
		return fmt.Errorf("CONFIG SET %s on %s: %w", param, c.addr, err)
	}
	return nil
}
