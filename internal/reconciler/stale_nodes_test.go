// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import (
	"testing"

	"github.com/inditextech/redkey-robin/internal/redis"
)

func TestShouldResetStaleNode(t *testing.T) {
	pristine := redis.NodeState{ID: "new", KnownNodes: 1}
	staleReplica := redis.NodeState{ID: "old", IsReplica: true, KnownNodes: 4, DBSize: 100}
	knowsPeers := redis.NodeState{ID: "old", KnownNodes: 4}
	ownsSlots := redis.NodeState{ID: "old", KnownNodes: 1, OwnsSlots: true}
	hasKeys := redis.NodeState{ID: "old", KnownNodes: 1, DBSize: 3}
	memberMaster := redis.NodeState{ID: "m", KnownNodes: 4}

	tests := []struct {
		name         string
		state        redis.NodeState
		isMember     bool
		mustBeMaster bool
		want         bool
	}{
		{"non-member pristine pod is left alone", pristine, false, true, false},
		{"non-member stale replica is reset", staleReplica, false, true, true},
		{"non-member stale replica is reset even when a replica is expected", staleReplica, false, false, true},
		{"non-member knowing peers is reset", knowsPeers, false, false, true},
		{"non-member owning slots is reset", ownsSlots, false, false, true},
		{"non-member holding keys is reset", hasKeys, false, false, true},
		{"member master is never reset (idempotent after meet)", memberMaster, true, true, false},
		{"member replica is kept when a replica is acceptable", staleReplica, true, false, false},
		{"member replica is reset when a master is required (stuck cluster recovery)", staleReplica, true, true, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldResetStaleNode(tt.state, tt.isMember, tt.mustBeMaster); got != tt.want {
				t.Fatalf("shouldResetStaleNode() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClusterMembershipContains(t *testing.T) {
	m := clusterMembership{
		ids:          map[string]struct{}{"abc": {}},
		handshakeIPs: map[string]struct{}{"10.0.0.9": {}},
	}
	if !m.contains("abc", "10.0.0.1:6379") {
		t.Error("expected established member to be contained")
	}
	if !m.contains("tmp", "10.0.0.9:6379") {
		t.Error("expected node mid-handshake (matched by IP) to be contained")
	}
	if m.contains("old", "10.0.0.5:6379") {
		t.Error("expected unknown node not to be contained")
	}
}
