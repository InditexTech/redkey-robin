// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
)

func TestNodeState_Pristine(t *testing.T) {
	tests := []struct {
		name  string
		state NodeState
		want  bool
	}{
		{"fresh empty master", NodeState{KnownNodes: 1}, true},
		{"replica", NodeState{KnownNodes: 1, IsReplica: true}, false},
		{"knows other nodes", NodeState{KnownNodes: 3}, false},
		{"owns slots", NodeState{KnownNodes: 1, OwnsSlots: true}, false},
		{"holds keys", NodeState{KnownNodes: 1, DBSize: 1}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.state.Pristine(); got != tt.want {
				t.Fatalf("Pristine() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_GetNodeState_FailsOnClosedServer(t *testing.T) {
	mr := miniredis.RunT(t)
	c := NewClient(mr.Addr(), "")
	defer func() { _ = c.Close() }()
	mr.Close()

	if _, err := c.GetNodeState(context.Background()); err == nil {
		t.Fatal("expected error when server is closed")
	}
}

func TestClient_ResetToPristine_FailsOnClosedServer(t *testing.T) {
	mr := miniredis.RunT(t)
	c := NewClient(mr.Addr(), "")
	defer func() { _ = c.Close() }()
	mr.Close()

	if _, err := c.ResetToPristine(context.Background()); err == nil {
		t.Fatal("expected error when server is closed")
	}
}
