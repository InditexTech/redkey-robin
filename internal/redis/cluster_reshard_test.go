// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"testing"
	"time"
)

func TestClient_ClusterReshard_Success(t *testing.T) {
	originalFactory := newRedisCLICommand
	newRedisCLICommand = func(ctx context.Context, _ []string, env map[string]string) *RedisCLICommand {
		return newCLICommand(ctx, "sh", []string{"-c", `
printf 'Moving slot 0 from source to target\nDone.\n'
exit 0
`}, env)
	}
	defer func() { newRedisCLICommand = originalFactory }()

	client := NewClient("127.0.0.1:6379", "secret")
	result, err := client.ClusterReshard(context.Background(), "source-id", "target-id", 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.CommandCodeOutput != 0 {
		t.Fatalf("expected exit code 0, got %d", result.CommandCodeOutput)
	}
	if result.Output == "" {
		t.Fatal("expected non-empty output")
	}
}

func TestClient_ClusterReshard_NonZeroExit(t *testing.T) {
	originalFactory := newRedisCLICommand
	newRedisCLICommand = func(ctx context.Context, _ []string, env map[string]string) *RedisCLICommand {
		return newCLICommand(ctx, "sh", []string{"-c", `
printf 'ERR reshard failed\n'
exit 1
`}, env)
	}
	defer func() { newRedisCLICommand = originalFactory }()

	client := NewClient("127.0.0.1:6379", "")
	_, err := client.ClusterReshard(context.Background(), "src", "dst", 50)
	if err == nil {
		t.Fatal("expected error on non-zero exit code")
	}
}

func TestClient_ClusterReshard_Timeout(t *testing.T) {
	originalFactory := newRedisCLICommand
	newRedisCLICommand = func(ctx context.Context, _ []string, env map[string]string) *RedisCLICommand {
		return newCLICommand(ctx, "sh", []string{"-c", "sleep 60"}, env)
	}
	defer func() { newRedisCLICommand = originalFactory }()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	client := NewClient("127.0.0.1:6379", "")
	_, err := client.ClusterReshard(ctx, "src", "dst", 10)
	if err == nil {
		t.Fatal("expected error on timeout")
	}
}

func TestClient_ClusterReshard_UsesAuth(t *testing.T) {
	originalFactory := newRedisCLICommand
	newRedisCLICommand = func(ctx context.Context, _ []string, env map[string]string) *RedisCLICommand {
		return newCLICommand(ctx, "sh", []string{"-c", `
if [ "$REDISCLI_AUTH" != "mypass" ]; then
  echo "auth missing" >&2
  exit 9
fi
echo ok
exit 0
`}, env)
	}
	defer func() { newRedisCLICommand = originalFactory }()

	client := NewClient("127.0.0.1:6379", "mypass")
	result, err := client.ClusterReshard(context.Background(), "src", "dst", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.CommandCodeOutput != 0 {
		t.Fatalf("expected exit code 0, got %d", result.CommandCodeOutput)
	}
}

// --- CountSlotsFromRanges ---

func TestCountSlotsFromRanges(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  int
	}{
		{"empty", "", 0},
		{"single slot", "42", 1},
		{"single range", "0-5460", 5461},
		{"multiple ranges", "0-5460 5461-10922 10923-16383", 16384},
		{"mixed single and range", "0-5460 10000 10923-16383", 5461 + 1 + 5461},
		{"importing marker skipped", "[123->-abc123] 0-100", 101},
		{"migrating marker skipped", "[456-<-def456] 500-600", 101},
		{"with whitespace", "  0-99  200-299  ", 200},
		{"slot boundaries", "0 16383 1-16382", 16384},
		{"reversed range ignored", "100-5 0-9", 10},
		{"non-numeric token ignored", "abc 0-9", 10},
		{"comma-joined token ignored", "5,6 0-9", 10},
		{"out-of-range slot ignored", "16384 0-9", 10},
		{"out-of-range range ignored", "0-16384 0-9", 10},
		{"overflowing range ignored", "0-9223372036854775807", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := CountSlotsFromRanges(tc.input)
			if got != tc.want {
				t.Fatalf("CountSlotsFromRanges(%q) = %d, want %d", tc.input, got, tc.want)
			}
		})
	}
}

// --- splitFields ---

func TestSplitFields(t *testing.T) {
	cases := []struct {
		input string
		want  int
	}{
		{"", 0},
		{"hello", 1},
		{"a b c", 3},
		{"  spaced  out  ", 2},
		{"tabs\there", 2},
	}
	for _, tc := range cases {
		got := splitFields(tc.input)
		if len(got) != tc.want {
			t.Fatalf("splitFields(%q) returned %d fields, want %d", tc.input, len(got), tc.want)
		}
	}
}

// --- OwnsSlots ---

func TestOwnsSlots(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  bool
	}{
		{"empty", "", false},
		{"whitespace only", "  \t ", false},
		{"single slot", "42", true},
		{"range", "0-5460", true},
		{"only migrating marker", "[123->-abc123]", false},
		{"only importing marker", "[456-<-def456]", false},
		{"markers and range", "[123->-abc123] 0-100", true},
		{"malformed token is conservatively treated as ownership", "abc", true},
		{"reversed range is conservatively treated as ownership", "100-5", true},
		{"out-of-range slot is conservatively treated as ownership", "16384", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := OwnsSlots(tc.input); got != tc.want {
				t.Fatalf("OwnsSlots(%q) = %v, want %v", tc.input, got, tc.want)
			}
		})
	}
}
