// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import "testing"

func TestParseSlotRanges_ParsesWhitespaceAndCommas(t *testing.T) {
	ranges, err := ParseSlotRanges("0-1,2-3 6")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ranges) != 3 {
		t.Fatalf("expected 3 ranges, got %d", len(ranges))
	}
	if ranges[0] != (SlotRange{Start: 0, End: 1}) {
		t.Fatalf("unexpected first range %#v", ranges[0])
	}
	if ranges[2] != (SlotRange{Start: 6, End: 6}) {
		t.Fatalf("unexpected last range %#v", ranges[2])
	}
	if CountSlots(ranges) != 5 {
		t.Fatalf("expected 5 slots, got %d", CountSlots(ranges))
	}
}

func TestParseSlotRanges_EmptyInput(t *testing.T) {
	ranges, err := ParseSlotRanges("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ranges) != 0 {
		t.Fatalf("expected no ranges, got %d", len(ranges))
	}
}

func TestParseSlotRanges_RejectsUnsupportedTokens(t *testing.T) {
	_, err := ParseSlotRanges("[1234->-node]")
	if err == nil {
		t.Fatal("expected parse error for unsupported slot token")
	}
}

func TestParseSlotRanges_AcceptsSlotBoundaries(t *testing.T) {
	ranges, err := ParseSlotRanges("0 16383 0-16383")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []SlotRange{{Start: 0, End: 0}, {Start: 16383, End: 16383}, {Start: 0, End: 16383}}
	if len(ranges) != len(want) {
		t.Fatalf("expected %d ranges, got %d", len(want), len(ranges))
	}
	for i := range want {
		if ranges[i] != want[i] {
			t.Fatalf("range %d = %#v, want %#v", i, ranges[i], want[i])
		}
	}
}

func TestParseSlotRanges_RejectsInvalidSlots(t *testing.T) {
	for _, raw := range []string{
		"16384",
		"99999",
		"0-16384",
		"16384-16385",
		"0-9223372036854775807",
		"100-5",
		"-1",
		"0-5460 16384",
	} {
		if _, err := ParseSlotRanges(raw); err == nil {
			t.Fatalf("expected parse error for %q", raw)
		}
	}
}
