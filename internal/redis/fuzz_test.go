// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"strings"
	"testing"
)

func FuzzParseSlotRanges(f *testing.F) {
	for _, s := range []string{"0-5460", "0-1,2-3 6", "", "[1234->-node]", "100-5", "0-9223372036854775807", "16384", "-1", "1--2"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		ranges, err := ParseSlotRanges(raw)
		if err != nil {
			return
		}
		for _, r := range ranges {
			if r.Start < 0 || r.End < r.Start {
				t.Fatalf("ParseSlotRanges(%q) returned invalid range %+v", raw, r)
			}
		}
	})
}

func FuzzCountSlotsFromRanges(f *testing.F) {
	for _, s := range []string{"", "42", "0-5460 10923-16383", "[123->-abc] 0-100", "100-5", "abc", "0-9223372036854775807"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		n := CountSlotsFromRanges(s)
		if withMarker := CountSlotsFromRanges(s + " [123->-abc]"); withMarker != n {
			t.Fatalf("CountSlotsFromRanges(%q) = %d, but %d after appending a migrating marker", s, n, withMarker)
		}
	})
}

func FuzzParseOpenSlots(f *testing.F) {
	for _, s := range []string{"[123->-abc]", "[456-<-def] 0-100", "[", "[]", "[99999999999999999999->-x]"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		slots := ParseOpenSlots(s)
		if len(slots) > len(splitFields(s)) {
			t.Fatalf("ParseOpenSlots(%q) returned more slots (%d) than fields", s, len(slots))
		}
		for _, slot := range slots {
			if slot < 0 {
				t.Fatalf("ParseOpenSlots(%q) returned negative slot %d", s, slot)
			}
		}
	})
}

func FuzzParseClusterNodesOutput(f *testing.F) {
	f.Add("07c37dfeb235213a872192d90877d0cd55635b91 127.0.0.1:30004@31004 slave e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 0 1426238317239 4 connected\n" +
		"e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 127.0.0.1:30001@31001 myself,master - 0 0 1 connected 0-5460 [123->-abc]")
	f.Add("")
	f.Add("a b c d e f g h")
	f.Fuzz(func(t *testing.T, out string) {
		nodes := ParseClusterNodesOutput(out)
		if len(nodes) > strings.Count(out, "\n")+1 {
			t.Fatalf("more nodes (%d) than input lines", len(nodes))
		}
		for _, n := range nodes {
			if n.ID == "" || n.State == "" {
				t.Fatalf("node with empty mandatory field: %+v", n)
			}
			if !strings.HasPrefix(n.Addr, n.IP) {
				t.Fatalf("IP %q is not a prefix of Addr %q", n.IP, n.Addr)
			}
		}
	})
}

func FuzzParseClusterCheckOutput(f *testing.F) {
	f.Add("[OK] All 16384 slots covered.\n[ERR] Nodes don't agree about configuration!\n[WARNING] Node has open slots")
	f.Add("")
	f.Fuzz(func(t *testing.T, out string) {
		res := parseClusterCheckOutput(out)
		for _, list := range [][]string{res.Errors, res.Warnings} {
			seen := make(map[string]struct{}, len(list))
			for _, e := range list {
				if _, dup := seen[e]; dup {
					t.Fatalf("duplicate entry %q", e)
				}
				seen[e] = struct{}{}
			}
		}
	})
}
