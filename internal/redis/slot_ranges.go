// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

// SlotRange represents an inclusive Redis slot range.
type SlotRange struct {
	Start int
	End   int
}

// ParseSlotRanges parses a Redis slot specification into structured ranges.
func ParseSlotRanges(raw string) ([]SlotRange, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}

	tokens := strings.FieldsFunc(raw, func(r rune) bool {
		return unicode.IsSpace(r) || r == ','
	})
	ranges := make([]SlotRange, 0, len(tokens))
	for _, token := range tokens {
		rng, err := parseSlotToken(token)
		if err != nil {
			return nil, err
		}
		ranges = append(ranges, rng)
	}

	return ranges, nil
}

func parseSlotToken(token string) (SlotRange, error) {
	const maxSlot = TotalClusterSlots - 1
	if token == "" {
		return SlotRange{}, fmt.Errorf("empty slot token")
	}
	if strings.Contains(token, "[") || strings.Contains(token, "]") {
		return SlotRange{}, fmt.Errorf("unsupported slot token %q", token)
	}

	if strings.Contains(token, "-") {
		parts := strings.Split(token, "-")
		if len(parts) != 2 {
			return SlotRange{}, fmt.Errorf("invalid slot range %q", token)
		}
		start, err := strconv.Atoi(parts[0])
		if err != nil {
			return SlotRange{}, fmt.Errorf("invalid slot range %q: %w", token, err)
		}
		end, err := strconv.Atoi(parts[1])
		if err != nil {
			return SlotRange{}, fmt.Errorf("invalid slot range %q: %w", token, err)
		}
		if start < 0 || end < start || end > maxSlot {
			return SlotRange{}, fmt.Errorf("invalid slot range %q", token)
		}
		return SlotRange{Start: start, End: end}, nil
	}

	slot, err := strconv.Atoi(token)
	if err != nil {
		return SlotRange{}, fmt.Errorf("invalid slot token %q: %w", token, err)
	}
	if slot < 0 || slot > maxSlot {
		return SlotRange{}, fmt.Errorf("invalid slot token %q", token)
	}
	return SlotRange{Start: slot, End: slot}, nil
}

// CountSlots returns the total number of slots represented by the ranges.
func CountSlots(ranges []SlotRange) int {
	total := 0
	for _, rng := range ranges {
		total += rng.End - rng.Start + 1
	}
	return total
}
