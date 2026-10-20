// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import (
	"reflect"

	redisv1 "github.com/inditextech/redkey-operator/api/v1beta1"
)

// SupersedingResult holds the outcome of the superseding process.
type SupersedingResult struct {
	// Selected is the final config to process after superseding.
	Selected *redisv1.RedkeyConfig
	// Superseded contains configs that should be marked as Superseded (in order).
	Superseded []*redisv1.RedkeyConfig
}

// ApplySuperseding evaluates whether the selected config can be superseded by
// a later config in the list. It returns the final config to process and the
// list of configs that should be marked as Superseded.
//
// Pre-conditions for superseding to start:
//   - selected.Status.ConfigPhase is neither InProgress nor Applied.
//   - selected.Spec.SkipIfSuperseded is true.
//   - The change from the baseline (last Applied config) to selected only affects
//     Primaries and/or ReplicasPerPrimary (all other spec fields are identical).
//
// The chain continues forward as long as the next config also only changes
// Primaries and/or ReplicasPerPrimary vs. the baseline. The next config does NOT
// need SkipIfSuperseded to be selected — but it does need it for the chain to
// continue past it.
//
// configs must be sorted by Spec.Sequence ascending.
func ApplySuperseding(configs []redisv1.RedkeyConfig, selected *redisv1.RedkeyConfig) SupersedingResult {
	result := SupersedingResult{Selected: selected}

	// Pre-condition checks.
	if selected.Status.ConfigPhase == redisv1.ConfigPhaseInProgress ||
		selected.Status.ConfigPhase == redisv1.ConfigPhaseApplied {
		return result
	}
	if !selected.Spec.SkipIfSuperseded {
		return result
	}

	// Find the baseline: the last Applied config before selected.
	baseline := findBaseline(configs, selected)

	// Verify that the change from baseline to selected is topology-only.
	if baseline != nil && !onlyTopologyChange(baseline.Spec, selected.Spec) {
		return result
	}

	// Walk forward from selected, looking for subsequent configs that can supersede it.
	selectedIdx := indexOfConfig(configs, selected)
	if selectedIdx < 0 {
		return result
	}

	current := selected
	for next := selectedIdx + 1; next < len(configs); next++ {
		candidate := &configs[next]

		// The candidate must only differ in topology vs. the baseline.
		if baseline != nil && !onlyTopologyChange(baseline.Spec, candidate.Spec) {
			break
		}
		// If there is no baseline, compare candidate against the current selected.
		if baseline == nil && !onlyTopologyChange(current.Spec, candidate.Spec) {
			break
		}

		// Supersede current, advance to candidate.
		result.Superseded = append(result.Superseded, current)
		result.Selected = candidate
		current = candidate

		// The chain can continue only if the new current also has SkipIfSuperseded.
		if !current.Spec.SkipIfSuperseded {
			break
		}
	}

	return result
}

// ApplyScaleToZeroLookahead looks ahead of the selected config for a pending
// configuration that scales the cluster to zero primaries. If one exists, the
// LAST such config is selected and every non-terminal config from selected up
// to it is returned as Superseded, so Robin can jump straight to the simple and
// safe scale-to-zero operation instead of applying the intermediate changes.
//
// Unlike ApplySuperseding, this ignores SkipIfSuperseded and the topology-only
// restriction, and it can supersede a selected config that is already
// InProgress. This makes it an escape hatch for configs that are wedged
// InProgress (e.g. an upgrade to an invalid image).
//
// An InProgress config is only superseded when scaling to zero discards the
// cluster data (ephemeral cluster, or deletePVC=true on the scale-to-zero
// config). On storage clusters that retain their PVCs, abandoning an operation
// midway (e.g. with slots already moved to nodes that are not recreated) could
// leave persisted state that cannot be restored when scaling back up from zero,
// so Robin waits for the InProgress config to finish. Pending configs are
// always superseded, since they have not touched the cluster yet.
//
// No jump is performed when the selected config is InProgress and already
// targets zero primaries, since it is already scaling to zero.
//
// configs must be sorted by Spec.Sequence ascending.
func ApplyScaleToZeroLookahead(configs []redisv1.RedkeyConfig, selected *redisv1.RedkeyConfig) SupersedingResult {
	result := SupersedingResult{Selected: selected}
	if selected == nil {
		return result
	}
	if selected.Status.ConfigPhase == redisv1.ConfigPhaseApplied ||
		selected.Status.ConfigPhase == redisv1.ConfigPhaseSuperseded {
		return result
	}
	if selected.Status.ConfigPhase == redisv1.ConfigPhaseInProgress && selected.Spec.Primaries == 0 {
		return result
	}

	selectedIdx := indexOfConfig(configs, selected)
	if selectedIdx < 0 {
		return result
	}

	zeroIdx := -1
	for i := len(configs) - 1; i > selectedIdx; i-- {
		phase := configs[i].Status.ConfigPhase
		if (phase == redisv1.ConfigPhasePending || phase == "") && configs[i].Spec.Primaries == 0 {
			zeroIdx = i
			break
		}
	}
	if zeroIdx < 0 {
		return result
	}
	if selected.Status.ConfigPhase == redisv1.ConfigPhaseInProgress && !scaleToZeroDiscardsData(&configs[zeroIdx]) {
		return result
	}

	// Supersede the selected config itself (it is the slice element) and every
	// non-terminal config between it and the scale-to-zero target.
	for i := selectedIdx; i < zeroIdx; i++ {
		phase := configs[i].Status.ConfigPhase
		if phase == redisv1.ConfigPhaseApplied || phase == redisv1.ConfigPhaseSuperseded {
			continue
		}
		if i == selectedIdx {
			result.Superseded = append(result.Superseded, selected)
			continue
		}
		result.Superseded = append(result.Superseded, &configs[i])
	}
	result.Selected = &configs[zeroIdx]
	return result
}

// scaleToZeroDiscardsData reports whether applying the given scale-to-zero
// config leaves no cluster data behind: the cluster is ephemeral or its PVCs
// are deleted. A nil deletePVC keeps the PVCs, as in handleScalingToZero.
func scaleToZeroDiscardsData(cfg *redisv1.RedkeyConfig) bool {
	return cfg.Spec.Ephemeral || (cfg.Spec.DeletePVC != nil && *cfg.Spec.DeletePVC)
}

// findBaseline returns the last Applied config with a sequence lower than
// selected. Returns nil if no such config exists.
func findBaseline(configs []redisv1.RedkeyConfig, selected *redisv1.RedkeyConfig) *redisv1.RedkeyConfig {
	var baseline *redisv1.RedkeyConfig
	for i := range configs {
		if configs[i].Spec.Sequence >= selected.Spec.Sequence {
			break
		}
		if configs[i].Status.ConfigPhase == redisv1.ConfigPhaseApplied {
			baseline = &configs[i]
		}
	}
	return baseline
}

// indexOfConfig returns the index of cfg in configs, matched by Spec.Sequence.
// Returns -1 if not found.
func indexOfConfig(configs []redisv1.RedkeyConfig, cfg *redisv1.RedkeyConfig) int {
	for i := range configs {
		if configs[i].Spec.Sequence == cfg.Spec.Sequence {
			return i
		}
	}
	return -1
}

// onlyTopologyChange returns true if the only differences between a and b are
// in Primaries and/or ReplicasPerPrimary (and the control fields Sequence and
// SkipIfSuperseded, which are always expected to differ).
// All other spec fields must be identical.
func onlyTopologyChange(a, b redisv1.RedkeyConfigSpec) bool {
	// Normalise the fields we allow to differ.
	aNorm := a
	bNorm := b

	aNorm.Sequence = 0
	bNorm.Sequence = 0
	aNorm.SkipIfSuperseded = false
	bNorm.SkipIfSuperseded = false
	aNorm.Primaries = 0
	bNorm.Primaries = 0
	aNorm.ReplicasPerPrimary = 0
	bNorm.ReplicasPerPrimary = 0

	return reflect.DeepEqual(aNorm, bNorm)
}
