// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	redisv1 "github.com/inditextech/redkey-operator/api/v1beta1"
	"github.com/inditextech/redkey-robin/internal/kubernetes"
	"github.com/inditextech/redkey-robin/internal/redis"
)

// defaultRebalanceMaxAttempts bounds how many times a rebalance is retried when it
// fails with a transient migration error (TRYAGAIN and friends).
const defaultRebalanceMaxAttempts = 5

// fastScalingEligible reports whether the cluster qualifies for fast scaling, which
// recreates the cluster from scratch (losing all data) instead of migrating slots.
// It is only safe for ephemeral clusters with no replicas where the operator has
// explicitly opted in via purgeKeysOnRebalance.
func fastScalingEligible(config *redisv1.RedkeyConfig) bool {
	if !config.Spec.Ephemeral {
		return false
	}
	if config.Spec.ReplicasPerPrimary != 0 {
		return false
	}
	return config.Spec.PurgeKeysOnRebalance != nil && *config.Spec.PurgeKeysOnRebalance
}

// totalNodes returns the desired total node count for a topology.
func totalNodes(config *redisv1.RedkeyConfig) int {
	return int(config.Spec.Primaries + (config.Spec.Primaries * config.Spec.ReplicasPerPrimary))
}

// scaleUpTopology classifies the nodes seen during scale-up against the current cluster membership.
type scaleUpTopology struct {
	memberIDs map[string]struct{}
	// existingPrimaries are cluster members that are masters WITH slots assigned.
	existingPrimaries []*redis.Node
	// existingReplicas are cluster members that are replicas.
	existingReplicas []*redis.Node
	// pendingNodes are cluster members that are empty masters (met on a previous pass but
	// not yet configured) — these need to be assigned a role.
	pendingNodes []*redis.Node
	// newNodes are pods not yet in the cluster at all.
	newNodes []*redis.Node
}

// scaleUpPlan is the role assignment computed for a scale-up pass.
type scaleUpPlan struct {
	futurePrimaries []*redis.Node
	futureReplicas  []*redis.Node
	allPrimaries    []*redis.Node
	anchorNode      *redis.Node
}

// handleScalingUp grows the cluster: it adds the new primaries and replicas, rebalances
// the slots onto the new (empty) primaries, and finally attaches the replicas.
// See docs/operator-guide/scaling.md (Scale Up flow) for the detailed protocol.
func (cr *ClusterReconciler) handleScalingUp(ctx context.Context, config *redisv1.RedkeyConfig) (reconcileSchedule, error) {
	if fastScalingEligible(config) {
		return cr.handleFastScaling(ctx, config)
	}

	targetTotal := totalNodes(config)
	primaryCount := int(config.Spec.Primaries)
	cr.logger.Info("Starting scale-up reconciliation", "targetTotal", targetTotal, "primaries", primaryCount)

	// Phase 1: grow the StatefulSet so every new primary and replica pod exists.
	if schedule, done, err := cr.growStatefulSetForScaleUp(ctx, config, targetTotal); done {
		return schedule, err
	}

	password := cr.getPassword(ctx, config)
	nodes, err := cr.initNodes(ctx, config, password)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("initializing nodes for scale up: %w", err)
	}
	defer closeNodes(nodes)
	if len(nodes) < targetTotal {
		cr.logger.Info("Not all nodes available for scale up, will retry", "available", len(nodes), "expected", targetTotal)
		return reconcileAfterWaitInterval, nil
	}

	topo, err := cr.classifyScaleUpNodes(ctx, nodes)
	if err != nil {
		return reconcileAfterInterval, err
	}

	plan, schedule, ok := cr.planScaleUpRoles(ctx, config, topo, nodes)
	if !ok {
		return schedule, nil
	}

	// Introduce only the future primary nodes that are not yet in the cluster.
	// Pending nodes (empty masters from a previous pass) are already met.
	cr.updateSubstatus(ctx, config, redisv1.SubstatusInitializingNodes)
	if !cr.meetScaleUpPrimaries(ctx, topo, plan) {
		return reconcileAfterWaitInterval, nil
	}

	// Heal the topology before doing any slot work: forget the lingering entries of pods
	// recreated mid-operation (a recreated ephemeral pod rejoins with a new ID while its
	// old ID lingers in the gossip table, still owning slots, which makes redis-cli
	// rebalance across stale masters and never converge) and re-cover any orphaned slots.
	// meetMissing is false here: scale-up's empty-masters rebalance would otherwise hand
	// slots to a freshly met, not-yet-classified node destined to become a replica —
	// scale-up meets its own new nodes with explicit roles below.
	if changed, err := cr.healTopology(ctx, config, false); err != nil {
		cr.logger.Warn("Failed to heal topology during scale up", "error", err)
		return reconcileAfterWaitInterval, nil
	} else if changed {
		// Re-evaluate from a clean topology on the next pass.
		return reconcileImmediately, nil
	}

	// Phase 2: rebalance slots onto the new empty primaries.
	if !cr.rebalanceScaleUp(ctx, config, plan, password) {
		return reconcileAfterWaitInterval, nil
	}

	// Phase 3: attach the replicas.
	if !cr.attachScaleUpReplicas(ctx, config, nodes, topo, plan) {
		return reconcileAfterWaitInterval, nil
	}

	// Phase 4: validate.
	cr.updateSubstatus(ctx, config, redisv1.SubstatusVerifying)
	ok, err = cr.verifyCluster(ctx, nodes[0])
	if err != nil {
		cr.logger.Error("Failed to verify cluster after scale up", "error", err)
		return reconcileAfterWaitInterval, nil
	}
	if !ok {
		cr.logger.Info("Cluster not yet healthy after scale up, will retry")
		return reconcileAfterWaitInterval, nil
	}

	return cr.finishScaling(ctx, config, nodes, "scale up")
}

// growStatefulSetForScaleUp scales the StatefulSet to the target size and waits for every
// pod to be ready. When done is true the caller must return schedule and err as is.
func (cr *ClusterReconciler) growStatefulSetForScaleUp(
	ctx context.Context, config *redisv1.RedkeyConfig, targetTotal int,
) (schedule reconcileSchedule, done bool, err error) {
	cr.updateSubstatus(ctx, config, redisv1.SubstatusWaitingForPods)
	if err := kubernetes.ScaleStatefulSet(ctx, cr.client, cr.clusterName, cr.namespace, int32(targetTotal)); err != nil {
		return reconcileAfterInterval, true, fmt.Errorf("scaling StatefulSet up: %w", err)
	}
	ready, err := kubernetes.AllPodsReady(ctx, cr.client, cr.clusterName, cr.namespace, int32(targetTotal))
	if err != nil {
		return reconcileAfterInterval, true, fmt.Errorf("checking pod readiness during scale up: %w", err)
	}
	if !ready {
		cr.logger.Info("Waiting for new pods to be ready before scaling up", "expected", targetTotal)
		return reconcileAfterWaitInterval, true, nil
	}
	return 0, false, nil
}

// classifyScaleUpNodes splits the nodes into existing cluster members and truly new pods
// that have not yet been introduced to the cluster. This avoids the broken ordinal-based
// split which would try to convert existing replicas into primaries.
func (cr *ClusterReconciler) classifyScaleUpNodes(ctx context.Context, nodes []*redis.Node) (scaleUpTopology, error) {
	// Discover which nodes are already part of the cluster by querying an existing
	// member's CLUSTER NODES output. nodes[0] is always an existing primary (lowest
	// ordinal, created during the initial cluster formation).
	clusterMembers, err := nodes[0].Client().GetClusterNodes(ctx)
	if err != nil {
		return scaleUpTopology{}, fmt.Errorf("getting cluster nodes for topology discovery: %w", err)
	}
	topo := scaleUpTopology{memberIDs: make(map[string]struct{}, len(clusterMembers))}
	for _, cm := range clusterMembers {
		if cm.ID != "" && !strings.Contains(cm.Flags, "handshake") {
			topo.memberIDs[cm.ID] = struct{}{}
		}
	}

	for _, node := range nodes {
		if _, isMember := topo.memberIDs[node.ID]; !isMember {
			topo.newNodes = append(topo.newNodes, node)
			continue
		}
		if err := node.RefreshInfo(ctx); err != nil {
			return scaleUpTopology{}, fmt.Errorf("refreshing existing node %s: %w", node.Name, err)
		}
		switch {
		case node.IsReplica():
			topo.existingReplicas = append(topo.existingReplicas, node)
		case node.Slots != "":
			// Master with slots — a real existing primary.
			topo.existingPrimaries = append(topo.existingPrimaries, node)
		default:
			// Master with no slots — met on a previous pass but not yet assigned a
			// role (could be a new primary waiting for rebalance, or a replica-to-be
			// that was met but not yet CLUSTER REPLICATE'd).
			topo.pendingNodes = append(topo.pendingNodes, node)
		}
	}
	return topo, nil
}

// planScaleUpRoles decides which available nodes become primaries and which become
// replicas. When ok is false the caller must return schedule.
func (cr *ClusterReconciler) planScaleUpRoles(
	ctx context.Context, config *redisv1.RedkeyConfig, topo scaleUpTopology, nodes []*redis.Node,
) (plan scaleUpPlan, schedule reconcileSchedule, ok bool) {
	primaryCount := int(config.Spec.Primaries)

	// Nodes available for role assignment: pending (already met, prefer for primary role
	// since they're already in the cluster) + new (not yet met).
	availableNodes := append(append([]*redis.Node{}, topo.pendingNodes...), topo.newNodes...)

	// Determine how many new primaries we need.
	newPrimariesNeeded := max(primaryCount-len(topo.existingPrimaries), 0)
	if newPrimariesNeeded > len(availableNodes) {
		return scaleUpPlan{}, cr.handleScaleUpPrimaryShortage(ctx, config, topo, newPrimariesNeeded, len(availableNodes)), false
	}
	plan.futurePrimaries = availableNodes[:newPrimariesNeeded]
	plan.futureReplicas = availableNodes[newPrimariesNeeded:]

	// All primaries = existing + future primaries to add.
	plan.allPrimaries = append(topo.existingPrimaries, plan.futurePrimaries...)

	// anchorNode is the cluster member used to issue cluster-wide commands (cluster
	// fix, rebalance, slot-stability checks). It is normally an existing primary, but
	// after a chaos event that wipes every primary's slots (e.g. ephemeral pods that
	// restart and rejoin empty), existingPrimaries can be empty. In that case fall back
	// to the first available primary so scale-up can still drive recovery instead of
	// panicking on existingPrimaries[0].
	switch {
	case len(topo.existingPrimaries) > 0:
		plan.anchorNode = topo.existingPrimaries[0]
	case len(plan.allPrimaries) > 0:
		plan.anchorNode = plan.allPrimaries[0]
	default:
		plan.anchorNode = nodes[0]
	}

	cr.logger.Info("Scale-up topology discovery",
		"existingPrimaries", len(topo.existingPrimaries),
		"existingReplicas", len(topo.existingReplicas),
		"pendingNodes", len(topo.pendingNodes),
		"futurePrimaries", len(plan.futurePrimaries),
		"futureReplicas", len(plan.futureReplicas))
	return plan, 0, true
}

// handleScaleUpPrimaryShortage is called when there are not enough pending/new nodes to
// fill the primary role and returns how the scale-up should be requeued.
func (cr *ClusterReconciler) handleScaleUpPrimaryShortage(
	ctx context.Context, config *redisv1.RedkeyConfig, topo scaleUpTopology, newPrimariesNeeded, availableNodes int,
) reconcileSchedule {
	// A chaos event (e.g. a pod deleted mid-scale) can leave the cluster carrying
	// surplus replicas — nodes attached as replicas beyond what the target topology
	// requires — instead of the empty primaries scale-up expects. Those replicas are
	// cluster members with a master, so they are excluded from availableNodes and the
	// primary requirement can never be met from pending/new pods alone: scale-up would
	// dead-end here retrying forever (observed as a 2->3 scale stuck with
	// newPrimariesNeeded=2, availableNodes=1 and one stray replica). Promote the surplus
	// replicas to empty primaries via the shared replica-topology remediation (CLUSTER
	// RESET SOFT + re-MEET), then re-enter and reclassify on the healed topology so the
	// promoted nodes count as available primaries.
	desiredReplicaTotal := int(config.Spec.Primaries) * int(config.Spec.ReplicasPerPrimary)
	if surplusReplicas := len(topo.existingReplicas) - desiredReplicaTotal; surplusReplicas > 0 {
		cr.logger.Info("Not enough available nodes but surplus replicas can be promoted, remediating replica topology",
			"newPrimariesNeeded", newPrimariesNeeded, "availableNodes", availableNodes,
			"surplusReplicas", surplusReplicas)
		if _, err := cr.ensureReplicaTopology(ctx, config); err != nil {
			cr.logger.Warn("Failed to promote surplus replicas during scale up, will retry", "error", err)
			return reconcileAfterWaitInterval
		}
		return reconcileImmediately
	}
	cr.logger.Info("Not enough available nodes to satisfy primary requirement, will retry",
		"newPrimariesNeeded", newPrimariesNeeded, "availableNodes", availableNodes)
	return reconcileAfterWaitInterval
}

// meetScaleUpPrimaries meets the future primaries that are not yet cluster members and
// waits for gossip convergence. It returns false when the scale-up must be retried.
func (cr *ClusterReconciler) meetScaleUpPrimaries(ctx context.Context, topo scaleUpTopology, plan scaleUpPlan) bool {
	unmeetPrimaries := filterUnmet(plan.futurePrimaries, topo.memberIDs)
	if len(unmeetPrimaries) == 0 {
		return true
	}
	// Meet unmet primaries with all existing cluster members so gossip propagates.
	meetGroup := append(append([]*redis.Node{}, topo.existingPrimaries...), unmeetPrimaries...)
	if err := cr.meetNodes(ctx, meetGroup); err != nil {
		cr.logger.Error("Failed to meet new primary nodes during scale up", "error", err)
		return false
	}
	converged, err := cr.checkGossipConvergence(ctx, meetGroup)
	if err != nil {
		cr.logger.Warn("Failed to check gossip convergence during scale up", "error", err)
		return false
	}
	if !converged {
		cr.logger.Info("Gossip not yet converged during scale up, will retry")
		return false
	}
	return true
}

// rebalanceScaleUp moves slots onto the new empty primaries (TRYAGAIN-protected) or, when
// there are none, repairs orphaned slots. It returns false when the scale-up must be retried.
func (cr *ClusterReconciler) rebalanceScaleUp(
	ctx context.Context, config *redisv1.RedkeyConfig, plan scaleUpPlan, password string,
) bool {
	// Only run if there are futurePrimaries that need slots. On re-entry (e.g. after
	// Phase 3 gossip convergence failed), all primaries already have slots and no
	// rebalance is needed — running it would distribute slots to pending replica nodes
	// that are visible as empty masters but should become replicas.
	if len(plan.futurePrimaries) == 0 {
		// No new primaries need slots, but a primary killed mid-operation (chaos) can leave slots
		// orphaned on the existing topology — the surviving masters then cover only part of the
		// keyspace. rebalanceWithRetry, which repairs open/uncovered slots, only runs when there are
		// future primaries, so repair here directly. Without this, verifyCluster fails on the missing
		// coverage every pass and the scale-up loops forever (WaitingForPods -> InitializingNodes ->
		// Verifying, never reaching Ready). nil weights: this is not a drain, so slots may be recovered
		// onto any master.
		if err := cr.recoverOpenSlots(ctx, plan.anchorNode, password); err != nil {
			cr.logger.Warn("Failed to repair orphaned slots during scale up, will retry", "error", err)
			return false
		}
		return true
	}

	cr.updateSubstatus(ctx, config, redisv1.SubstatusRebalancing)
	cr.logger.Info("Starting rebalance to distribute slots to new primaries", "newPrimaries", len(plan.futurePrimaries))
	if err := cr.rebalanceWithRetry(ctx, config, plan.anchorNode, nil, false); err != nil {
		cr.logger.Error("Rebalance failed during scale up", "error", err)
		return false
	}

	// Wait until no slots remain in a migrating/importing state.
	stable, err := cr.slotsStable(ctx, plan.anchorNode)
	if err != nil {
		cr.logger.Warn("Failed to check slot stability during scale up", "error", err)
		return false
	}
	if !stable {
		cr.logger.Info("Slots still migrating during scale up, will retry")
		return false
	}
	return true
}

// attachScaleUpReplicas introduces future replica nodes and attaches ALL replicas (existing +
// future) to their correct primaries. Adding replicas after rebalance avoids replicating empty
// or in-flux nodes, and prevents the rebalance from assigning slots to replicas. It returns
// false when the scale-up must be retried.
func (cr *ClusterReconciler) attachScaleUpReplicas(
	ctx context.Context, config *redisv1.RedkeyConfig, nodes []*redis.Node, topo scaleUpTopology, plan scaleUpPlan,
) bool {
	allReplicas := append(topo.existingReplicas, plan.futureReplicas...)
	if config.Spec.ReplicasPerPrimary == 0 || len(allReplicas) == 0 {
		return true
	}

	cr.updateSubstatus(ctx, config, redisv1.SubstatusAttachingReplicas)
	unmeetReplicas := filterUnmet(plan.futureReplicas, topo.memberIDs)
	if len(unmeetReplicas) > 0 {
		// Meet unmet replicas with the existing cluster so they join with empty state.
		meetGroup := append(append([]*redis.Node{}, plan.allPrimaries...), unmeetReplicas...)
		if err := cr.meetNodes(ctx, meetGroup); err != nil {
			cr.logger.Warn("Failed to meet new replica nodes during scale up", "error", err)
			return false
		}
		allActive := append(append([]*redis.Node{}, plan.allPrimaries...), allReplicas...)
		converged, err := cr.checkGossipConvergence(ctx, allActive)
		if err != nil {
			cr.logger.Warn("Failed to check gossip convergence for replicas", "error", err)
			return false
		}
		if !converged {
			cr.logger.Info("Gossip not yet converged for replica nodes, will retry")
			return false
		}
	}

	if err := cr.refreshRoles(ctx, nodes); err != nil {
		cr.logger.Warn("Failed to refresh roles before attaching replicas", "error", err)
		return false
	}
	if err := cr.setReplicas(ctx, plan.allPrimaries, allReplicas, config.Spec.ReplicasPerPrimary); err != nil {
		cr.logger.Warn("Failed to attach replicas during scale up, will retry", "error", err)
		return false
	}
	return true
}

// scaleDownPlan splits the current nodes into the ones kept and the ones removed.
type scaleDownPlan struct {
	currentPrimaries []*redis.Node
	currentReplicas  []*redis.Node
	keepPrimaries    []*redis.Node
	drainPrimaries   []*redis.Node
	keepReplicas     []*redis.Node
	removeReplicas   []*redis.Node
}

// planScaleDown classifies nodes by current role — scale-down decisions must be
// topology-aware, not ordinal-based, to handle clusters where failovers have shuffled roles.
func planScaleDown(nodes []*redis.Node, targetPrimaries, targetReplicaCount int) scaleDownPlan {
	var p scaleDownPlan
	for _, n := range nodes {
		if n.IsPrimary() {
			p.currentPrimaries = append(p.currentPrimaries, n)
		} else {
			p.currentReplicas = append(p.currentReplicas, n)
		}
	}

	// Determine which primaries to keep and which to drain.
	// Keep the first targetPrimaries primaries (by ordinal, since they were sorted).
	if len(p.currentPrimaries) > targetPrimaries {
		p.keepPrimaries = p.currentPrimaries[:targetPrimaries]
		p.drainPrimaries = p.currentPrimaries[targetPrimaries:]
	} else {
		p.keepPrimaries = p.currentPrimaries
	}

	// Determine which replicas to keep: targetReplicas = targetPrimaries * replicasPerPrimary.
	if len(p.currentReplicas) > targetReplicaCount {
		p.keepReplicas = p.currentReplicas[:targetReplicaCount]
		p.removeReplicas = p.currentReplicas[targetReplicaCount:]
	} else {
		p.keepReplicas = p.currentReplicas
	}
	return p
}

// handleScalingDown shrinks the cluster: it drains the highest-ordinal primaries, removes
// the surplus replicas and primaries, and finally shrinks the StatefulSet.
// See docs/operator-guide/scaling.md (Scale Down flow) for the detailed protocol.
func (cr *ClusterReconciler) handleScalingDown(ctx context.Context, config *redisv1.RedkeyConfig) (reconcileSchedule, error) {
	targetTotal := totalNodes(config)
	targetPrimaries := int(config.Spec.Primaries)
	cr.logger.Info("Starting scale-down reconciliation", "targetTotal", targetTotal, "targetPrimaries", targetPrimaries)

	password := cr.getPassword(ctx, config)
	nodes, err := cr.initAllNodes(ctx, password)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("initializing nodes for scale down: %w", err)
	}
	defer closeNodes(nodes)
	if err := cr.refreshRoles(ctx, nodes); err != nil {
		cr.logger.Warn("Failed to refresh roles during scale down", "error", err)
		return reconcileAfterWaitInterval, nil
	}

	// Fast scaling is only safe when the cluster currently has no replicas.
	// Transitioning from replicasPerPrimary>0 to 0 must use normal scaling so that
	// existing replicas are cleanly removed without destroying the cluster data.
	if fastScalingEligible(config) && !hasReplicaNodes(nodes) {
		closeNodes(nodes)
		return cr.handleFastScaling(ctx, config)
	}

	// Heal the topology before evaluating node counts or draining: forget the lingering
	// entries of pods that were deleted (e.g. a chaos pod kill) or recreated mid-operation
	// and reintegrate their replacements. A dead pod's old entry lingers in the gossip
	// table flagged "fail" — often still owning slots and unreachable — so the drain
	// rebalance would try to connect to that dead address (timing out) and reference a
	// master redis-cli can no longer load ("No such master node"), deadlocking the
	// scale-down. Healing evicts those ghosts and re-covers orphaned slots so the drain
	// operates on a clean, reachable topology. meetMissing is true: scale-down adds no
	// pods, so every live pod is a legitimate member and reintegrating a recreated one is
	// safe.
	//
	// This must run BEFORE the currentTotal check below: when a slot-owning pod is killed
	// mid-scale-down it goes briefly unreachable, so initAllNodes reports fewer nodes than
	// targetTotal and the "nodes already removed" branch would otherwise verify a cluster
	// whose slots the dead pod orphaned — a check that can never pass, leaving the config
	// stuck InProgress (rk frozen in Configuring) forever.
	if changed, err := cr.healTopology(ctx, config, true); err != nil {
		cr.logger.Warn("Failed to heal topology during scale down", "error", err)
		return reconcileAfterWaitInterval, nil
	} else if changed {
		// Re-evaluate from a clean topology on the next pass.
		return reconcileImmediately, nil
	}

	if len(nodes) <= targetTotal {
		return cr.completeShrunkScaleDown(ctx, config, nodes, targetTotal)
	}

	plan := planScaleDown(nodes, targetPrimaries, targetPrimaries*int(config.Spec.ReplicasPerPrimary))

	// Build the keep and remove sets.
	keep := append(append([]*redis.Node{}, plan.keepPrimaries...), plan.keepReplicas...)
	remove := append(append([]*redis.Node{}, plan.removeReplicas...), plan.drainPrimaries...)

	cr.logger.Info("Scale-down topology classification",
		"currentPrimaries", len(plan.currentPrimaries),
		"currentReplicas", len(plan.currentReplicas),
		"keepPrimaries", len(plan.keepPrimaries),
		"keepReplicas", len(plan.keepReplicas),
		"drainPrimaries", len(plan.drainPrimaries),
		"removeReplicas", len(plan.removeReplicas))

	// Phase 1: drain the primaries that are being removed.
	if !cr.drainScaleDownPrimaries(ctx, config, plan) {
		return reconcileAfterWaitInterval, nil
	}

	// Phase 2: remove the surplus nodes — replicas first to avoid spurious failovers,
	// then the now-empty primaries — propagating CLUSTER FORGET across the survivors.
	cr.updateSubstatus(ctx, config, redisv1.SubstatusRemovingNodes)
	cr.logger.Info("Removing surplus nodes from cluster", "count", len(remove))
	if err := cr.removeNodes(ctx, keep, remove); err != nil {
		cr.logger.Error("Failed to remove nodes during scale down", "error", err)
		return reconcileAfterWaitInterval, nil
	}

	// Phase 3: shrink the StatefulSet now that the cluster no longer references the pods.
	cr.updateSubstatus(ctx, config, redisv1.SubstatusShrinkingStatefulSet)
	cr.logger.Info("Shrinking StatefulSet after node removal", "target", targetTotal)
	if err := kubernetes.ScaleStatefulSet(ctx, cr.client, cr.clusterName, cr.namespace, int32(targetTotal)); err != nil {
		return reconcileAfterInterval, fmt.Errorf("scaling StatefulSet down: %w", err)
	}

	// Re-attach replicas if needed (former primaries demoted to replicas).
	if config.Spec.ReplicasPerPrimary > 0 && len(plan.keepReplicas) > 0 {
		cr.updateSubstatus(ctx, config, redisv1.SubstatusAttachingReplicas)
		if err := cr.refreshRoles(ctx, keep); err != nil {
			cr.logger.Warn("Failed to refresh roles before re-attaching replicas", "error", err)
			return reconcileAfterWaitInterval, nil
		}
		if err := cr.setReplicas(ctx, plan.keepPrimaries, plan.keepReplicas, config.Spec.ReplicasPerPrimary); err != nil {
			cr.logger.Warn("Failed to re-attach replicas during scale down, will retry", "error", err)
			return reconcileAfterWaitInterval, nil
		}
	}

	if len(keep) == 0 {
		return reconcileAfterWaitInterval, nil
	}
	cr.updateSubstatus(ctx, config, redisv1.SubstatusVerifying)
	ok, err := cr.verifyCluster(ctx, keep[0])
	if err != nil || !ok {
		cr.logger.Info("Cluster not yet healthy after scale down, will retry")
		return reconcileAfterWaitInterval, nil
	}

	return cr.finishScaling(ctx, config, keep, "scale down")
}

// completeShrunkScaleDown handles a scale-down whose surplus nodes are already gone: it makes
// sure the StatefulSet matches the target size and verifies the cluster.
func (cr *ClusterReconciler) completeShrunkScaleDown(
	ctx context.Context, config *redisv1.RedkeyConfig, nodes []*redis.Node, targetTotal int,
) (reconcileSchedule, error) {
	cr.updateSubstatus(ctx, config, redisv1.SubstatusVerifying)
	if err := kubernetes.ScaleStatefulSet(ctx, cr.client, cr.clusterName, cr.namespace, int32(targetTotal)); err != nil {
		return reconcileAfterInterval, fmt.Errorf("scaling StatefulSet down: %w", err)
	}
	if len(nodes) == 0 {
		return reconcileAfterWaitInterval, nil
	}
	ok, err := cr.verifyCluster(ctx, nodes[0])
	if err != nil || !ok {
		cr.logger.Info("Cluster not yet healthy after scale down, will retry")
		return reconcileAfterWaitInterval, nil
	}
	return cr.finishScaling(ctx, config, nodes[:targetTotal], "scale down")
}

// drainScaleDownPrimaries moves every slot off the primaries being removed (weight 0) onto
// the surviving primaries (weight 1). It returns false when the scale-down must be retried.
func (cr *ClusterReconciler) drainScaleDownPrimaries(
	ctx context.Context, config *redisv1.RedkeyConfig, plan scaleDownPlan,
) bool {
	if len(plan.drainPrimaries) == 0 {
		return true
	}

	cr.updateSubstatus(ctx, config, redisv1.SubstatusDrainingPrimaries)
	cr.logger.Info("Draining primaries before removal", "drainCount", len(plan.drainPrimaries))
	weights := make(map[string]int)
	drainHasSlots := false
	for _, n := range plan.keepPrimaries {
		weights[n.ID] = 1
	}
	for _, n := range plan.drainPrimaries {
		weights[n.ID] = 0
		if strings.TrimSpace(n.Slots) != "" {
			drainHasSlots = true
		}
	}
	if !drainHasSlots {
		return true
	}

	if err := cr.rebalanceWithRetry(ctx, config, plan.keepPrimaries[0], weights, true); err != nil {
		cr.logger.Error("Rebalance failed during scale down", "error", err)
		return false
	}
	stable, err := cr.slotsStable(ctx, plan.keepPrimaries[0])
	if err != nil {
		cr.logger.Warn("Failed to check slot stability during scale down", "error", err)
		return false
	}
	if !stable {
		cr.logger.Info("Slots still migrating during scale down, will retry")
		return false
	}
	// Validate the drained primaries no longer hold slots.
	if err := cr.refreshRoles(ctx, plan.drainPrimaries); err != nil {
		cr.logger.Warn("Failed to refresh roles after drain", "error", err)
		return false
	}
	for _, n := range plan.drainPrimaries {
		if strings.TrimSpace(n.Slots) != "" {
			cr.logger.Info("Drained primary still holds slots, will retry", "node", n.Name)
			return false
		}
	}
	return true
}

// handleFastScaling recreates the cluster from scratch at the new topology. It is only
// used for eligible clusters (ephemeral, no replicas, purgeKeysOnRebalance) and accepts
// full data loss in exchange for a much faster operation.
func (cr *ClusterReconciler) handleFastScaling(ctx context.Context, config *redisv1.RedkeyConfig) (reconcileSchedule, error) {
	targetTotal := totalNodes(config)
	cr.logger.Info("Starting fast scaling reconciliation", "targetTotal", targetTotal)

	exists, err := kubernetes.StatefulSetExists(ctx, cr.client, cr.clusterName, cr.namespace)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("checking StatefulSet for fast scaling: %w", err)
	}

	if exists {
		current, err := kubernetes.GetStatefulSetReplicas(ctx, cr.client, cr.clusterName, cr.namespace)
		if err != nil {
			return reconcileAfterInterval, fmt.Errorf("reading StatefulSet replicas for fast scaling: %w", err)
		}
		// The StatefulSet still reflects the old topology: delete it so Kubernetes wipes
		// the pods, then recreate it on the next pass.
		if int(current) != targetTotal {
			cr.updateSubstatus(ctx, config, redisv1.SubstatusDeletingStatefulSet)
			cr.logger.Info("Fast scaling: deleting StatefulSet to recreate at new size", "from", current, "to", targetTotal)
			if err := kubernetes.DeleteStatefulSet(ctx, cr.client, cr.clusterName, cr.namespace); err != nil {
				return reconcileAfterInterval, fmt.Errorf("deleting StatefulSet for fast scaling: %w", err)
			}
			return reconcileAfterWaitInterval, nil
		}
	} else {
		// Recreate all cluster objects (StatefulSet is built with the new replica count).
		cr.updateSubstatus(ctx, config, redisv1.SubstatusRecreatingCluster)
		owner, err := cr.getOwner(ctx)
		if err != nil {
			return reconcileAfterInterval, fmt.Errorf("getting owner for fast scaling: %w", err)
		}
		password := cr.getPassword(ctx, config)
		if err := kubernetes.EnsureClusterObjects(ctx, cr.client, config, owner, password); err != nil {
			return reconcileAfterInterval, fmt.Errorf("recreating cluster objects for fast scaling: %w", err)
		}
		cr.logger.Info("Fast scaling: StatefulSet recreated, waiting for pods")
		return reconcileAfterWaitInterval, nil
	}

	// StatefulSet is at the new size: wait for pods, then form the cluster from scratch.
	cr.updateSubstatus(ctx, config, redisv1.SubstatusWaitingForPods)
	ready, err := kubernetes.AllPodsReady(ctx, cr.client, cr.clusterName, cr.namespace, int32(targetTotal))
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("checking pod readiness during fast scaling: %w", err)
	}
	if !ready {
		cr.logger.Info("Fast scaling: waiting for recreated pods to be ready", "expected", targetTotal)
		return reconcileAfterWaitInterval, nil
	}

	password := cr.getPassword(ctx, config)
	nodes, err := cr.initNodes(ctx, config, password)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("initializing nodes for fast scaling: %w", err)
	}
	defer closeNodes(nodes)
	if len(nodes) < targetTotal {
		cr.logger.Info("Fast scaling: not all nodes available, will retry", "available", len(nodes), "expected", targetTotal)
		return reconcileAfterWaitInterval, nil
	}

	cr.updateSubstatus(ctx, config, redisv1.SubstatusFormingCluster)
	cr.logger.Info("Fast scaling: forming new cluster from scratch", "nodes", len(nodes))
	if !cr.formCluster(ctx, config, nodes) {
		return reconcileAfterWaitInterval, nil
	}

	return cr.finishScaling(ctx, config, nodes, "fast scaling")
}

// --- Scaling helpers ---

// finishScaling transitions the cluster to Ready, marks the config as applied, and
// records the final node topology in the status.
func (cr *ClusterReconciler) finishScaling(ctx context.Context, config *redisv1.RedkeyConfig, nodes []*redis.Node, op string) (reconcileSchedule, error) {
	// Before declaring the operation complete, ensure the cluster actually matches the
	// desired primary/replica distribution. A pod recreated during scaling can rejoin with
	// the wrong role (e.g. a replica comes back as an empty primary), leaving the wrong
	// number of primaries or a primary with too many/few replicas — which the slot/state
	// checks alone do not catch. Remediate and requeue until the topology is correct so we
	// never mark Ready with a distribution that does not match the spec.
	topologyOK, err := cr.ensureReplicaTopology(ctx, config)
	if err != nil {
		cr.logger.Warn("Replica topology check failed while finishing scaling, will retry",
			"operation", op, "error", err)
		return reconcileAfterWaitInterval, nil
	}
	if !topologyOK {
		cr.logger.Info("Replica topology not yet correct, remediating before completing scaling",
			"operation", op)
		return reconcileAfterWaitInterval, nil
	}

	config.Status.Substatus.Status = ""
	if err := cr.updateClusterStatus(ctx, config, redisv1.ClusterStatusReady); err != nil {
		return reconcileAfterInterval, err
	}
	if err := cr.setConfigPhaseApplied(ctx, config); err != nil {
		return reconcileAfterInterval, err
	}

	// Populate the data-plane health conditions now so reaching Ready reflects health immediately,
	// rather than leaving them Unknown until the next reconcile cycle runs handleReady.
	cr.refreshHealthConditions(ctx, config, cr.getPassword(ctx, config))

	if err := cr.updateNodeStatus(ctx, config, nodes); err != nil {
		cr.logger.Error("Failed to update node status after scaling", "error", err)
		// Non-critical, continue.
	}
	cr.logger.Info("Scaling complete, status set to Ready", "operation", op)
	return reconcileAfterInterval, nil
}

// hasReplicaNodes reports whether any node in the set has the replica role.
func hasReplicaNodes(nodes []*redis.Node) bool {
	for _, n := range nodes {
		if n.IsReplica() {
			return true
		}
	}
	return false
}

// initAllNodes initializes every pod that currently belongs to the cluster (regardless
// of the target topology) ordered by StatefulSet ordinal. Scale-down needs to talk to
// the nodes that are about to be removed, which are not part of the target topology.
func (cr *ClusterReconciler) initAllNodes(ctx context.Context, password string) ([]*redis.Node, error) {
	clusterCfg := cr.runtimeConfig.ClusterConfig()
	maxRetries := clusterCfg.ConnectionMaxRetries
	backoff := time.Duration(clusterCfg.ConnectionBackOffSeconds) * time.Second

	podAddrs, err := kubernetes.GetPodAddresses(ctx, cr.client, cr.clusterName, cr.namespace)
	if err != nil {
		return nil, fmt.Errorf("getting pod addresses: %w", err)
	}

	names := make([]string, 0, len(podAddrs))
	for name := range podAddrs {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		return cr.ordinalOf(names[i]) < cr.ordinalOf(names[j])
	})

	var nodes []*redis.Node
	for _, name := range names {
		node := redis.NewNode(name, podAddrs[name], password)
		if err := node.Init(ctx, maxRetries, backoff); err != nil {
			cr.logger.Error("Failed to initialize node", "node", name, "error", err)
			closeNodes(nodes)
			return nil, err
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

// filterUnmet returns the subset of nodes whose IDs are NOT in the given memberIDs set.
func filterUnmet(nodes []*redis.Node, memberIDs map[string]struct{}) []*redis.Node {
	var unmet []*redis.Node
	for _, n := range nodes {
		if _, ok := memberIDs[n.ID]; !ok {
			unmet = append(unmet, n)
		}
	}
	return unmet
}

// ordinalOf extracts the StatefulSet ordinal from a pod name (e.g. "mycluster-3" -> 3).
// Unrecognized names sort last.
func (cr *ClusterReconciler) ordinalOf(name string) int {
	idx := strings.LastIndex(name, "-")
	if idx < 0 || idx == len(name)-1 {
		return 1 << 30
	}
	ordinal, err := strconv.Atoi(name[idx+1:])
	if err != nil {
		return 1 << 30
	}
	return ordinal
}

// refreshRoles refreshes the cached role/slot metadata for every node.
func (cr *ClusterReconciler) refreshRoles(ctx context.Context, nodes []*redis.Node) error {
	for _, n := range nodes {
		if err := n.RefreshInfo(ctx); err != nil {
			return fmt.Errorf("refreshing %s: %w", n.Name, err)
		}
	}
	return nil
}

// rebalanceWithRetry runs a weighted rebalance from the given seed node, retrying with
// backoff until it succeeds or the attempts are exhausted. A nil/empty weights map
// performs an empty-masters rebalance that distributes slots evenly across all primaries.
//
// The retry decision is driven by the cluster's live topology and the context state, not
// by parsing redis-cli error text (which is brittle and could change between Redis
// versions):
//   - Before every attempt it inspects CLUSTER NODES for open (importing/migrating)
//     slots left behind by a previous, possibly interrupted, rebalance and repairs them
//     with redis-cli --cluster fix. redis-cli refuses to rebalance a cluster with open
//     slots, so this repair is what lets the operation make progress.
//   - A failure is retried as long as the parent context is still alive. The per-attempt
//     rebalance timeout cancels only an inner context, so a slow reshard is naturally
//     retryable; cancellation of the parent context (e.g. shutdown) aborts immediately.
func (cr *ClusterReconciler) rebalanceWithRetry(ctx context.Context, config *redisv1.RedkeyConfig, seed *redis.Node, weights map[string]int, meetMissing bool) error {
	backoff := cr.runtimeConfig.ClusterMeetWait()
	if backoff <= 0 {
		backoff = time.Second
	}
	password := cr.getPassword(ctx, config)

	var lastErr error
	for attempt := 1; attempt <= defaultRebalanceMaxAttempts; attempt++ {
		// Heal membership before touching redis-cli: a pod deleted since the last attempt leaves a
		// dead ghost in the gossip table that redis-cli --cluster fix/rebalance keeps hitting,
		// stalling ~2 minutes on its connect timeout. If healing changed the topology, the weights
		// map now references stale node IDs, so bail and let the caller re-classify nodes and rebuild
		// the weights from the healed topology.
		if changed, healErr := cr.healTopology(ctx, config, meetMissing); healErr != nil {
			cr.logger.Warn("Failed to heal topology before rebalance, continuing", "error", healErr)
		} else if changed {
			if lastErr != nil {
				return fmt.Errorf("topology changed while rebalancing from %s, re-evaluating from a clean state: %w", seed.Name, lastErr)
			}
			return fmt.Errorf("topology changed before rebalancing from %s, re-evaluating from a clean state", seed.Name)
		}

		// A pod deleted mid-operation can linger in the gossip table with an unreachable IP that
		// healTopology cannot yet forget (Kubernetes still reports a pod at that IP, or gossip has not
		// marked it fail). redis-cli --cluster fix/rebalance begins by connecting to every node, so it
		// would hang ~2 minutes on that dead node's TCP connect timeout. Probe reachability first and
		// bail if any master is unreachable, so the caller retries against a clean topology instead of
		// blocking.
		if !cr.mastersReachable(ctx, seed, password) {
			if lastErr != nil {
				return fmt.Errorf("rebalance from %s deferred, a cluster master is unreachable: %w", seed.Name, lastErr)
			}
			return fmt.Errorf("rebalance from %s deferred, a cluster master is unreachable", seed.Name)
		}

		// Repair any leftover open slots (detected structurally from CLUSTER NODES), then rebalance.
		if err := cr.recoverOpenSlots(ctx, seed, password); err != nil {
			cr.logger.Warn("Failed to recover open slots before rebalance, continuing", "error", err)
		}

		err := cr.runRebalanceOnce(ctx, seed, weights)
		if err == nil {
			return nil
		}
		lastErr = err

		// Abort immediately only if the parent context was cancelled (e.g. shutdown). A
		// per-attempt rebalance timeout cancels only the inner context, so it falls
		// through to the retry path where the cluster is repaired and the rebalance re-run.
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt == defaultRebalanceMaxAttempts {
			break
		}

		cr.logger.Info("Rebalance attempt failed, will repair open slots and retry", "attempt", attempt, "error", err)

		// redis-cli --cluster rebalance rejects a cluster for several reasons beyond open slots on the
		// seed's own view — notably a leftover half-migration open on another node (whose importing/
		// migrating marker is local and invisible to the seed) and "Nodes don't agree about
		// configuration" epoch disagreements. redis-cli --cluster fix repairs all of them, and it
		// connects to every node so it sees state the seed's view hides. recoverOpenSlots' structural
		// detection cannot cover every refusal reason, so force a fix here after any failure to guarantee
		// forward progress and never loop indefinitely refusing and retrying. The fix is a no-op when the
		// cluster is already clean.
		if fixErr := cr.runClusterFix(ctx, seed, password); fixErr != nil {
			cr.logger.Warn("Post-failure cluster fix did not complete, will retry", "attempt", attempt, "error", fixErr)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}
	return fmt.Errorf("rebalance from %s did not converge after %d attempt(s): %w", seed.Name, defaultRebalanceMaxAttempts, lastErr)
}

// runRebalanceOnce performs a single weighted rebalance bounded by the configured
// rebalance timeout.
func (cr *ClusterReconciler) runRebalanceOnce(ctx context.Context, seed *redis.Node, weights map[string]int) error {
	rebalanceCtx := ctx
	if timeout := cr.runtimeConfig.RebalanceTimeout(); timeout > 0 {
		var cancel context.CancelFunc
		rebalanceCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	_, err := seed.Client().ClusterRebalanceWithWeights(rebalanceCtx, weights)
	return err
}

// recoverOpenSlots detects slots left in a migrating/importing state by a previously
// interrupted rebalance, or a coverage gap left behind by a primary that vanished (for
// example an ephemeral pod deleted mid-operation that rejoined empty with a new ID), and
// runs redis-cli --cluster fix to repair them. redis-cli refuses to rebalance a cluster
// with open OR uncovered slots, so this repair is what lets the operation make progress.
// It is a no-op when the cluster has no in-flight slots and full slot coverage.
//
// Open-slot (importing/migrating) markers are LOCAL to the node that holds them: they appear only in
// that node's own CLUSTER NODES line and are not propagated by gossip. The seed's view therefore
// cannot see a slot left open on another node, so detection probes every master's own view — without
// it, a leftover half-migration on a non-seed node stays invisible, the fix never runs, and redis-cli
// rebalance (which checks all nodes) refuses forever. Slot coverage, by contrast, is gossiped and so
// is read from the seed's view directly.
func (cr *ClusterReconciler) recoverOpenSlots(ctx context.Context, seed *redis.Node, password string) error {
	clusterNodes, err := seed.Client().GetClusterNodes(ctx)
	if err != nil {
		return err
	}
	covered := redis.SlotsFullyCovered(clusterNodes)
	inFlight := redis.HasInFlightSlots(clusterNodes) || cr.anyMasterHasOpenSlots(ctx, clusterNodes, password)
	if !inFlight && covered {
		return nil
	}

	cr.logger.Info("Slot problems detected before rebalance, running cluster fix",
		"node", seed.Name, "inFlightSlots", inFlight, "fullyCovered", covered)
	return cr.runClusterFix(ctx, seed, password)
}

// anyMasterHasOpenSlots reports whether any master currently holds a slot in a migrating or importing
// state, by inspecting each master's OWN CLUSTER NODES view. These in-flight markers are local to the
// node that owns them and are not propagated by gossip, so the seed's view alone cannot detect an open
// slot left behind on another node — the blind spot that lets a leftover half-migration stall the
// rebalance indefinitely. Masters are probed concurrently with a short per-node timeout; a probe error
// is treated as "no marker" because reachability is already guarded upstream by mastersReachable.
func (cr *ClusterReconciler) anyMasterHasOpenSlots(
	ctx context.Context, clusterNodes []redis.ClusterNode, password string,
) bool {
	var open bool
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, cn := range clusterNodes {
		if cn.IP == "" || !strings.Contains(cn.Flags, "master") {
			continue
		}
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			probeCtx, cancel := context.WithTimeout(ctx, rebalanceProbeTimeout)
			defer cancel()
			c := redis.NewClient(addr, password)
			defer func() { _ = c.Close() }()
			nodes, perr := c.GetClusterNodes(probeCtx)
			if perr != nil {
				return
			}
			if redis.HasInFlightSlots(nodes) {
				mu.Lock()
				open = true
				mu.Unlock()
			}
		}(fmt.Sprintf("%s:%d", cn.IP, redis.DefaultPort))
	}
	wg.Wait()
	return open
}

// clusterFixWatchdogInterval is how often the cluster-fix watchdog re-probes master reachability
// while a fix is in flight. It must stay well below clusterFixTimeout so a mid-fix pod death is
// detected and the fix aborted within a couple of probe cycles.
const clusterFixWatchdogInterval = 3 * time.Second

// runClusterFix runs redis-cli --cluster fix from the seed node, bounded by clusterFixTimeout (or
// the configured rebalance timeout when it is shorter). The fix both closes open (migrating/
// importing) slots and reassigns slots left uncovered, which is needed after forgetting a
// slot-owning stale node. Bounding it well below RebalanceTimeout means a fix that hits a pod
// deleted mid-operation fails fast on the dead node instead of stalling ~2 minutes on redis-cli's
// own connect timeout, letting the retry loop heal membership and re-evaluate.
//
// The static timeout alone is a blunt instrument: large clusters (40-50 nodes) need a generous cap
// for a legitimately slow but healthy fix, yet that same generous cap makes a mid-fix pod death stall
// for the whole duration. A reachability watchdog resolves the tension: it re-probes the masters
// while the fix runs and cancels it the instant one becomes unreachable, so a mid-fix death aborts
// within a couple of probe cycles regardless of how high the timeout is set.
func (cr *ClusterReconciler) runClusterFix(ctx context.Context, seed *redis.Node, password string) error {
	timeout := clusterFixTimeout
	if rebalance := cr.runtimeConfig.RebalanceTimeout(); rebalance > 0 && rebalance < timeout {
		timeout = rebalance
	}
	fixCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Watchdog: mastersReachable already ran before this fix, but a pod can be deleted while the fix is
	// in flight, and redis-cli would then block on that dead node's TCP connect for the full timeout.
	// Poll reachability during the fix and cancel it the moment a master drops so the retry loop heals
	// membership and re-evaluates, without shortening the timeout that healthy large clusters rely on.
	watchdogDone := make(chan struct{})
	defer close(watchdogDone)
	go func() {
		ticker := time.NewTicker(clusterFixWatchdogInterval)
		defer ticker.Stop()
		for {
			select {
			case <-watchdogDone:
				return
			case <-fixCtx.Done():
				return
			case <-ticker.C:
				if !cr.mastersReachable(fixCtx, seed, password) {
					cr.logger.Info("Master became unreachable during cluster fix, aborting to heal and retry",
						"node", seed.Name)
					cancel()
					return
				}
			}
		}
	}()

	if _, err := seed.Client().ClusterFix(fixCtx); err != nil {
		return fmt.Errorf("cluster fix: %w", err)
	}
	return nil
}

// rebalanceProbeTimeout bounds the per-node reachability probe run before a rebalance/fix.
const rebalanceProbeTimeout = 3 * time.Second

// mastersReachable probes every master in the seed's view concurrently with a short per-node timeout
// and reports whether they all respond. redis-cli --cluster fix/rebalance starts by connecting to
// every node, so a single unreachable master (a pod deleted mid-operation whose entry still lingers
// in gossip) hangs the whole command for ~2 minutes on its TCP connect timeout. Probing first lets
// the caller skip a doomed rebalance and retry once the topology is clean and reachable.
func (cr *ClusterReconciler) mastersReachable(ctx context.Context, seed *redis.Node, password string) bool {
	clusterNodes, err := seed.Client().GetClusterNodes(ctx)
	if err != nil {
		return false
	}
	reachable := true
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, cn := range clusterNodes {
		if cn.IP == "" || !strings.Contains(cn.Flags, "master") {
			continue
		}
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			probeCtx, cancel := context.WithTimeout(ctx, rebalanceProbeTimeout)
			defer cancel()
			c := redis.NewClient(addr, password)
			defer func() { _ = c.Close() }()
			if _, perr := c.GetClusterInfo(probeCtx); perr != nil {
				mu.Lock()
				reachable = false
				mu.Unlock()
			}
		}(fmt.Sprintf("%s:%d", cn.IP, redis.DefaultPort))
	}
	wg.Wait()
	return reachable
}

// slotsStable reports whether no node currently has slots in a migrating or importing state.
func (cr *ClusterReconciler) slotsStable(ctx context.Context, seed *redis.Node) (bool, error) {
	clusterNodes, err := seed.Client().GetClusterNodes(ctx)
	if err != nil {
		return false, err
	}
	return !redis.HasInFlightSlots(clusterNodes), nil
}

// removeNodes removes the given nodes from the cluster. Replicas are removed before
// primaries to avoid spurious failovers, each removal is propagated to every surviving
// node via CLUSTER FORGET, and the removed node is shut down cleanly.
func (cr *ClusterReconciler) removeNodes(ctx context.Context, keep, remove []*redis.Node) error {
	ordered := make([]*redis.Node, 0, len(remove))
	for _, n := range remove {
		if n.IsReplica() {
			ordered = append(ordered, n)
		}
	}
	for _, n := range remove {
		if !n.IsReplica() {
			ordered = append(ordered, n)
		}
	}

	for _, n := range ordered {
		for _, k := range keep {
			if k.ID == n.ID {
				continue
			}
			if err := k.Client().ClusterForget(ctx, n.ID); err != nil {
				// A node may already not know about the forgotten node; log and continue.
				cr.logger.Warn("Failed to forget node, continuing", "from", k.Name, "node", n.Name, "error", err)
			}
		}
		if err := n.Client().ShutdownSave(ctx); err != nil {
			cr.logger.Warn("Failed to shut down node cleanly, continuing", "node", n.Name, "error", err)
		}
		cr.logger.Info("Removed node from cluster", "node", n.Name)
	}

	// Allow gossip to propagate the removals before continuing.
	if wait := cr.runtimeConfig.ClusterMeetWait(); wait > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
	return nil
}
