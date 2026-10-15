// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/inditextech/redkey-robin/internal/config"
	"github.com/inditextech/redkey-robin/internal/health"
	"github.com/inditextech/redkey-robin/internal/redis"
)

// RedisClientFactory creates Redis clients for a given address and password.
type RedisClientFactory func(addr, password string) *redis.Client

// HealthReconciler performs health checks and remediation on a Ready cluster.
type HealthReconciler struct {
	checker       *health.Checker
	clientFactory RedisClientFactory
	runtimeConfig *config.RuntimeConfig
	logger        *slog.Logger

	// ephemeral records whether the cluster uses ephemeral (emptyDir) storage. It governs
	// how membership remediation treats a stale node that still owns slots: on ephemeral
	// clusters the recreated pod lost its data and rejoins with a new ID, so the phantom is
	// forgotten and its slots reassigned; on persistent clusters the node keeps its identity
	// and reclaims its slots on rejoin, so it is left untouched to avoid stranding data. It
	// is set per-reconcile via SetEphemeral before Reconcile runs; the zero value (false =
	// persistent) is the conservative default that never reassigns slots out from under a
	// node that could still hold their data.
	ephemeral bool

	// lastReport caches the health report evaluated by the most recent Reconcile call so the
	// caller can surface health conditions/substatus without re-running the check. The reconcile
	// loop drives a single HealthReconciler sequentially, so it needs no synchronization.
	lastReport *health.Report
}

// NewHealthReconciler creates a new HealthReconciler.
func NewHealthReconciler(
	checker *health.Checker,
	clientFactory RedisClientFactory,
	runtimeConfig *config.RuntimeConfig,
	logger *slog.Logger,
) *HealthReconciler {
	if logger == nil {
		logger = slog.Default()
	}
	return &HealthReconciler{
		checker:       checker,
		clientFactory: clientFactory,
		runtimeConfig: runtimeConfig,
		logger:        logger.With("component", "health-reconciler"),
	}
}

// Close releases resources held by the health reconciler, closing all cached
// health-check Redis clients. It should be called on shutdown.
func (hr *HealthReconciler) Close() {
	if hr.checker != nil {
		hr.checker.CloseAll()
	}
}

// SetEphemeral records whether the cluster uses ephemeral storage, controlling how
// membership remediation reclaims a stale slot-owning node (see the ephemeral field and
// HealMembership). It must be called before Reconcile so remediation uses the correct
// storage semantics.
func (hr *HealthReconciler) SetEphemeral(ephemeral bool) {
	hr.ephemeral = ephemeral
}

// Reconcile performs health checks and, if needed, remediation on the cluster.
// Returns the recommended schedule for the next reconciliation. Membership remediation
// uses the ephemeral setting recorded via SetEphemeral (see HealMembership).
//
// The health report it evaluated is stored and exposed via LastReport so the caller can surface the
// health state (conditions/substatus) without re-running the check. The reconcile loop drives a
// single HealthReconciler sequentially, so this cached report is safe to read right after the call.
func (hr *HealthReconciler) Reconcile(ctx context.Context, nodes []health.Node, password string, desiredPrimaries, desiredReplicasPerPrimary int) (reconcileSchedule, error) {
	hr.lastReport = nil

	if len(nodes) == 0 {
		return reconcileAfterInterval, fmt.Errorf("no nodes provided for health reconciliation")
	}

	hr.logger.Info("Starting cluster health check", "nodes", len(nodes))

	report, err := hr.checker.Check(ctx, nodes, password, desiredPrimaries, desiredReplicasPerPrimary)
	if err != nil {
		hr.logger.Warn("Health check completed with errors", "error", err)
	}
	if report == nil {
		return reconcileAfterInterval, fmt.Errorf("health check returned nil report: %w", err)
	}
	hr.lastReport = report

	hr.logReport(report)

	if report.Healthy {
		hr.logger.Info("Cluster is healthy")
		return reconcileAfterInterval, nil
	}

	// Sequential remediation pipeline
	hr.logger.Info("Cluster health issues detected, starting remediation")

	if !report.MembershipOK {
		if remErr := hr.remediateMembership(ctx, report, nodes, password); remErr != nil {
			hr.logger.Error("Membership remediation failed", "error", remErr)
			return reconcileAfterWaitInterval, remErr
		}
		// Membership changed — the cluster topology is in flux. Further remediation
		// (slots, balance) must use fresh cluster state, not the stale report captured
		// before the MEET. Defer to the next reconciliation cycle.
		hr.logger.Info("Membership remediated, deferring further checks to next cycle")
		return reconcileAfterWaitInterval, nil
	}

	if !report.SlotsCoveredOK {
		if remErr := hr.remediateSlotCoverage(ctx, report, nodes, password); remErr != nil {
			hr.logger.Error("Slot coverage remediation failed", "error", remErr)
			return reconcileAfterWaitInterval, remErr
		}
	}

	if !report.ReplicaSpreadOK {
		topologyChanged, remErr := hr.remediateReplicaSpread(ctx, report, nodes, password, desiredPrimaries, desiredReplicasPerPrimary)
		if remErr != nil {
			hr.logger.Error("Replica spread remediation failed", "error", remErr)
			return reconcileAfterWaitInterval, remErr
		}
		if topologyChanged {
			// Promoting or demoting nodes reshapes the cluster. The remaining checks
			// (cluster check, balance) must run against fresh state, not the report
			// captured before the change. Defer them to the next reconciliation cycle.
			hr.logger.Info("Replica topology changed, deferring further checks to next cycle")
			return reconcileAfterWaitInterval, nil
		}
	}

	if !report.ClusterCheckOK {
		if remErr := hr.remediateClusterCheck(ctx, nodes, password); remErr != nil {
			hr.logger.Error("Cluster check remediation failed", "error", remErr)
			return reconcileAfterWaitInterval, remErr
		}
	}

	if !report.BalancedOK {
		if remErr := hr.remediateBalance(ctx, nodes, password); remErr != nil {
			hr.logger.Error("Balance remediation failed", "error", remErr)
			return reconcileAfterWaitInterval, remErr
		}
	}

	hr.logger.Info("Remediation complete, will re-check soon")
	return reconcileAfterWaitInterval, nil
}

// LastReport returns the health report evaluated by the most recent Reconcile call, or nil if the
// last call could not produce one (e.g. no nodes). Intended to be read immediately after Reconcile.
func (hr *HealthReconciler) LastReport() *health.Report {
	return hr.lastReport
}

// Check runs the health check WITHOUT remediation and returns the report (also cached for
// LastReport). It lets operation-completion paths populate the health conditions the moment a
// cluster becomes Ready, instead of leaving them Unknown until the next periodic Reconcile.
func (hr *HealthReconciler) Check(ctx context.Context, nodes []health.Node, password string, desiredPrimaries, desiredReplicasPerPrimary int) *health.Report {
	hr.lastReport = nil
	if len(nodes) == 0 {
		return nil
	}
	report, err := hr.checker.Check(ctx, nodes, password, desiredPrimaries, desiredReplicasPerPrimary)
	if err != nil {
		hr.logger.Warn("Post-operation health check completed with errors", "error", err)
	}
	hr.lastReport = report
	return report
}

// logReport logs the health report details.
func (hr *HealthReconciler) logReport(report *health.Report) {
	hr.logger.Info("Health check report",
		"membershipOK", report.MembershipOK,
		"slotsCoveredOK", report.SlotsCoveredOK,
		"replicaSpreadOK", report.ReplicaSpreadOK,
		"clusterCheckOK", report.ClusterCheckOK,
		"balancedOK", report.BalancedOK,
		"healthy", report.Healthy,
	)
	if len(report.ClusterCheckErrors) > 0 {
		hr.logger.Warn("Cluster check errors", "errors", report.ClusterCheckErrors)
	}
	if len(report.ClusterCheckWarnings) > 0 {
		hr.logger.Warn("Cluster check warnings", "warnings", report.ClusterCheckWarnings)
	}
}

// --- Remediation: Membership ---

// remediateMembership fixes cluster membership by:
// - FOGETting nodes visible in the cluster that don't correspond to real K8s pods
// - MEETing nodes that should be in the cluster but aren't visible
//
// It delegates to HealMembership, the shared membership healer reused by the operation
// reconcilers (scaling and upgrade) so that pod recreation is handled identically in the
// steady state and mid-operation. The slot-owner protection uses the ephemeral setting
// recorded via SetEphemeral.
func (hr *HealthReconciler) remediateMembership(ctx context.Context, _ *health.Report, nodes []health.Node, password string) error {
	hr.logger.Info("Remediating cluster membership")
	_, err := hr.HealMembership(ctx, nodes, password, true, hr.ephemeral)
	return err
}

// membershipPeerOpTimeout bounds each individual CLUSTER FORGET/MEET issued to a peer during
// membership healing. Under pod churn some peers are transiently unreachable; without a per-op
// bound each call would block on the client dial timeout and its retries (~15-25s), and the serial
// fan-out over every peer would stall the whole reconcile loop for minutes. Bounding each op and
// running the fan-out concurrently keeps membership healing to roughly one timeout regardless of how
// many peers are unreachable, so the reconcile loop keeps making progress and re-evaluates against a
// fresh topology on the next cycle.
const membershipPeerOpTimeout = 3 * time.Second

// forgetFromAllPeers issues CLUSTER FORGET <targetID> to every peer concurrently, each bounded by
// membershipPeerOpTimeout so an unreachable peer fails fast instead of stalling the reconcile loop.
func (hr *HealthReconciler) forgetFromAllPeers(ctx context.Context, nodes []health.Node, password, targetID string) {
	var wg sync.WaitGroup
	for _, node := range nodes {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			opCtx, cancel := context.WithTimeout(ctx, membershipPeerOpTimeout)
			defer cancel()
			client := hr.clientFactory(addr, password)
			defer func() { _ = client.Close() }()
			if forgetErr := client.ClusterForget(opCtx, targetID); forgetErr != nil {
				hr.logger.Warn("Failed to forget node from peer", "peer", addr, "targetID", targetID, "error", forgetErr)
			}
		}(node.Addr)
	}
	wg.Wait()
}

// meetFromAllPeers issues CLUSTER MEET <targetIP> to every in-cluster peer concurrently, each bounded
// by membershipPeerOpTimeout. Peers not yet in the cluster view are skipped.
func (hr *HealthReconciler) meetFromAllPeers(ctx context.Context, nodes []health.Node, clusterIPs map[string]struct{}, password, targetIP string) {
	var wg sync.WaitGroup
	for _, peer := range nodes {
		peerIP := extractIP(peer.Addr)
		if _, peerInCluster := clusterIPs[peerIP]; !peerInCluster {
			continue // skip nodes not yet in the cluster
		}
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			opCtx, cancel := context.WithTimeout(ctx, membershipPeerOpTimeout)
			defer cancel()
			client := hr.clientFactory(addr, password)
			defer func() { _ = client.Close() }()
			if meetErr := client.ClusterMeet(opCtx, targetIP, redis.DefaultPort); meetErr != nil {
				hr.logger.Warn("Failed to meet from peer", "peer", addr, "target", targetIP, "error", meetErr)
			}
		}(peer.Addr)
	}
	wg.Wait()
}

// promoteReplicaOfDeadMaster force-promotes the first reachable replica of the given (dead) master to
// a primary via CLUSTER FAILOVER TAKEOVER, so the master's slots stay served by the replica's data
// copy and the dead master — no longer anyone's master — can then be forgotten without a replica
// refusing with "Can't forget my master". It is the failover Robin performs when Redis' own
// auto-failover is stuck (e.g. the replica exceeded cluster-replica-validity-factor after a long
// disconnect). Only one replica is promoted; any others re-replicate the new primary via gossip.
func (hr *HealthReconciler) promoteReplicaOfDeadMaster(ctx context.Context, clusterNodes []redis.ClusterNode, expectedIPs map[string]struct{}, password, masterID string) bool {
	for _, rn := range clusterNodes {
		if rn.Primary != masterID || rn.IP == "" {
			continue
		}
		if !strings.Contains(rn.Flags, "slave") || isConfirmedFail(rn.Flags) {
			continue
		}
		if _, live := expectedIPs[rn.IP]; !live {
			continue // only promote a replica backed by a live pod
		}
		addr := fmt.Sprintf("%s:%d", rn.IP, redis.DefaultPort)
		client := hr.clientFactory(addr, password)
		err := client.ClusterFailoverTakeover(ctx)
		_ = client.Close()
		if err != nil {
			hr.logger.Warn("Failed to promote replica of dead master via takeover",
				"replicaID", rn.ID, "replicaIP", rn.IP, "masterID", masterID, "error", err)
			continue
		}
		hr.logger.Info("Promoted replica of dead master to primary via failover takeover",
			"replicaID", rn.ID, "replicaIP", rn.IP, "masterID", masterID)
		return true
	}
	return false
}

// isForgettableNoaddrGhost reports whether a cluster node is a Redis `noaddr` phantom that can be
// forgotten safely: it has no address (":0@0"), carries the `noaddr` flag, and owns no slots. Such
// an entry is left behind when a pod is deleted and recreated (e.g. during a rolling upgrade) and
// the cluster loses its address; it is unreachable, holds no data, and otherwise keeps membership
// unhealthy until it is removed. A node still mid-handshake has a real address and is therefore not
// matched, and a slot-owning entry is excluded so its data is never stranded by a premature forget.
func isForgettableNoaddrGhost(cn redis.ClusterNode) bool {
	return cn.IP == "" &&
		strings.Contains(cn.Flags, "noaddr") &&
		!redis.OwnsSlots(cn.Slots)
}

// HealMembership reconciles cluster membership against the set of live pods provided in
// nodes, which is the source of truth. When Kubernetes recreates a pod (e.g. it is moved
// to another node during scaling or upgrade), the pod comes back with a new IP — and, for
// ephemeral clusters, a new node ID — while its former entry lingers in the gossip table,
// often still owning slots. HealMembership:
//   - FORGETs every cluster node whose IP is not among the live pod IPs (the lingering
//     trace of a deleted or recreated pod), issuing the FORGET from every live node;
//   - when meetMissing is true, MEETs every live pod not yet in the cluster view (a
//     recreated member that must be reintegrated), issuing MEET from all in-cluster peers
//     so a post-FORGET blacklist on any single node cannot silently drop it;
//   - runs redis-cli --cluster fix once if a slot-owning node was forgotten, to re-cover
//     the slots orphaned by the removal.
//
// The ephemeral flag governs how a stale node that STILL OWNS SLOTS is handled — the one
// case where forgetting is destructive:
//   - ephemeral=true: the recreated pod loses its data (emptyDir) and rejoins with a brand
//     new node ID, so the old slot-owning entry is a true phantom that will never reclaim
//     its slots. It is forgotten and its slots are re-covered with cluster fix.
//   - ephemeral=false (persistent): the recreated pod keeps its PVC, hence its nodes.conf
//     and node ID, and will reclaim those slots on rejoin (or a replica has already failed
//     over). Forgetting and reassigning now would strand its data, so slot-owning stale
//     entries are left untouched and simply waited out. Only slotless stale entries (which
//     hold no data) are forgotten. This mirrors forgetFailedNodes' slot-owner protection.
//
// It returns changed=true if any node was forgotten or met, so callers can requeue and
// re-evaluate from a clean topology; when nothing changed it is a fast no-op.
//
// meetMissing MUST be false for the scale-up flow: that flow rebalances slots across all
// empty masters, so meeting a not-yet-classified empty node here could hand slots to a pod
// destined to become a replica. Scale-up meets its own new nodes with explicit roles.
func (hr *HealthReconciler) HealMembership(ctx context.Context, nodes []health.Node, password string, meetMissing, ephemeral bool) (bool, error) {
	if len(nodes) == 0 {
		return false, fmt.Errorf("no nodes provided for membership healing")
	}
	// Build the set of expected IPs from the live pod list.
	expectedIPs := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		expectedIPs[extractIP(node.Addr)] = struct{}{}
	}

	// Use the first reachable node as the command executor.
	seedAddr := nodes[0].Addr
	seedClient := hr.clientFactory(seedAddr, password)
	defer func() { _ = seedClient.Close() }()

	clusterNodes, err := seedClient.GetClusterNodes(ctx)
	if err != nil {
		return false, fmt.Errorf("getting cluster nodes from seed %s: %w", seedAddr, err)
	}

	// FORGET nodes whose IP does not correspond to a live pod.
	forgot := false
	forgotSlotOwner := false
	for _, cn := range clusterNodes {
		if cn.IP == "" {
			// A node with no address (":0@0") carries the Redis `noaddr` flag: the cluster kept an
			// entry for a node whose address it lost — typically a pod deleted and recreated during a
			// rolling upgrade, whose new incarnation rejoined under a fresh ID. It is unreachable and
			// cannot be matched to a live pod, so membership stays unhealthy until it is removed, yet
			// it does not always age out of gossip within a reconcile window (observed as a lingering
			// 7th "known node" that fails the post-upgrade node-count check). Forget the phantom so the
			// cluster converges. A node still mid-handshake has a real address (so it is untouched
			// here), and a slot-owning entry is left in place (its data is reclaimed on rejoin / served
			// by a replica) to avoid stranding data — mirroring the slot-owner protection below.
			if isForgettableNoaddrGhost(cn) {
				hr.logger.Info("Forgetting noaddr phantom node from cluster",
					"nodeID", cn.ID, "addr", cn.Addr, "flags", cn.Flags)
				hr.forgetFromAllPeers(ctx, nodes, password, cn.ID)
				forgot = true
				continue
			}
			hr.logger.Warn("Skipping node with empty IP in membership healing", "nodeID", cn.ID, "addr", cn.Addr)
			continue
		}
		_, expected := expectedIPs[cn.IP]
		// A master in confirmed fail is a dead pod. On ephemeral clusters the recreated pod
		// rejoins with a brand-new node ID, so a fail entry is a true ghost even while its old
		// IP is still (transiently) reported by Kubernetes. Forgetting it here — instead of only
		// when its IP is no longer a live pod — clears the unreachable masters that otherwise make
		// the subsequent redis-cli --cluster fix abort ("Fixing slots coverage with N unreachable
		// masters is dangerous"), so slot re-coverage can make progress in this cycle rather than
		// being deferred until gossip ages the entry out on its own. Only confirmed fail is treated
		// as a ghost; transient pfail/fail? are left alone. Slot-owner protection below still keeps
		// persistent slot owners in place so they can reclaim their data on rejoin.
		staleGhost := expected && isConfirmedFail(cn.Flags)
		if expected && !staleGhost {
			continue
		}
		ownsSlots := redis.OwnsSlots(cn.Slots)
		if ownsSlots {
			// The stale node still owns slots. If it has a reachable replica, force that replica to
			// take over: this preserves the slots (served by the replica's data copy) and, once the
			// dead node is no longer anyone's master, lets the FORGET below succeed instead of being
			// rejected with "Can't forget my master". This is the failover Robin performs when Redis'
			// own auto-failover is stuck (e.g. the replica exceeded cluster-replica-validity-factor).
			if hr.promoteReplicaOfDeadMaster(ctx, clusterNodes, expectedIPs, password, cn.ID) {
				hr.logger.Info("Promoted a replica of a slot-owning stale node; deferring forget to next cycle",
					"nodeID", cn.ID, "nodeIP", cn.IP)
				forgot = true // topology changed — requeue and re-evaluate from the promoted state
				continue
			}
			if !ephemeral {
				// Persistent cluster with no replica to promote: this node keeps its identity and
				// will reclaim its slots on rejoin. Forgetting and reassigning now would strand its
				// on-disk data, so leave it in place and wait for it to come back.
				hr.logger.Info("Skipping forget of slot-owning stale node on persistent cluster (no replica to promote)",
					"nodeID", cn.ID, "nodeIP", cn.IP, "nodeFlags", cn.Flags, "slots", cn.Slots)
				continue
			}
			// Ephemeral with no replica: the recreated pod lost its data, so the slot-owning entry is
			// a true phantom. Fall through to forget it; its slots are re-covered by cluster fix.
		}
		hr.logger.Info("Forgetting stale node from cluster",
			"nodeID", cn.ID, "nodeIP", cn.IP, "nodeFlags", cn.Flags, "ownsSlots", ownsSlots, "staleGhost", staleGhost)
		hr.forgetFromAllPeers(ctx, nodes, password, cn.ID)
		forgot = true
		if ownsSlots {
			forgotSlotOwner = true
		}
	}

	met := false
	if meetMissing {
		// Meet nodes that are missing from cluster view.
		//
		// The node we queried reports itself in CLUSTER NODES with an empty IP (the
		// "myself" entry shows ":6379@16379"). If we indexed that empty string, the seed —
		// which is always a live cluster member — would not be recognized as in-cluster.
		// When the cluster has collapsed to a single surviving master (e.g. after a chaos
		// event wiped every other node), that seed is the ONLY member able to issue MEET,
		// so failing to recognize it leaves the meet loop unable to readmit any node and
		// the healing spins forever. Resolve the empty self-IP to the seed's real IP.
		seedIP := extractIP(seedAddr)
		clusterIPs := make(map[string]struct{}, len(clusterNodes))
		for _, cn := range clusterNodes {
			ip := cn.IP
			if ip == "" {
				ip = seedIP
			}
			clusterIPs[ip] = struct{}{}
		}

		for _, node := range nodes {
			ip := extractIP(node.Addr)
			if _, inCluster := clusterIPs[ip]; inCluster {
				continue
			}
			hr.logger.Info("Meeting missing node from all cluster members", "addr", node.Addr, "ip", ip)
			// Issue CLUSTER MEET from all existing cluster nodes, not just the seed.
			// After CLUSTER FORGET, each node maintains a 60-second blacklist for the
			// forgotten node's ID. CLUSTER MEET is silently dropped if the revealed ID
			// is blacklisted. By meeting from all nodes we ensure the node is re-added
			// as soon as any node's blacklist entry expires.
			hr.meetFromAllPeers(ctx, nodes, clusterIPs, password, ip)
			met = true
		}
	}

	// Re-cover slots orphaned by forgetting a slot-owning node (only possible on ephemeral
	// clusters, where the recreated pod lost its data). redis-cli refuses to rebalance or
	// reshard a cluster with uncovered slots, so this repair is what lets the subsequent
	// operation make progress. Forgetting slotless nodes orphans nothing, so no fix runs.
	if forgotSlotOwner {
		if fixErr := hr.clusterFix(ctx, seedClient); fixErr != nil {
			hr.logger.Warn("Failed to fix cluster after forgetting stale slot owner, continuing", "error", fixErr)
		}
	}

	changed := forgot || met
	if changed {
		// Wait for gossip convergence before the caller re-evaluates the topology.
		meetWait := hr.runtimeConfig.ClusterMeetWait()
		hr.logger.Info("Membership changed, waiting for gossip convergence", "duration", meetWait)
		select {
		case <-ctx.Done():
			return changed, ctx.Err()
		case <-time.After(meetWait):
		}
	}

	hr.logger.Info("Membership healing complete", "forgot", forgot, "forgotSlotOwner", forgotSlotOwner, "met", met)
	return changed, nil
}

// clusterFixTimeout bounds every redis-cli --cluster fix invocation (membership healing here, and
// the scale reconciler's pre-rebalance open-slot repair). A legitimate open-slot repair or slot
// re-cover completes in seconds; a fix that runs much longer is almost always blocked on the TCP
// connect of a dead pod's IP that still lingers in the gossip table (redis-cli's own connect timeout
// is very long, ~2 min). Bounding it here — well below the RebalanceTimeout used for genuine
// rebalances — caps the worst-case stall so the reconcile loop fails fast and re-evaluates against a
// fresher topology, instead of blocking for minutes on ghosts that will age out of gossip.
const clusterFixTimeout = 45 * time.Second

// clusterFix runs redis-cli --cluster fix from the given seed client, bounded by clusterFixTimeout
// (or the configured rebalance timeout when it is shorter). It both closes open (migrating/importing)
// slots and reassigns slots left uncovered after forgetting a slot-owning stale node.
func (hr *HealthReconciler) clusterFix(ctx context.Context, seed *redis.Client) error {
	timeout := clusterFixTimeout
	if rebalance := hr.runtimeConfig.RebalanceTimeout(); rebalance > 0 && rebalance < timeout {
		timeout = rebalance
	}
	fixCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if _, err := seed.ClusterFix(fixCtx); err != nil {
		return fmt.Errorf("cluster fix: %w", err)
	}
	return nil
}

// --- Remediation: Slot Coverage ---

// remediateSlotCoverage assigns missing slots to primaries.
func (hr *HealthReconciler) remediateSlotCoverage(ctx context.Context, report *health.Report, nodes []health.Node, password string) error {
	hr.logger.Info("Remediating slot coverage")

	// Identify primaries and their current slots
	type primaryInfo struct {
		addr     string
		id       string
		numSlots int
	}

	var primaries []primaryInfo
	assignedSlots := make(map[int]struct{})

	for _, cn := range report.ClusterNodes {
		if !strings.Contains(cn.Flags, "master") {
			continue
		}
		if cn.IP == "" {
			hr.logger.Warn("Skipping primary with empty IP in slot coverage remediation", "nodeID", cn.ID, "addr", cn.Addr)
			continue
		}
		ranges, err := redis.ParseSlotRanges(cn.Slots)
		if err != nil {
			continue
		}
		count := redis.CountSlots(ranges)
		primaries = append(primaries, primaryInfo{addr: cn.IP + ":6379", id: cn.ID, numSlots: count})
		for _, r := range ranges {
			for s := r.Start; s <= r.End; s++ {
				assignedSlots[s] = struct{}{}
			}
		}
	}

	if len(primaries) == 0 {
		return fmt.Errorf("no primaries found in cluster nodes")
	}

	// Find missing slots
	var missingSlots []int
	for s := range clusterTotalSlots {
		if _, ok := assignedSlots[s]; !ok {
			missingSlots = append(missingSlots, s)
		}
	}

	if len(missingSlots) == 0 {
		hr.logger.Info("No missing slots found")
		return nil
	}

	sort.Ints(missingSlots)
	hr.logger.Info("Found missing slots", "count", len(missingSlots))

	// Distribute missing slots among primaries: fill up nodes that need more
	slotsPerPrimary := int(math.Ceil(float64(clusterTotalSlots) / float64(len(primaries))))

	// Sort primaries by current slot count (ascending) to fill emptier ones first
	sort.Slice(primaries, func(i, j int) bool {
		return primaries[i].numSlots < primaries[j].numSlots
	})

	slotIdx := 0
	for i := range primaries {
		if slotIdx >= len(missingSlots) {
			break
		}

		needed := slotsPerPrimary - primaries[i].numSlots
		if needed <= 0 {
			continue
		}

		// Last primary gets all remaining
		end := slotIdx + needed
		if i == len(primaries)-1 || end > len(missingSlots) {
			end = len(missingSlots)
		}

		slotsToAssign := missingSlots[slotIdx:end]
		if len(slotsToAssign) == 0 {
			continue
		}

		client := hr.clientFactory(primaries[i].addr, password)
		err := client.ClusterAddSlots(ctx, slotsToAssign...)
		_ = client.Close()
		if err != nil {
			if strings.Contains(err.Error(), "already busy") {
				// Config disagreement: the seed thinks slots are unassigned, but
				// the target node sees them as owned by someone. Resolve via SETSLOT.
				hr.logger.Warn("Slot already busy, resolving config disagreement",
					"target", primaries[i].addr, "slots", len(slotsToAssign), "error", err)
				if resolveErr := hr.resolveSlotDisagreement(ctx, nodes, password, missingSlots); resolveErr != nil {
					return fmt.Errorf("resolving slot disagreement: %w", resolveErr)
				}
				// All missing slots handled via SETSLOT, stop ADDSLOTS loop.
				break
			}
			return fmt.Errorf("assigning slots to %s: %w", primaries[i].addr, err)
		}

		hr.logger.Info("Assigned missing slots to primary",
			"primary", primaries[i].addr, "count", len(slotsToAssign))
		slotIdx = end
	}

	// Wait for convergence
	meetWait := hr.runtimeConfig.ClusterMeetWait()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(meetWait):
	}

	hr.logger.Info("Slot coverage remediation complete")
	return nil
}

// resolveSlotDisagreement handles the case where the seed node thinks slots are
// unassigned but other nodes still see them as owned. It queries all nodes to find
// the actual owner (by highest epoch/majority) and forces agreement via CLUSTER SETSLOT NODE.
func (hr *HealthReconciler) resolveSlotDisagreement(ctx context.Context, nodes []health.Node, password string, disputedSlots []int) error {
	const perNodeTimeout = 5 * time.Second

	// Build a lookup set for fast slot membership check.
	disputedSet := make(map[int]struct{}, len(disputedSlots))
	for _, s := range disputedSlots {
		disputedSet[s] = struct{}{}
	}

	// Query all nodes to find who owns each disputed slot.
	// ownerInfo: slot -> (nodeID with highest epoch/votes)
	type candidate struct {
		nodeID string
		epoch  int
		votes  int
	}
	slotOwners := make(map[int]*candidate)

	for _, node := range nodes {
		queryCtx, cancel := context.WithTimeout(ctx, perNodeTimeout)
		client := hr.clientFactory(node.Addr, password)
		clusterNodes, err := client.GetClusterNodes(queryCtx)
		_ = client.Close()
		cancel()
		if err != nil {
			continue
		}
		for _, cn := range clusterNodes {
			if !strings.Contains(cn.Flags, "master") {
				continue
			}
			ranges, err := redis.ParseSlotRanges(cn.Slots)
			if err != nil {
				continue
			}
			for _, rng := range ranges {
				for slot := rng.Start; slot <= rng.End; slot++ {
					if _, disputed := disputedSet[slot]; !disputed {
						continue
					}
					existing := slotOwners[slot]
					if existing == nil {
						slotOwners[slot] = &candidate{nodeID: cn.ID, epoch: cn.Epoch, votes: 1}
					} else if cn.ID == existing.nodeID {
						existing.votes++
						if cn.Epoch > existing.epoch {
							existing.epoch = cn.Epoch
						}
					} else if cn.Epoch > existing.epoch || (cn.Epoch == existing.epoch && 1 > existing.votes) {
						slotOwners[slot] = &candidate{nodeID: cn.ID, epoch: cn.Epoch, votes: 1}
					}
				}
			}
		}
	}

	if len(slotOwners) == 0 {
		return fmt.Errorf("no owner found for disputed slots across any node")
	}

	hr.logger.Info("Resolving slot disagreement via CLUSTER SETSLOT",
		"disputedSlots", len(disputedSlots), "ownersFound", len(slotOwners))

	// Force agreement on all nodes using CLUSTER SETSLOT <slot> NODE <owner>.
	var setSlotErrors int
	for _, node := range nodes {
		setCtx, cancel := context.WithTimeout(ctx, perNodeTimeout)
		client := hr.clientFactory(node.Addr, password)
		for slot, owner := range slotOwners {
			if err := client.ClusterSetSlotNode(setCtx, slot, owner.nodeID); err != nil {
				setSlotErrors++
			}
		}
		_ = client.Close()
		cancel()
	}

	// For slots that had no owner from any node (truly unassigned everywhere),
	// assign them to the first primary via ADDSLOTS.
	var trulyMissing []int
	for _, s := range disputedSlots {
		if _, found := slotOwners[s]; !found {
			trulyMissing = append(trulyMissing, s)
		}
	}
	if len(trulyMissing) > 0 {
		// Pick the first node to own these truly missing slots.
		client := hr.clientFactory(nodes[0].Addr, password)
		if err := client.ClusterAddSlots(ctx, trulyMissing...); err != nil {
			_ = client.Close()
			return fmt.Errorf("assigning truly missing slots to %s: %w", nodes[0].Addr, err)
		}
		_ = client.Close()
	}

	if setSlotErrors > 0 {
		hr.logger.Warn("Some CLUSTER SETSLOT commands failed during slot disagreement resolution",
			"errors", setSlotErrors)
	}

	hr.logger.Info("Slot disagreement resolution complete", "resolvedViaSetSlot", len(slotOwners), "trulyMissing", len(trulyMissing))
	return nil
}

// --- Remediation: Replica Spread ---

// replicaInfo describes a replica node in the cluster topology.
type replicaInfo struct {
	id        string
	addr      string
	primaryID string
}

// remediateReplicaSpread fixes the replica-to-primary mapping. It returns true when it
// reshaped the cluster topology (promoted or demoted nodes), signalling the caller to
// defer the remaining health checks to the next cycle so they run against fresh state.
func (hr *HealthReconciler) remediateReplicaSpread(ctx context.Context, report *health.Report, nodes []health.Node, password string, desiredPrimaries, desiredReplicasPerPrimary int) (bool, error) {
	hr.logger.Info("Remediating replica spread")

	// Identify primaries and replicas from cluster state
	primaryIDs := make(map[string]string) // id -> addr
	var replicas []replicaInfo

	for _, cn := range report.ClusterNodes {
		if cn.IP == "" {
			hr.logger.Warn("Skipping node with empty IP in replica spread remediation", "nodeID", cn.ID, "addr", cn.Addr)
			continue
		}
		if strings.Contains(cn.Flags, "master") {
			primaryIDs[cn.ID] = cn.IP + ":6379"
		} else if strings.Contains(cn.Flags, "slave") {
			replicas = append(replicas, replicaInfo{
				id:        cn.ID,
				addr:      cn.IP + ":6379",
				primaryID: cn.Primary,
			})
		}
	}

	// If primary count doesn't match desired, check for failover scenario
	if len(primaryIDs) != desiredPrimaries {
		if len(primaryIDs) > desiredPrimaries {
			// Failover scenario: a replica was promoted and the replacement pod joined as an empty primary.
			// Demote empty primaries (0 slots, not importing/migrating) to replicas of primaries that need them.
			return hr.remediateExcessPrimaries(ctx, report, primaryIDs, replicas, password, desiredPrimaries, desiredReplicasPerPrimary)
		}
		// Fewer primaries than desired. If the cluster carries surplus replicas (more
		// replicas than the target topology requires), promote them to empty primaries so
		// the cluster can reach the desired primary count and a later balance step can
		// distribute slots onto them. Without this, a chaos event that leaves the cluster
		// with too few primaries but an extra replica (e.g. a node that rejoined as a
		// replica during scale-up) deadlocks: replica spread stays broken forever because
		// nothing ever promotes the surplus replica.
		return hr.remediatePrimaryDeficit(ctx, primaryIDs, replicas, nodes, password, desiredPrimaries, desiredReplicasPerPrimary)
	}

	// Count replicas per primary
	replicaCount := make(map[string]int)
	var orphaned []replicaInfo     // replicas pointing to unknown primaries
	var overAssigned []replicaInfo // excess replicas from primaries with too many

	for _, r := range replicas {
		if _, exists := primaryIDs[r.primaryID]; !exists {
			orphaned = append(orphaned, r)
		} else {
			replicaCount[r.primaryID]++
		}
	}

	// Find primaries with too many replicas (collect excess)
	for _, r := range replicas {
		if _, exists := primaryIDs[r.primaryID]; !exists {
			continue
		}
		if replicaCount[r.primaryID] > desiredReplicasPerPrimary {
			overAssigned = append(overAssigned, r)
			replicaCount[r.primaryID]--
		}
	}

	// Find primaries that need more replicas
	var needsReplicas []string // primary IDs needing replicas
	for id := range primaryIDs {
		deficit := desiredReplicasPerPrimary - replicaCount[id]
		for range deficit {
			needsReplicas = append(needsReplicas, id)
		}
	}

	// Available replicas to reassign: orphaned + overAssigned
	available := append(orphaned, overAssigned...)

	if len(available) < len(needsReplicas) {
		hr.logger.Warn("Not enough replicas to fully remediate spread",
			"available", len(available), "needed", len(needsReplicas))
	}

	// Reassign replicas
	reassignCount := min(len(available), len(needsReplicas))
	for i := range reassignCount {
		replica := available[i]
		targetPrimaryID := needsReplicas[i]

		hr.logger.Info("Reassigning replica to primary",
			"replica", replica.addr, "replicaID", replica.id,
			"fromPrimary", replica.primaryID, "toPrimary", targetPrimaryID)

		client := hr.clientFactory(replica.addr, password)
		if err := client.ClusterReplicate(ctx, targetPrimaryID); err != nil {
			_ = client.Close()
			return false, fmt.Errorf("replicating %s to primary %s: %w", replica.addr, targetPrimaryID, err)
		}
		_ = client.Close()
	}

	if reassignCount > 0 {
		hr.logger.Info("Replica spread remediation complete", "reassigned", reassignCount)
	} else {
		hr.logger.Info("No replica reassignment needed")
	}

	// Reassigning replicas only changes which primary a replica follows; it does not
	// change primary/replica counts, so the remaining health checks may safely proceed.
	return false, nil
}

// RemediateReplicaTopology validates the cluster's primary/replica distribution against the
// desired topology and remediates it if wrong. It is the operation-time counterpart of the
// replica-spread step in Reconcile: the scaling and upgrade reconcilers call it just before
// declaring an operation complete, so a cluster never reaches Ready with the wrong number of
// primaries or with a primary that has the wrong number of replicas — a state that a pod
// recreated mid-operation can leave behind when it rejoins with the wrong role, and which
// the slot/state checks in verifyCluster do not catch.
//
// It returns ok=true when the topology already matches (nothing to do). Otherwise it runs
// the replica-spread remediation and returns ok=false, with changed indicating whether a fix
// was applied, so the caller can requeue and re-check on the next cycle.
func (hr *HealthReconciler) RemediateReplicaTopology(ctx context.Context, nodes []health.Node, password string, desiredPrimaries, desiredReplicasPerPrimary int) (ok bool, changed bool, err error) {
	if len(nodes) == 0 {
		return false, false, fmt.Errorf("no nodes provided for replica topology check")
	}

	// Read the current cluster view from the first reachable node.
	var clusterNodes []redis.ClusterNode
	for _, n := range nodes {
		client := hr.clientFactory(n.Addr, password)
		cn, e := client.GetClusterNodes(ctx)
		_ = client.Close()
		if e == nil && len(cn) > 0 {
			clusterNodes = cn
			break
		}
	}
	if len(clusterNodes) == 0 {
		return false, false, fmt.Errorf("no reachable node to read cluster topology")
	}

	if health.ReplicaSpreadOK(clusterNodes, desiredPrimaries, desiredReplicasPerPrimary) {
		return true, false, nil
	}

	hr.logger.Info("Replica topology does not match desired, remediating",
		"desiredPrimaries", desiredPrimaries, "desiredReplicasPerPrimary", desiredReplicasPerPrimary)
	report := &health.Report{ClusterNodes: clusterNodes}
	changed, err = hr.remediateReplicaSpread(ctx, report, nodes, password, desiredPrimaries, desiredReplicasPerPrimary)
	return false, changed, err
}

// remediateExcessPrimaries handles the case where more primaries exist than desired —
// typically after a failover where a replica was promoted and the replacement pod joined
// as an empty master, but also after a topology corruption / split-brain that leaves a
// surplus primary still holding slots.
//
// It prefers demoting EMPTY primaries (no data to move) to replicas of primaries that lack
// them. When there are no empty primaries to demote but a surplus still holds slots, it
// drains the surplus primaries' slots with a weight-0 rebalance so they become empty and
// can be demoted on a subsequent cycle. This mirrors the old robin's convertNodesToReplica
// (drain-then-convert) and guarantees the replica-topology gate can always converge instead
// of deadlocking on a slot-owning excess primary.
func (hr *HealthReconciler) remediateExcessPrimaries(
	ctx context.Context,
	report *health.Report,
	primaryIDs map[string]string,
	replicas []replicaInfo,
	password string,
	desiredPrimaries, desiredReplicasPerPrimary int,
) (bool, error) {
	excess := len(primaryIDs) - desiredPrimaries
	if excess <= 0 {
		return false, nil
	}
	hr.logger.Info("Detected excess primaries, remediating",
		"actual", len(primaryIDs), "desired", desiredPrimaries, "excess", excess)

	// Classify primaries into empty (0 slots, no in-flight migration) and non-empty (slot
	// owners, with their slot count for deterministic drain selection).
	type primaryNode struct {
		id    string
		addr  string
		slots int
	}
	var emptyPrimaries, nonEmptyPrimaries []primaryNode
	for _, cn := range report.ClusterNodes {
		if cn.IP == "" || !strings.Contains(cn.Flags, "master") {
			continue
		}
		pn := primaryNode{id: cn.ID, addr: cn.IP + ":6379"}
		// A node with an in-flight migration marker ('[') is treated as non-empty so we
		// never demote a node mid-migration.
		if strings.Contains(cn.Slots, "[") {
			nonEmptyPrimaries = append(nonEmptyPrimaries, pn)
			continue
		}
		parsed, err := redis.ParseSlotRanges(cn.Slots)
		if err == nil {
			pn.slots = redis.CountSlots(parsed)
		}
		if pn.slots > 0 {
			nonEmptyPrimaries = append(nonEmptyPrimaries, pn)
		} else {
			emptyPrimaries = append(emptyPrimaries, pn)
		}
	}

	// Prefer demoting empty primaries: they carry no data, so demotion is instant.
	if len(emptyPrimaries) > 0 {
		emptyIDs := make(map[string]struct{}, len(emptyPrimaries))
		for _, ep := range emptyPrimaries {
			emptyIDs[ep.id] = struct{}{}
		}

		// Count replicas of each non-empty primary to find those below the desired count.
		replicaCount := make(map[string]int)
		for _, r := range replicas {
			if _, isTarget := primaryIDs[r.primaryID]; isTarget {
				replicaCount[r.primaryID]++
			}
		}
		var needsReplicas []string
		for id := range primaryIDs {
			if _, isEmpty := emptyIDs[id]; isEmpty {
				continue
			}
			deficit := desiredReplicasPerPrimary - replicaCount[id]
			for range deficit {
				needsReplicas = append(needsReplicas, id)
			}
		}

		demoteEmpty := min(excess, len(emptyPrimaries))
		demoted := 0
		for i := range demoteEmpty {
			if i >= len(needsReplicas) {
				hr.logger.Warn("No more primaries needing replicas, stopping empty-primary demotion",
					"demoted", demoted, "remaining", demoteEmpty-demoted)
				break
			}

			ep := emptyPrimaries[i]
			targetPrimaryID := needsReplicas[i]

			hr.logger.Info("Demoting empty primary to replica",
				"emptyPrimary", ep.addr, "emptyPrimaryID", ep.id,
				"targetPrimary", targetPrimaryID)

			client := hr.clientFactory(ep.addr, password)
			if err := client.ClusterReplicate(ctx, targetPrimaryID); err != nil {
				_ = client.Close()
				return false, fmt.Errorf("demoting %s to replica of %s: %w", ep.addr, targetPrimaryID, err)
			}
			_ = client.Close()
			demoted++
		}

		hr.logger.Info("Excess primary demotion complete", "demoted", demoted)
		// Demoting reshapes the topology; defer the re-check to the next cycle. If excess
		// remains (fewer empties than surplus), a later cycle drains the slot-owning surplus.
		return demoted > 0, nil
	}

	// No empty primaries, but a surplus of slot-owning primaries remains. Drain the surplus
	// primaries (fewest slots first, to minimise data movement) so they become empty and can
	// be demoted next cycle. Never drain below desiredPrimaries.
	sort.Slice(nonEmptyPrimaries, func(i, j int) bool {
		return nonEmptyPrimaries[i].slots < nonEmptyPrimaries[j].slots
	})
	drainCount := excess
	if drainCount > len(nonEmptyPrimaries)-1 {
		drainCount = len(nonEmptyPrimaries) - 1
	}
	if drainCount <= 0 {
		hr.logger.Warn("Excess primaries but none can be safely drained, skipping",
			"nonEmptyPrimaries", len(nonEmptyPrimaries))
		return false, nil
	}

	drainSet := make(map[string]struct{}, drainCount)
	drainIDs := make([]string, 0, drainCount)
	for i := range drainCount {
		drainIDs = append(drainIDs, nonEmptyPrimaries[i].id)
		drainSet[nonEmptyPrimaries[i].id] = struct{}{}
	}

	// Seed the rebalance from a keeper primary (one not being drained) so its slots have
	// a destination.
	var seedAddr string
	for id, addr := range primaryIDs {
		if _, draining := drainSet[id]; !draining {
			seedAddr = addr
			break
		}
	}
	if seedAddr == "" {
		return false, fmt.Errorf("no keeper primary available to seed drain rebalance")
	}

	hr.logger.Info("Draining surplus primaries so they can be demoted next cycle",
		"drainCount", drainCount, "drainIDs", drainIDs)
	if err := hr.drainPrimaries(ctx, seedAddr, password, drainIDs); err != nil {
		return false, fmt.Errorf("draining surplus primaries: %w", err)
	}
	return true, nil
}

// drainPrimaries moves all slots off the given primaries (by node ID) onto the remaining
// primaries via a weight-0 rebalance seeded from seedAddr, bounded by the configured
// rebalance timeout. The drained nodes end up as empty masters, ready to be demoted to
// replicas.
func (hr *HealthReconciler) drainPrimaries(ctx context.Context, seedAddr, password string, drainIDs []string) error {
	weights := make(map[string]int, len(drainIDs))
	for _, id := range drainIDs {
		weights[id] = 0
	}

	rebalanceCtx := ctx
	if timeout := hr.runtimeConfig.RebalanceTimeout(); timeout > 0 {
		var cancel context.CancelFunc
		rebalanceCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	client := hr.clientFactory(seedAddr, password)
	defer func() { _ = client.Close() }()

	if _, err := client.ClusterRebalanceWithWeights(rebalanceCtx, weights); err != nil {
		return fmt.Errorf("weight-0 rebalance from %s: %w", seedAddr, err)
	}
	return nil
}

// remediatePrimaryDeficit handles the case where the cluster has fewer primaries than
// desired but still carries replicas that can be promoted. This occurs after chaos events
// (e.g. pod deletion during scale-up) leave a node attached as a replica instead of
// becoming the expected empty primary, which would otherwise deadlock the health
// reconciler: replica spread stays broken forever because no other step promotes the
// surplus replica.
//
// Surplus replicas (those beyond what the target topology requires) are promoted to empty
// primaries by detaching them from replication via CLUSTER RESET (soft) and re-MEETing
// them, mirroring the original robin's convertNodesToPrimary flow. A later reconciliation
// cycle then rebalances slots onto the newly empty primaries. If there are no surplus
// replicas, the deficit must be resolved by adding pods (scale-up), which is outside the
// health reconciler's scope, so it skips.
//
// It returns true when it promoted at least one replica, signalling the caller to defer
// the remaining health checks to the next cycle.
func (hr *HealthReconciler) remediatePrimaryDeficit(
	ctx context.Context,
	primaryIDs map[string]string,
	replicas []replicaInfo,
	nodes []health.Node,
	password string,
	desiredPrimaries, desiredReplicasPerPrimary int,
) (bool, error) {
	deficit := desiredPrimaries - len(primaryIDs)

	// Replicas beyond the target topology's needs are surplus and may be promoted. The
	// rest are still required to satisfy replica spread once the primary count recovers.
	desiredReplicaTotal := desiredPrimaries * desiredReplicasPerPrimary
	surplus := len(replicas) - desiredReplicaTotal
	if surplus <= 0 {
		hr.logger.Info("Primary count below desired and no surplus replicas to promote, skipping replica spread remediation",
			"actual", len(primaryIDs), "desired", desiredPrimaries, "replicas", len(replicas))
		return false, nil
	}

	promoteCount := min(deficit, surplus)
	hr.logger.Info("Primary count below desired, promoting surplus replicas to empty primaries",
		"actual", len(primaryIDs), "desired", desiredPrimaries,
		"surplusReplicas", surplus, "promoting", promoteCount)

	// Detach each selected replica into a standalone empty master via CLUSTER RESET SOFT.
	promoted := make([]replicaInfo, 0, promoteCount)
	for i := 0; i < promoteCount && i < len(replicas); i++ {
		r := replicas[i]
		hr.logger.Info("Promoting replica to empty primary",
			"replica", r.addr, "replicaID", r.id, "fromPrimary", r.primaryID)

		client := hr.clientFactory(r.addr, password)
		if err := client.ClusterReset(ctx, false); err != nil {
			_ = client.Close()
			return false, fmt.Errorf("resetting replica %s to empty primary: %w", r.addr, err)
		}
		_ = client.Close()
		promoted = append(promoted, r)
	}

	if len(promoted) == 0 {
		return false, nil
	}

	// Re-MEET the reset nodes so they rejoin the cluster as empty primaries, then wait for
	// gossip convergence before the next cycle rebalances slots onto them.
	hr.meetResetReplicas(ctx, promoted, nodes, password)

	meetWait := hr.runtimeConfig.ClusterMeetWait()
	hr.logger.Info("Waiting for gossip convergence after promoting replicas", "duration", meetWait)
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-time.After(meetWait):
	}

	hr.logger.Info("Replica promotion complete, deferring rebalance to next cycle", "promoted", len(promoted))
	return true, nil
}

// meetResetReplicas re-introduces nodes that were just reset into empty primaries by
// issuing CLUSTER MEET toward each of them from every other cluster member, so gossip
// re-admits them. Meets are best-effort: a single peer failure is logged and tolerated
// because any successful meet is enough for the node to rejoin.
func (hr *HealthReconciler) meetResetReplicas(ctx context.Context, resetNodes []replicaInfo, nodes []health.Node, password string) {
	resetIPs := make(map[string]struct{}, len(resetNodes))
	for _, r := range resetNodes {
		resetIPs[extractIP(r.addr)] = struct{}{}
	}

	for _, r := range resetNodes {
		targetIP := extractIP(r.addr)
		for _, node := range nodes {
			peerIP := extractIP(node.Addr)
			if peerIP == "" {
				continue
			}
			if _, isReset := resetIPs[peerIP]; isReset {
				continue // don't meet from a node we just reset
			}
			peerClient := hr.clientFactory(node.Addr, password)
			if meetErr := peerClient.ClusterMeet(ctx, targetIP, redis.DefaultPort); meetErr != nil {
				hr.logger.Warn("Failed to meet promoted node from peer",
					"peer", node.Addr, "target", targetIP, "error", meetErr)
			}
			_ = peerClient.Close()
		}
	}
}

// --- Remediation: Cluster Check (Fix) ---

// remediateClusterCheck runs redis-cli --cluster fix to repair cluster issues.
// It attempts the fix from each node in the cluster, since nodes may have different
// views of the configuration. If all attempts fail, it returns nil so the pipeline
// can continue to the balance step (which may resolve the underlying issue).
func (hr *HealthReconciler) remediateClusterCheck(ctx context.Context, nodes []health.Node, password string) error {
	hr.logger.Info("Remediating cluster check issues with redis-cli --cluster fix")

	if len(nodes) == 0 {
		return fmt.Errorf("no nodes available for cluster fix")
	}

	timeout := hr.runtimeConfig.ClusterCommandTimeout()

	var lastErr error
	for _, node := range nodes {
		fixCtx, cancel := context.WithTimeout(ctx, timeout)
		client := hr.clientFactory(node.Addr, password)

		result, err := client.ClusterFix(fixCtx)
		_ = client.Close()
		cancel()

		if err == nil {
			hr.logger.Info("Cluster fix completed",
				"node", node.Addr,
				"exitCode", result.CommandCodeOutput,
				"output", truncateOutput(result.Output, 500))
			return nil
		}

		hr.logger.Warn("Cluster fix failed on node, trying next",
			"node", node.Addr, "error", err)
		lastErr = err
	}

	// All nodes failed — try to resolve config disagreement directly.
	hr.logger.Warn("Cluster fix failed on all nodes, attempting config disagreement resolution",
		"lastError", lastErr, "nodesAttempted", len(nodes))

	if resolveErr := hr.resolveConfigDisagreement(ctx, nodes, password); resolveErr != nil {
		hr.logger.Warn("Config disagreement resolution failed", "error", resolveErr)
		return fmt.Errorf("cluster fix failed on all %d nodes: %w", len(nodes), lastErr)
	}

	hr.logger.Info("Config disagreement resolved via CLUSTER SETSLOT")
	return nil
}

// resolveConfigDisagreement queries CLUSTER NODES from all nodes and forces
// agreement on disputed slots using CLUSTER SETSLOT <slot> NODE <owner>.
// This handles the case where nodes disagree about slot ownership after a
// reshard/migration, which redis-cli --cluster fix cannot resolve.
func (hr *HealthReconciler) resolveConfigDisagreement(ctx context.Context, nodes []health.Node, password string) error {
	if len(nodes) < 2 {
		return fmt.Errorf("need at least 2 nodes to resolve disagreement")
	}

	// Use a short per-node timeout for CLUSTER NODES queries (not the CLI timeout).
	const perNodeTimeout = 5 * time.Second

	// Step 1: Query CLUSTER NODES from all nodes to build per-node slot maps.
	type nodeView struct {
		addr         string
		clusterNodes []redis.ClusterNode
	}
	views := make([]nodeView, 0, len(nodes))
	for _, node := range nodes {
		queryCtx, cancel := context.WithTimeout(ctx, perNodeTimeout)
		client := hr.clientFactory(node.Addr, password)
		clusterNodes, err := client.GetClusterNodes(queryCtx)
		_ = client.Close()
		cancel()
		if err != nil {
			hr.logger.Warn("Failed to query CLUSTER NODES for disagreement resolution",
				"node", node.Addr, "error", err)
			continue
		}
		views = append(views, nodeView{addr: node.Addr, clusterNodes: clusterNodes})
	}

	if len(views) < 2 {
		return fmt.Errorf("could not query enough nodes for disagreement resolution")
	}

	// Step 2: Build slot ownership map from each node's perspective.
	// For each slot, collect the owner claimed by each view.
	// The "correct" owner is determined by highest epoch.
	const totalSlots = 16384
	type ownerCandidate struct {
		nodeID string
		epoch  int
		votes  int
	}

	// Build a map: slot -> ownerID -> (epoch, vote count)
	slotCandidates := make(map[int]map[string]*ownerCandidate)

	for _, view := range views {
		for _, cn := range view.clusterNodes {
			if !strings.Contains(cn.Flags, "master") {
				continue
			}
			ranges, err := redis.ParseSlotRanges(cn.Slots)
			if err != nil {
				continue
			}
			for _, rng := range ranges {
				for slot := rng.Start; slot <= rng.End; slot++ {
					if slotCandidates[slot] == nil {
						slotCandidates[slot] = make(map[string]*ownerCandidate)
					}
					if slotCandidates[slot][cn.ID] == nil {
						slotCandidates[slot][cn.ID] = &ownerCandidate{nodeID: cn.ID, epoch: cn.Epoch}
					}
					slotCandidates[slot][cn.ID].votes++
					if cn.Epoch > slotCandidates[slot][cn.ID].epoch {
						slotCandidates[slot][cn.ID].epoch = cn.Epoch
					}
				}
			}
		}
	}

	// Step 3: Find disputed slots (where not all views agree on the same owner).
	// For each disputed slot, pick the owner with the highest epoch (tie-break: most votes).
	type slotFix struct {
		slot    int
		ownerID string
	}
	var fixes []slotFix

	for slot := 0; slot < totalSlots; slot++ {
		candidates := slotCandidates[slot]
		if len(candidates) <= 1 {
			continue // all agree (or uncovered — handled elsewhere)
		}
		// Multiple candidates for this slot — pick best.
		var best *ownerCandidate
		for _, c := range candidates {
			if best == nil || c.epoch > best.epoch || (c.epoch == best.epoch && c.votes > best.votes) {
				best = c
			}
		}
		if best != nil {
			fixes = append(fixes, slotFix{slot: slot, ownerID: best.nodeID})
		}
	}

	// Also find slots that are uncovered (no candidate at all from any view).
	// This shouldn't normally happen if slotsCoveredOK=true, but handle it.

	if len(fixes) == 0 {
		return fmt.Errorf("no disputed slots found, cannot resolve disagreement")
	}

	hr.logger.Info("Found disputed slots, forcing agreement",
		"disputedSlots", len(fixes))

	// Step 4: Send CLUSTER SETSLOT <slot> NODE <owner> to ALL nodes.
	var setSlotErrors int
	for _, node := range nodes {
		setCtx, cancel := context.WithTimeout(ctx, perNodeTimeout)
		client := hr.clientFactory(node.Addr, password)
		for _, fix := range fixes {
			if err := client.ClusterSetSlotNode(setCtx, fix.slot, fix.ownerID); err != nil {
				setSlotErrors++
				// Don't fail on individual slot errors — continue best effort.
			}
		}
		_ = client.Close()
		cancel()
	}

	if setSlotErrors > 0 {
		hr.logger.Warn("Some CLUSTER SETSLOT commands failed",
			"errors", setSlotErrors, "totalCommands", len(fixes)*len(nodes))
	}

	hr.logger.Info("Config disagreement resolution complete",
		"fixedSlots", len(fixes), "errors", setSlotErrors)
	return nil
}

// --- Remediation: Balance ---

// remediateBalance runs redis-cli --cluster rebalance to distribute slots evenly.
func (hr *HealthReconciler) remediateBalance(ctx context.Context, nodes []health.Node, password string) error {
	hr.logger.Info("Remediating cluster balance with redis-cli --cluster rebalance")

	if len(nodes) == 0 {
		return fmt.Errorf("no nodes available for cluster rebalance")
	}

	timeout := hr.runtimeConfig.RebalanceTimeout()
	rebalanceCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	seedAddr := nodes[0].Addr
	client := hr.clientFactory(seedAddr, password)
	defer func() { _ = client.Close() }()

	result, err := client.ClusterRebalance(rebalanceCtx)
	if err != nil {
		return fmt.Errorf("redis-cli --cluster rebalance on %s: %w", seedAddr, err)
	}

	hr.logger.Info("Cluster rebalance completed",
		"exitCode", result.CommandCodeOutput,
		"output", truncateOutput(result.Output, 500))

	return nil
}

// --- Helpers ---

func extractIP(addr string) string {
	if idx := strings.Index(addr, ":"); idx > 0 {
		return addr[:idx]
	}
	return addr
}

func truncateOutput(output string, maxLen int) string {
	if len(output) <= maxLen {
		return output
	}
	return output[:maxLen] + "...(truncated)"
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
