// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	redisv1 "github.com/inditextech/redkey-operator/api/v1beta1"
	robinconfig "github.com/inditextech/redkey-robin/internal/config"
	"github.com/inditextech/redkey-robin/internal/health"
	"github.com/inditextech/redkey-robin/internal/kubernetes"
	"github.com/inditextech/redkey-robin/internal/redis"
)

const (
	// clusterTotalSlots is the total number of hash slots in a Redis cluster.
	clusterTotalSlots = 16384
)

// ClusterReconciler handles the Redis cluster lifecycle state machine.
type ClusterReconciler struct {
	client           client.Client
	clusterName      string
	namespace        string
	runtimeConfig    *robinconfig.RuntimeConfig
	logger           *slog.Logger
	healthReconciler *HealthReconciler
}

// NewClusterReconciler creates a new ClusterReconciler.
func NewClusterReconciler(c client.Client, clusterName, namespace string, runtimeConfig *robinconfig.RuntimeConfig) *ClusterReconciler {
	logger := slog.Default().With("component", "cluster-reconciler", "cluster", clusterName)

	// Create health checker with redis client factory
	healthClientFactory := func(addr, password string) health.ClusterClient {
		return redis.NewClient(addr, password)
	}
	checker := health.NewChecker(
		logger.With("subcomponent", "health-checker"),
		healthClientFactory,
		5*time.Second,
	)

	// Create health reconciler
	redisClientFactory := func(addr, password string) *redis.Client {
		return redis.NewClient(addr, password)
	}
	healthReconciler := NewHealthReconciler(checker, redisClientFactory, runtimeConfig, logger)

	return &ClusterReconciler{
		client:           c,
		clusterName:      clusterName,
		namespace:        namespace,
		runtimeConfig:    runtimeConfig,
		logger:           logger,
		healthReconciler: healthReconciler,
	}
}

// Close releases resources held by the cluster reconciler, closing all cached
// Redis clients used for health checks. It should be called on shutdown.
func (cr *ClusterReconciler) Close() {
	if cr.healthReconciler != nil {
		cr.healthReconciler.Close()
	}
}

// ReconcileCluster processes the cluster creation/configuration state machine.
// It returns whether the outer reconciliation loop should run immediately again
// or wait for the configured interval.
func (cr *ClusterReconciler) ReconcileCluster(ctx context.Context, targetConfig *redisv1.RedkeyConfig, previousConfig *redisv1.RedkeyConfig) (schedule reconcileSchedule, err error) {
	// Standalone (single-node, non-clustered) deployments follow a simplified
	// lifecycle that never issues CLUSTER commands. Route them to a dedicated
	// reconcile path.
	if targetConfig.Spec.IsStandalone() {
		return cr.reconcileStandalone(ctx, targetConfig, previousConfig)
	}

	switch targetConfig.Status.Status {
	case "":
		if previousConfig == nil {
			// New cluster: ensure K8s objects and set status to Initializing.
			return cr.handleNew(ctx, targetConfig)
		}
		// Existing cluster with a new config: detect changes and determine status transition.
		return cr.handleConfigChange(ctx, targetConfig, previousConfig)
	case redisv1.ClusterStatusInitializing:
		// Waiting for pods to be ready, then init nodes.
		return cr.handleInitializing(ctx, targetConfig)
	case redisv1.ClusterStatusConfiguring:
		// Cluster formation: meet, assign slots, set replicas.
		return cr.handleConfiguring(ctx, targetConfig)
	case redisv1.ClusterStatusReady:
		// Cluster is ready, perform health check.
		return cr.handleReady(ctx, targetConfig, previousConfig)
	case redisv1.ClusterStatusScalingUp:
		// Scaling up: add primaries/replicas and rebalance slots onto the new nodes.
		return cr.handleScalingUp(ctx, targetConfig)
	case redisv1.ClusterStatusScalingDown:
		// Scaling down: drain and remove the highest-ordinal nodes, then shrink the StatefulSet.
		return cr.handleScalingDown(ctx, targetConfig)
	case redisv1.ClusterStatusScalingToZero:
		// Scaling to zero: delete all cluster objects.
		return cr.handleScalingToZero(ctx, targetConfig)
	case redisv1.ClusterStatusUpgrading:
		return cr.handleUpgrading(ctx, targetConfig)
	case redisv1.ClusterStatusMaintenance:
		// In maintenance mode, Robin should not perform any operations on the cluster.
		cr.logger.Info("Cluster in maintenance mode, skipping reconciliation")
		return reconcileAfterInterval, nil
	default:
		cr.logger.Info("Unhandled cluster status, skipping", "status", targetConfig.Status.Status)
		return reconcileAfterInterval, nil
	}
}

// handleConfigChange processes a new configuration for an existing cluster.
// It detects what changed and determines the appropriate status transition.
// If auth changes are detected, they are applied via CONFIG SET to all running
// nodes BEFORE any cluster operation, ensuring all nodes share the same auth
// configuration throughout the upgrade/scaling process.
func (cr *ClusterReconciler) handleConfigChange(ctx context.Context, targetConfig *redisv1.RedkeyConfig, previousConfig *redisv1.RedkeyConfig) (reconcileSchedule, error) {
	report := DetectChanges(previousConfig.Spec, targetConfig.Spec)

	cr.logger.Info("Detected configuration changes",
		"config", targetConfig.Name,
		"hasRobinChanges", report.HasRobinChanges,
		"hasTopologyChanges", report.HasTopologyChanges,
		"hasKubernetesChanges", report.HasKubernetesChanges,
		"hasRedisConfigChanges", report.HasRedisConfigChanges,
		"hasAuthChanges", report.HasAuthChanges,
		"hasPurgeKeysOnRebalanceChange", report.HasPurgeKeysOnRebalanceChange,
		"topologyScaleDirection", report.TopologyScaleDirection,
		"primariesDelta", report.PrimariesDelta,
		"replicasDelta", report.ReplicasDelta,
	)

	// If auth changed, apply it via CONFIG SET to all running nodes first.
	// This ensures all nodes share the same auth before any operation
	// (rolling upgrade, scaling, etc.) that might create or recycle pods.
	if report.HasAuthChanges {
		cr.logger.Info("Auth change detected, applying via CONFIG SET to all nodes",
			"config", targetConfig.Name,
			"secretName", targetConfig.Spec.Auth.SecretName)
		if err := cr.applyAuthToAllNodes(ctx, targetConfig, previousConfig); err != nil {
			return reconcileAfterInterval, fmt.Errorf("applying auth config: %w", err)
		}
	}

	// Determine the status transition.
	newStatus := DetermineStatusTransition(report)
	if newStatus == "" {
		// No cluster operation required (auth-only was already applied via CONFIG SET above).
		cr.logger.Info("No cluster operation required, marking config as Applied",
			"config", targetConfig.Name)
		if err := cr.setConfigPhaseApplied(ctx, targetConfig); err != nil {
			return reconcileAfterInterval, err
		}
		// Ensure the cluster status reflects the previous (Ready) state.
		if targetConfig.Status.Status != previousConfig.Status.Status {
			targetConfig.Status.Status = previousConfig.Status.Status
			if err := cr.client.Status().Update(ctx, targetConfig); err != nil {
				return reconcileAfterInterval, fmt.Errorf("updating cluster status from previous: %w", err)
			}
		}
		return reconcileAfterInterval, nil
	}

	// Transition the cluster to the new status (auth was already applied above).
	cr.logger.Info("Transitioning cluster status for config change",
		"config", targetConfig.Name,
		"newStatus", newStatus,
	)
	if err := cr.updateClusterStatus(ctx, targetConfig, newStatus); err != nil {
		return reconcileAfterInterval, err
	}

	return reconcileImmediately, nil
}

// handleNew ensures Kubernetes objects exist and transitions to Initializing.
func (cr *ClusterReconciler) handleNew(ctx context.Context, config *redisv1.RedkeyConfig) (reconcileSchedule, error) {
	// New cluster with 0 primaries: nothing to create, mark Applied immediately.
	if config.Spec.Primaries == 0 {
		cr.logger.Info("New cluster with 0 primaries, marking config as Applied", "config", config.Name)
		config.Status.Status = redisv1.ClusterStatusReady
		config.Status.Nodes = map[string]*redisv1.RedisNode{}
		if err := cr.client.Status().Update(ctx, config); err != nil {
			return reconcileAfterInterval, fmt.Errorf("updating status for zero-primary cluster: %w", err)
		}
		if err := cr.setConfigPhaseApplied(ctx, config); err != nil {
			return reconcileAfterInterval, err
		}
		return reconcileAfterInterval, nil
	}

	cr.logger.Info("New cluster detected, creating Kubernetes objects")

	owner, err := cr.getOwner(ctx)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("getting owner Redkey: %w", err)
	}

	password, err := kubernetes.GetRedisPassword(ctx, cr.client, config.Spec.Auth.SecretName, cr.namespace)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("getting Redis password: %w", err)
	}
	cr.runtimeConfig.SetAuthSecret(config.Spec.Auth.SecretName)

	if err := kubernetes.EnsureClusterObjects(ctx, cr.client, config, owner, password); err != nil {
		return reconcileAfterInterval, fmt.Errorf("ensuring cluster objects: %w", err)
	}

	// Transition to Initializing
	if err := cr.updateClusterStatus(ctx, config, redisv1.ClusterStatusInitializing); err != nil {
		return reconcileAfterInterval, err
	}

	cr.logger.Info("Kubernetes objects created, status set to Initializing")
	return reconcileImmediately, nil
}

// handleInitializing waits for all pods to be ready, then inits nodes.
func (cr *ClusterReconciler) handleInitializing(ctx context.Context, config *redisv1.RedkeyConfig) (reconcileSchedule, error) {
	expectedReplicas := config.Spec.Primaries + (config.Spec.Primaries * config.Spec.ReplicasPerPrimary)

	ready, err := kubernetes.AllPodsReady(ctx, cr.client, cr.clusterName, cr.namespace, expectedReplicas)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("checking pod readiness: %w", err)
	}

	if !ready {
		cr.logger.Info("Waiting for all pods to be ready",
			"expected", expectedReplicas)
		return reconcileAfterWaitInterval, nil
	}

	cr.logger.Info("All pods ready, initializing nodes")

	// Init nodes: connect, get IDs
	password := cr.getPassword(ctx, config)
	nodes, err := cr.initNodes(ctx, config, password)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("initializing nodes: %w", err)
	}
	defer closeNodes(nodes)

	// Verify all nodes responded
	if len(nodes) < int(expectedReplicas) {
		cr.logger.Info("Not all nodes could be initialized, will retry",
			"initialized", len(nodes), "expected", expectedReplicas)
		return reconcileAfterWaitInterval, nil
	}

	// Transition to Configuring
	if err := cr.updateClusterStatus(ctx, config, redisv1.ClusterStatusConfiguring); err != nil {
		return reconcileAfterInterval, err
	}

	cr.logger.Info("All nodes initialized, status set to Configuring")
	return reconcileImmediately, nil
}

// handleConfiguring performs cluster formation: meet, assign slots, set replicas.
func (cr *ClusterReconciler) handleConfiguring(ctx context.Context, config *redisv1.RedkeyConfig) (reconcileSchedule, error) {
	password := cr.getPassword(ctx, config)
	nodes, err := cr.initNodes(ctx, config, password)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("initializing nodes for configuration: %w", err)
	}
	defer closeNodes(nodes)

	expectedTotal := int(config.Spec.Primaries + (config.Spec.Primaries * config.Spec.ReplicasPerPrimary))
	if len(nodes) < expectedTotal {
		cr.logger.Info("Not all nodes available for configuration, will retry",
			"available", len(nodes), "expected", expectedTotal)
		return reconcileAfterWaitInterval, nil
	}

	// Form the cluster from scratch (meet, assign slots, replicas, verify).
	if !cr.formCluster(ctx, config, nodes) {
		return reconcileAfterWaitInterval, nil
	}

	// Success: transition to Ready and set ConfigPhase to Applied
	if err := cr.updateClusterStatus(ctx, config, redisv1.ClusterStatusReady); err != nil {
		return reconcileAfterInterval, err
	}
	if err := cr.setConfigPhaseApplied(ctx, config); err != nil {
		return reconcileAfterInterval, err
	}

	// Populate the data-plane health conditions now so reaching Ready reflects health immediately,
	// rather than leaving them Unknown until the next reconcile cycle runs handleReady.
	cr.refreshHealthConditions(ctx, config, password)

	// Update node topology in status
	if err := cr.updateNodeStatus(ctx, config, nodes); err != nil {
		cr.logger.Error("Failed to update node status", "error", err)
		// Non-critical, continue
	}

	cr.logger.Info("Cluster formation complete, status set to Ready")
	return reconcileAfterInterval, nil
}

// formCluster forms a Redis cluster from a freshly initialized set of nodes by meeting
// the nodes, assigning slots to primaries, and attaching replicas. It returns true once
// the cluster has fully converged; false indicates a transient state and the caller
// should retry on the next reconciliation pass.
//
// The nodes slice must be ordered by StatefulSet ordinal: nodes[0:Primaries] become
// primaries and the remaining nodes become replicas. It is shared by the initial
// configuration flow and the fast-scaling path, which both build a cluster from scratch.
func (cr *ClusterReconciler) formCluster(ctx context.Context, config *redisv1.RedkeyConfig, nodes []*redis.Node) bool {
	// Step 1: Meet all nodes
	if err := cr.meetNodes(ctx, nodes); err != nil {
		cr.logger.Error("Failed to meet nodes", "error", err)
		return false
	}

	// Restore fast-path: when the nodes come back with persisted cluster state that already makes
	// up a fully formed cluster — e.g. scaling a storage cluster back up from zero with
	// deletePVC=false — the cluster is already formed. Its slot ownership may no longer match the
	// ordinal layout this function would impose (a replica promoted during an earlier failover now
	// owns a primary's slots), so running assignSlots would issue ADDSLOTS against the ordinal
	// layout and fail with "slot already busy" against the persisted owner, looping forever. Detect
	// the already-formed cluster and skip formation; the health reconciler will rebalance roles
	// later if needed. The check requires the FULL desired topology (the right number of
	// slot-serving primaries AND attached replicas), so a half-formed initial cluster whose slots
	// were assigned but whose replicas have not been attached yet is NOT short-circuited (otherwise
	// its replicas would never be attached). A fresh cluster has no slots and state "fail", so this
	// never short-circuits a genuine from-scratch formation.
	if formed, err := cr.clusterAlreadyFormed(ctx, config, nodes); err != nil {
		cr.logger.Warn("Failed to check whether cluster is already formed, proceeding with formation", "error", err)
	} else if formed {
		cr.logger.Info("Cluster already formed from persisted state, skipping slot assignment")
		return true
	}

	// Step 2: Assign slots to primaries
	primaries := nodes[:config.Spec.Primaries]
	if err := cr.assignSlots(ctx, primaries); err != nil {
		// A pod deleted mid-formation (e.g. by chaos) leaves its old node ID lingering in the
		// survivors' gossip table, still claiming slots. When the pod is recreated it rejoins empty
		// with a new ID, so assignSlots sees it as slot-less and runs ADDSLOTS — which fails with
		// "slot N is already busy" because gossip still attributes the slot to the ghost. Without
		// intervention this deadlocks the from-scratch formation forever (observed as fast scaling
		// looping on "Slot 0 is already busy"). Forget the ghost via membership healing and retry on
		// the next pass against a clean topology. This only triggers on the ghost condition, so a
		// pristine initial formation is unaffected.
		if strings.Contains(err.Error(), "already busy") {
			cr.logger.Warn("Slot busy during cluster formation (stale ghost owner), healing membership and retrying",
				"error", err)
			if _, healErr := cr.healTopology(ctx, config, false); healErr != nil {
				cr.logger.Warn("Failed to heal membership after slot-busy during formation", "error", healErr)
			}
			return false
		}
		cr.logger.Error("Failed to assign slots", "error", err)
		return false
	}

	// Step 3: Verify gossip convergence before replication, then attach replicas.
	if config.Spec.ReplicasPerPrimary > 0 {
		converged, err := cr.checkGossipConvergence(ctx, nodes)
		if err != nil {
			cr.logger.Warn("Failed to check gossip convergence", "error", err)
			return false
		}
		if !converged {
			cr.logger.Info("Gossip not yet converged, will retry before setting replicas")
			return false
		}

		replicas := nodes[config.Spec.Primaries:]
		if err := cr.setReplicas(ctx, primaries, replicas, config.Spec.ReplicasPerPrimary); err != nil {
			cr.logger.Warn("Failed to set replicas, will retry", "error", err)
			return false
		}
	}

	// Step 4: Verify cluster state
	clusterOK, err := cr.verifyCluster(ctx, nodes[0])
	if err != nil {
		cr.logger.Error("Failed to verify cluster", "error", err)
		return false
	}
	if !clusterOK {
		cr.logger.Info("Cluster not yet converged, will retry")
		return false
	}

	return true
}

// handleReady performs a health check on the ready cluster.
func (cr *ClusterReconciler) handleReady(ctx context.Context, config *redisv1.RedkeyConfig, previousConfig *redisv1.RedkeyConfig) (reconcileSchedule, error) {
	// Build the health node list from current K8s pod state.
	nodes, err := cr.buildHealthNodes(ctx, config)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("getting pod addresses for health check: %w", err)
	}
	if len(nodes) == 0 {
		return reconcileAfterInterval, fmt.Errorf("no pods found for cluster health check")
	}

	// Get Redis password
	password, err := kubernetes.GetRedisPassword(ctx, cr.client, config.Spec.Auth.SecretName, cr.namespace)
	if err != nil {
		cr.logger.Warn("Could not retrieve Redis password for health check", "error", err)
		// Continue without password — it may not be configured
	}

	// Detect an out-of-band password rotation (the auth Secret was edited in
	// place, with no new RedkeyConfig) and apply the new password to all
	// nodes via CONFIG SET before health-checking, so the running nodes accept
	// the rotated credentials instead of failing with WRONGPASS.
	if err := cr.reconcileAuthRotation(ctx, config, password); err != nil {
		return reconcileAfterInterval, fmt.Errorf("reconciling auth rotation: %w", err)
	}

	// Run health reconciliation
	cr.healthReconciler.SetEphemeral(config.Spec.Ephemeral)
	schedule, err := cr.healthReconciler.Reconcile(ctx, nodes, password,
		int(config.Spec.Primaries), int(config.Spec.ReplicasPerPrimary))
	// Surface the live data-plane health of the applied cluster via status conditions and an
	// informational substatus, even when remediation returned an error — the report still reflects
	// what was observed. This does NOT change Status/ConfigPhase: a cluster stays Ready while the
	// health-reconciler heals or rebalances.
	if report := cr.healthReconciler.LastReport(); report != nil {
		cr.applyHealthStatus(ctx, config, report)
	}
	if err != nil {
		return schedule, err
	}

	// If the config is still InProgress and the cluster is healthy, check for remaining changes.
	// This handles the post-scaling scenario: after topology is adjusted, non-topology changes
	// (K8s objects, Redis config) may still need to be applied.
	if config.Status.ConfigPhase == redisv1.ConfigPhaseInProgress && schedule == reconcileAfterInterval {
		if previousConfig != nil {
			report := DetectChanges(previousConfig.Spec, config.Spec)
			// Only consider non-topology changes (topology is already applied at this point).
			if report.HasKubernetesChanges || report.HasRedisConfigChanges {
				newStatus := DetermineStatusTransition(ChangeReport{
					HasKubernetesChanges:  report.HasKubernetesChanges,
					HasRedisConfigChanges: report.HasRedisConfigChanges,
				})
				if newStatus != "" {
					cr.logger.Info("Post-scaling: remaining non-topology changes detected, transitioning",
						"config", config.Name,
						"newStatus", newStatus,
						"hasKubernetesChanges", report.HasKubernetesChanges,
						"hasRedisConfigChanges", report.HasRedisConfigChanges,
					)
					if err := cr.updateClusterStatus(ctx, config, newStatus); err != nil {
						return reconcileAfterInterval, err
					}
					return reconcileImmediately, nil
				}
			}
		}

		// No remaining cluster changes, mark as Applied.
		if err := cr.setConfigPhaseApplied(ctx, config); err != nil {
			return reconcileAfterInterval, err
		}
		cr.logger.Info("Config change verified healthy, marked as Applied", "name", config.Name)
	}

	return schedule, nil
}

// --- Node operations ---

func (cr *ClusterReconciler) initNodes(ctx context.Context, config *redisv1.RedkeyConfig, password string) ([]*redis.Node, error) {
	totalNodes := int(config.Spec.Primaries + (config.Spec.Primaries * config.Spec.ReplicasPerPrimary))
	clusterCfg := cr.runtimeConfig.ClusterConfig()
	maxRetries := clusterCfg.ConnectionMaxRetries
	backoff := time.Duration(clusterCfg.ConnectionBackOffSeconds) * time.Second

	// Use pod IPs for addressing — works both in-cluster and out-of-cluster (for debugging purposes).
	podAddrs, err := kubernetes.GetPodAddresses(ctx, cr.client, cr.clusterName, cr.namespace)
	if err != nil {
		return nil, fmt.Errorf("getting pod addresses: %w", err)
	}

	var nodes []*redis.Node
	for i := range totalNodes {
		name := fmt.Sprintf("%s-%d", cr.clusterName, i)
		addr, ok := podAddrs[name]
		if !ok {
			closeNodes(nodes)
			return nil, fmt.Errorf("pod %s not found or has no IP", name)
		}
		node := redis.NewNode(name, addr, password)

		if err := node.Init(ctx, maxRetries, backoff); err != nil {
			cr.logger.Error("Failed to initialize node", "node", name, "addr", addr, "error", err)
			closeNodes(nodes)
			return nil, err
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

func (cr *ClusterReconciler) meetNodes(ctx context.Context, nodes []*redis.Node) error {
	// Each node meets every other node.
	for i, node := range nodes {
		for j, other := range nodes {
			if i == j {
				continue
			}
			if err := node.Client().ClusterMeet(ctx, other.IP, redis.DefaultPort); err != nil {
				return fmt.Errorf("node %s meeting %s: %w", node.Name, other.Name, err)
			}
		}
	}
	cr.logger.Info("All nodes introduced to each other", "count", len(nodes))
	return nil
}

// checkGossipConvergence verifies that all nodes see every other node in their
// CLUSTER NODES output. This ensures gossip has fully propagated after CLUSTER MEET
// before attempting operations like CLUSTER REPLICATE that require node ID visibility.
func (cr *ClusterReconciler) checkGossipConvergence(ctx context.Context, nodes []*redis.Node) (bool, error) {
	expectedIDs := make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		expectedIDs[n.ID] = struct{}{}
	}

	for _, node := range nodes {
		clusterNodes, err := node.Client().GetClusterNodes(ctx)
		if err != nil {
			return false, fmt.Errorf("getting cluster nodes from %s: %w", node.Name, err)
		}

		visibleIDs := make(map[string]struct{}, len(clusterNodes))
		for _, cn := range clusterNodes {
			visibleIDs[cn.ID] = struct{}{}
		}

		for id := range expectedIDs {
			if _, ok := visibleIDs[id]; !ok {
				cr.logger.Info("Node does not yet see all peers",
					"node", node.Name, "missingNodeID", id)
				return false, nil
			}
		}
	}

	cr.logger.Info("Gossip fully converged, all nodes visible to each other")
	return true, nil
}

func (cr *ClusterReconciler) assignSlots(ctx context.Context, primaries []*redis.Node) error {
	numPrimaries := len(primaries)
	slotsPerNode := clusterTotalSlots / numPrimaries
	remainder := clusterTotalSlots % numPrimaries

	slotStart := 0
	for i, primary := range primaries {
		count := slotsPerNode
		if i < remainder {
			count++
		}
		slots := make([]int, count)
		for s := range count {
			slots[s] = slotStart + s
		}
		slotStart += count

		// Check if the node already has slots assigned
		if err := primary.RefreshInfo(ctx); err != nil {
			return fmt.Errorf("refreshing %s info: %w", primary.Name, err)
		}
		if primary.Slots != "" {
			cr.logger.Info("Node already has slots, skipping assignment",
				"node", primary.Name, "slots", primary.Slots)
			continue
		}

		if err := primary.Client().ClusterAddSlots(ctx, slots...); err != nil {
			return fmt.Errorf("assigning slots to %s: %w", primary.Name, err)
		}
		cr.logger.Info("Assigned slots to node",
			"node", primary.Name, "start", slots[0], "end", slots[len(slots)-1])
	}
	return nil
}

func (cr *ClusterReconciler) setReplicas(ctx context.Context, primaries, replicas []*redis.Node, replicasPerPrimary int32) error {
	for i, replica := range replicas {
		primaryIdx := i / int(replicasPerPrimary)
		if primaryIdx >= len(primaries) {
			primaryIdx = len(primaries) - 1
		}
		primary := primaries[primaryIdx]

		// Check if already replicating
		if err := replica.RefreshInfo(ctx); err != nil {
			return fmt.Errorf("refreshing %s info: %w", replica.Name, err)
		}
		if replica.IsReplica() && replica.PrimaryID == primary.ID {
			cr.logger.Info("Node already replicating correct primary, skipping",
				"replica", replica.Name, "primary", primary.Name)
			continue
		}

		if err := replica.Client().ClusterReplicate(ctx, primary.ID); err != nil {
			return fmt.Errorf("setting %s as replica of %s: %w", replica.Name, primary.Name, err)
		}
		cr.logger.Info("Set replica",
			"replica", replica.Name, "primary", primary.Name)
	}
	return nil
}

// clusterAlreadyFormed reports whether the given nodes already make up a fully formed cluster that
// matches the desired topology: cluster state ok, all slots covered, exactly `Primaries`
// slot-serving primaries, and exactly `Primaries * ReplicasPerPrimary` attached replicas. This is
// true when nodes are restored from persisted PVCs (e.g. scaling a storage cluster back up from
// zero), letting formCluster skip from-scratch slot assignment that would otherwise conflict with
// the persisted ownership.
//
// It deliberately requires the REPLICA topology to be complete too: a half-formed cluster whose
// slots are assigned but whose replicas have not been attached yet (e.g. an initial formation that
// assigned slots on one pass but deferred setReplicas until gossip converged) must NOT
// short-circuit, otherwise its replicas would never be attached.
func (cr *ClusterReconciler) clusterAlreadyFormed(ctx context.Context, config *redisv1.RedkeyConfig, nodes []*redis.Node) (bool, error) {
	info, err := nodes[0].Client().GetClusterInfo(ctx)
	if err != nil {
		return false, err
	}
	if info.State != "ok" || info.SlotsOK != clusterTotalSlots {
		return false, nil
	}

	if err := cr.refreshRoles(ctx, nodes); err != nil {
		return false, err
	}
	primariesWithSlots := 0
	replicas := 0
	for _, n := range nodes {
		switch {
		case n.IsReplica():
			replicas++
		case n.Slots != "":
			primariesWithSlots++
		}
	}
	expectedPrimaries := int(config.Spec.Primaries)
	expectedReplicas := int(config.Spec.Primaries) * int(config.Spec.ReplicasPerPrimary)
	return primariesWithSlots == expectedPrimaries && replicas == expectedReplicas, nil
}

func (cr *ClusterReconciler) verifyCluster(ctx context.Context, seedNode *redis.Node) (bool, error) {
	info, err := seedNode.Client().GetClusterInfo(ctx)
	if err != nil {
		return false, err
	}

	if info.State != "ok" {
		cr.logger.Info("Cluster state not ok", "state", info.State)
		return false, nil
	}
	if info.SlotsOK != clusterTotalSlots {
		cr.logger.Info("Not all slots covered",
			"slotsOK", info.SlotsOK, "expected", clusterTotalSlots)
		return false, nil
	}
	return true, nil
}

// --- Status updates ---

func (cr *ClusterReconciler) updateClusterStatus(ctx context.Context, config *redisv1.RedkeyConfig, status string) error {
	config.Status.Status = status
	if err := cr.client.Status().Update(ctx, config); err != nil {
		return fmt.Errorf("updating cluster status to %s: %w", status, err)
	}
	return nil
}

// updateSubstatus sets the informational substatus field to indicate the current phase
// within a scaling operation. This is purely observational — it does not affect control flow.
func (cr *ClusterReconciler) updateSubstatus(ctx context.Context, config *redisv1.RedkeyConfig, substatus string) {
	if config.Status.Substatus.Status == substatus {
		return
	}
	config.Status.Substatus.Status = substatus
	if err := cr.client.Status().Update(ctx, config); err != nil {
		cr.logger.Warn("Failed to update substatus (non-critical)", "substatus", substatus, "error", err)
	}
}

// buildHealthNodes builds the health.Node list for the cluster from the current pod addresses,
// shared by the periodic health check and the post-operation condition refresh.
func (cr *ClusterReconciler) buildHealthNodes(ctx context.Context, config *redisv1.RedkeyConfig) ([]health.Node, error) {
	podAddrs, err := kubernetes.GetPodAddresses(ctx, cr.client, cr.clusterName, cr.namespace)
	if err != nil {
		return nil, err
	}
	total := int(config.Spec.Primaries + (config.Spec.Primaries * config.Spec.ReplicasPerPrimary))
	nodes := make([]health.Node, 0, total)
	for i := range total {
		name := fmt.Sprintf("%s-%d", cr.clusterName, i)
		if addr, ok := podAddrs[name]; ok {
			nodes = append(nodes, health.Node{Name: name, Addr: addr})
		} else {
			cr.logger.Warn("Pod not found for health check, skipping", "pod", name)
		}
	}
	return nodes, nil
}

// refreshHealthConditions runs a health check (without remediation) and records the resulting
// conditions on the config, so that reaching Ready/Applied immediately reflects the cluster's health
// instead of leaving the conditions Unknown until the next handleReady cycle. It is best-effort and
// non-critical: on any error the next periodic health check populates them.
func (cr *ClusterReconciler) refreshHealthConditions(ctx context.Context, config *redisv1.RedkeyConfig, password string) {
	nodes, err := cr.buildHealthNodes(ctx, config)
	if err != nil || len(nodes) == 0 {
		cr.logger.Warn("Skipping post-operation health condition refresh (no nodes)", "error", err)
		return
	}
	cr.healthReconciler.SetEphemeral(config.Spec.Ephemeral)
	if report := cr.healthReconciler.Check(ctx, nodes, password,
		int(config.Spec.Primaries), int(config.Spec.ReplicasPerPrimary)); report != nil {
		cr.applyHealthStatus(ctx, config, report)
	}
}

// applyHealthStatus records the live data-plane health of an applied cluster on the config status:
// the Healthy rollup plus one condition per health axis, and an informational Substatus of
// Remediating while the cluster is not yet fully healthy. It never changes Status/ConfigPhase — a
// cluster stays Ready while the health-reconciler heals or rebalances. Failures are non-critical:
// the next reconciliation cycle refreshes the state.
func (cr *ClusterReconciler) applyHealthStatus(ctx context.Context, config *redisv1.RedkeyConfig, report *health.Report) {
	setHealthCondition(config, redisv1.ConditionHealthy, report.Healthy, "AllChecksPassed", "SomeChecksFailed")
	setHealthCondition(config, redisv1.ConditionMembershipHealthy, report.MembershipOK, "MembershipConsistent", "MembershipInconsistent")
	setHealthCondition(config, redisv1.ConditionSlotsCovered, report.SlotsCoveredOK, "AllSlotsAssigned", "SlotsUncovered")
	setHealthCondition(config, redisv1.ConditionSlotsBalanced, report.BalancedOK, "SlotsBalanced", "SlotsUnbalanced")
	setHealthCondition(config, redisv1.ConditionReplicasBalanced, report.ReplicaSpreadOK, "ReplicasBalanced", "ReplicasUnbalanced")
	setHealthCondition(config, redisv1.ConditionClusterCheckPassing, report.ClusterCheckOK, "ClusterCheckPassed", "ClusterCheckFailed")

	if report.Healthy {
		config.Status.Substatus.Status = ""
	} else {
		config.Status.Substatus.Status = redisv1.SubstatusRemediating
	}

	if err := cr.client.Status().Update(ctx, config); err != nil {
		cr.logger.Warn("Failed to update health status conditions (non-critical)", "error", err)
	}
}

// setHealthCondition upserts a boolean health condition on the config, mapping ok=true to
// ConditionTrue with trueReason and ok=false to ConditionFalse with falseReason.
func setHealthCondition(config *redisv1.RedkeyConfig, condType string, ok bool, trueReason, falseReason string) {
	status := metav1.ConditionFalse
	reason := falseReason
	if ok {
		status = metav1.ConditionTrue
		reason = trueReason
	}
	meta.SetStatusCondition(&config.Status.Conditions, metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		ObservedGeneration: config.Generation,
	})
}

func (cr *ClusterReconciler) setConfigPhaseApplied(ctx context.Context, config *redisv1.RedkeyConfig) error {
	config.Status.ConfigPhase = redisv1.ConfigPhaseApplied
	if err := cr.client.Status().Update(ctx, config); err != nil {
		return fmt.Errorf("setting ConfigPhase to Applied: %w", err)
	}
	return nil
}

func (cr *ClusterReconciler) updateNodeStatus(ctx context.Context, config *redisv1.RedkeyConfig, nodes []*redis.Node) error {
	nodeStatus := make(map[string]*redisv1.RedisNode)
	for _, node := range nodes {
		role := "primary"
		if node.IsReplica() {
			role = "replica"
		}
		nodeStatus[node.Name] = &redisv1.RedisNode{
			Role: role,
			IP:   node.IP,
		}
	}
	config.Status.Nodes = nodeStatus
	return cr.client.Status().Update(ctx, config)
}

// --- Helpers ---

func (cr *ClusterReconciler) getOwner(ctx context.Context) (*redisv1.Redkey, error) {
	cluster := &redisv1.Redkey{}
	if err := cr.client.Get(ctx, types.NamespacedName{Name: cr.clusterName, Namespace: cr.namespace}, cluster); err != nil {
		return nil, err
	}
	return cluster, nil
}

func (cr *ClusterReconciler) getPassword(ctx context.Context, config *redisv1.RedkeyConfig) string {
	password, err := kubernetes.GetRedisPassword(ctx, cr.client, config.Spec.Auth.SecretName, cr.namespace)
	if err != nil {
		cr.logger.Error("Failed to read auth secret, proceeding without password", "error", err)
		return ""
	}
	return password
}

// healTopology reconciles cluster membership against the live Kubernetes pods before a
// mutating operation (scaling or upgrade) proceeds. When Kubernetes recreates one or more
// pods — for example, moving them between nodes during the operation — each replacement
// comes back with a new IP (and, for ephemeral clusters, a new node ID) while the old
// entry lingers in the gossip table, often still owning slots. Left unattended this
// derails the operation: redis-cli rebalances/reshards "across" the phantom masters and
// never converges.
//
// healTopology forgets those lingering entries (keyed on the live pod IPs as the source
// of truth) and re-covers any slots they orphaned with cluster fix. When meetMissing is
// true it also reintegrates recreated members via CLUSTER MEET. A stale node that still
// owns slots is only forgotten (and its slots reassigned) on EPHEMERAL clusters, where the
// recreated pod has lost its data; on persistent clusters such a node keeps its identity
// and reclaims its slots on rejoin, so it is left in place to avoid stranding data (see
// HealMembership). It returns changed=true when the topology was modified, so the caller
// should requeue and re-evaluate from a clean state rather than proceeding on stale
// assumptions. It is a fast no-op on a healthy topology.
//
// meetMissing must be false for scale-up, whose empty-masters rebalance would otherwise
// hand slots to a freshly met, not-yet-classified node destined to become a replica;
// scale-up meets its own new nodes with explicit roles.
func (cr *ClusterReconciler) healTopology(ctx context.Context, config *redisv1.RedkeyConfig, meetMissing bool) (bool, error) {
	podAddrs, err := kubernetes.GetPodAddresses(ctx, cr.client, cr.clusterName, cr.namespace)
	if err != nil {
		return false, fmt.Errorf("getting pod addresses for topology healing: %w", err)
	}
	if len(podAddrs) == 0 {
		return false, nil
	}
	nodes := make([]health.Node, 0, len(podAddrs))
	for name, addr := range podAddrs {
		nodes = append(nodes, health.Node{Name: name, Addr: addr})
	}
	password := cr.getPassword(ctx, config)
	return cr.healthReconciler.HealMembership(ctx, nodes, password, meetMissing, config.Spec.Ephemeral)
}

// ensureReplicaTopology guarantees the live cluster matches the desired primary/replica
// distribution before an operation (scaling or upgrade) declares itself complete. Pod
// recreation mid-operation can leave a pod integrated with the wrong role — most commonly
// a replica that rejoined as an empty primary, leaving the wrong number of primaries or a
// primary with too many/few replicas — which the slot/state checks in verifyCluster do not
// detect.
//
// It first heals membership with meetMissing=true (safe here because the operation's slot
// movement is already finished, so no empty-masters rebalance follows that could grab slots
// from a freshly met node) to reintegrate any recreated member, then validates and, if
// needed, remediates the replica spread. It returns ok=true only once the number of
// primaries and the replicas-per-primary both match the spec; callers must requeue while ok
// is false so the cluster never reaches Ready with the wrong topology.
func (cr *ClusterReconciler) ensureReplicaTopology(ctx context.Context, config *redisv1.RedkeyConfig) (bool, error) {
	// A zero-primary cluster has no topology to validate.
	if config.Spec.Primaries == 0 {
		return true, nil
	}

	podAddrs, err := kubernetes.GetPodAddresses(ctx, cr.client, cr.clusterName, cr.namespace)
	if err != nil {
		return false, fmt.Errorf("getting pod addresses for replica topology check: %w", err)
	}
	if len(podAddrs) == 0 {
		return true, nil
	}
	nodes := make([]health.Node, 0, len(podAddrs))
	for name, addr := range podAddrs {
		nodes = append(nodes, health.Node{Name: name, Addr: addr})
	}
	password := cr.getPassword(ctx, config)

	// Reintegrate any recreated member first, then re-check on a clean topology.
	changed, err := cr.healthReconciler.HealMembership(ctx, nodes, password, true, config.Spec.Ephemeral)
	if err != nil {
		return false, fmt.Errorf("healing membership before replica topology check: %w", err)
	}
	if changed {
		return false, nil
	}

	ok, _, err := cr.healthReconciler.RemediateReplicaTopology(ctx, nodes, password,
		int(config.Spec.Primaries), int(config.Spec.ReplicasPerPrimary))
	if err != nil {
		return false, err
	}
	return ok, nil
}

// applyAuthToAllNodes applies the target configuration's auth settings to ALL
// running Redis nodes via CONFIG SET requirepass / CONFIG SET masterauth,
// then updates the ConfigMap so future pod restarts pick up the correct auth.
//
// This must be called BEFORE any cluster operation (rolling upgrade, scaling)
// that could create or recycle pods, ensuring all nodes share the same auth
// throughout the process and avoiding mixed-auth communication failures.
func (cr *ClusterReconciler) applyAuthToAllNodes(ctx context.Context, config, previousConfig *redisv1.RedkeyConfig) error {
	newPassword := cr.getPassword(ctx, config)

	// Use the previous config's credentials to connect to existing nodes.
	// If the previous config had no auth (empty secret), this returns ""
	// which means "connect without password" — correct for that case.
	oldPassword := cr.getPassword(ctx, previousConfig)

	if err := cr.applyAuthCredentials(ctx, config, oldPassword, newPassword); err != nil {
		return err
	}

	cr.logger.Info("Auth configuration applied to all nodes and ConfigMap updated",
		"secretName", config.Spec.Auth.SecretName)
	return nil
}

// reconcileAuthRotation detects and applies an out-of-band password rotation.
//
// A rotation happens when the auth Secret referenced by the cluster is edited in
// place: the SecretName does not change, so the operator generates no new
// RedkeyConfig and handleConfigChange/applyAuthToAllNodes never runs. The
// running Redis nodes therefore keep the old requirepass while the Secret (and
// every component that reads it) already holds the new password, which surfaces
// as WRONGPASS errors during health checks.
//
// The cluster's ConfigMap (redis.conf requirepass) is the source of truth for the
// password the running nodes actually use: it is what they were started with and
// only Robin changes it. Comparing it against the Secret detects a rotation
// robustly — without any in-memory state, so it survives Robin restarts and has
// no start-up race. When they differ, Robin connects to every node with the
// ConfigMap password and applies the Secret's password via CONFIG SET, then
// updates the ConfigMap so the two converge.
//
// currentPassword is the password just read from the Secret.
func (cr *ClusterReconciler) reconcileAuthRotation(ctx context.Context, config *redisv1.RedkeyConfig, currentPassword string) error {
	nodePassword, found, err := kubernetes.GetConfigMapPassword(ctx, cr.client, cr.clusterName, cr.namespace)
	if err != nil {
		return fmt.Errorf("reading current node password from ConfigMap: %w", err)
	}

	// No ConfigMap yet (cluster not fully provisioned) or already in sync: nothing to do.
	if !found || nodePassword == currentPassword {
		return nil
	}

	cr.logger.Info("Password rotation detected (auth Secret differs from ConfigMap), applying to all nodes",
		"secretName", config.Spec.Auth.SecretName)

	if err := cr.applyAuthCredentials(ctx, config, nodePassword, currentPassword); err != nil {
		return fmt.Errorf("applying rotated auth: %w", err)
	}

	cr.logger.Info("Password rotation applied to all nodes",
		"secretName", config.Spec.Auth.SecretName)
	return nil
}

// applyAuthCredentials applies newPassword to ALL running Redis nodes via
// CONFIG SET requirepass / CONFIG SET masterauth, connecting to each node with
// oldPassword, then updates the ConfigMap so future pod restarts pick up the
// correct auth and refreshes the runtime auth secret so other components
// (metrics, health) use it.
func (cr *ClusterReconciler) applyAuthCredentials(ctx context.Context, config *redisv1.RedkeyConfig, oldPassword, newPassword string) error {
	podAddrs, err := kubernetes.GetPodAddresses(ctx, cr.client, cr.clusterName, cr.namespace)
	if err != nil {
		return fmt.Errorf("getting pod addresses for auth config: %w", err)
	}

	for name, addr := range podAddrs {
		rc := redis.NewClient(addr, oldPassword)

		if err := rc.ConfigSet(ctx, "requirepass", newPassword); err != nil {
			_ = rc.Close()
			return fmt.Errorf("CONFIG SET requirepass on %s: %w", name, err)
		}
		if err := rc.ConfigSet(ctx, "masterauth", newPassword); err != nil {
			_ = rc.Close()
			return fmt.Errorf("CONFIG SET masterauth on %s: %w", name, err)
		}
		_ = rc.Close()

		cr.logger.Info("Applied auth configuration via CONFIG SET",
			"node", name, "hasAuth", newPassword != "")
	}

	// Update the ConfigMap so recycled/new pods start with the correct auth.
	// This also keeps the ConfigMap in sync with the Secret, which is what
	// reconcileAuthRotation compares against to detect future rotations.
	if err := kubernetes.UpdateConfigMap(ctx, cr.client, cr.clusterName, cr.namespace, config, newPassword); err != nil {
		return fmt.Errorf("updating ConfigMap with new auth: %w", err)
	}

	// Update the runtime auth secret so other components (metrics, health)
	// pick up the change immediately.
	cr.runtimeConfig.SetAuthSecret(config.Spec.Auth.SecretName)

	return nil
}

func closeNodes(nodes []*redis.Node) {
	for _, n := range nodes {
		_ = n.Close()
	}
}
