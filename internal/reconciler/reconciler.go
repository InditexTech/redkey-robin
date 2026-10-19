// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import (
	"context"
	"log/slog"
	"reflect"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	redisv1 "github.com/inditextech/redkey-operator/api/v1beta1"
	"github.com/inditextech/redkey-robin/internal/config"
)

const (
	// ClusterLabel is the label key used to filter RedkeyConfig CRs by cluster name.
	ClusterLabel = "redkey.inditex.dev/cluster"
)

type reconcileSchedule uint8

const (
	reconcileAfterInterval reconcileSchedule = iota
	reconcileAfterWaitInterval
	reconcileImmediately
)

// Reconciler implements the polling reconciliation loop for a single Redkey.
// It periodically lists RedkeyConfig CRs and processes them sequentially.
type Reconciler struct {
	client            client.Client
	clusterName       string
	namespace         string
	interval          time.Duration
	intervalOnError   time.Duration
	intervalOnWait    time.Duration
	runtimeConfig     *config.RuntimeConfig
	clusterReconciler *ClusterReconciler
	logger            *slog.Logger
}

// metricsRuntimeSnapshot captures the relevant parts of the runtime configuration related to metrics, for comparison and logging purposes.
type metricsRuntimeSnapshot struct {
	CollectionInterval time.Duration
	RedisInfoKeys      []string
	MetricsLabels      map[string]string
}

// clusterRuntimeSnapshot captures the relevant parts of the runtime configuration related to cluster connection and topology, for comparison and logging purposes.
type clusterRuntimeSnapshot struct {
	Connection config.ClusterConfig
	Topology   config.Topology
	AuthSecret string
}

// NewReconciler creates a new Reconciler.
func NewReconciler(c client.Client, clusterName, namespace string, runtimeConfig *config.RuntimeConfig) *Reconciler {
	if runtimeConfig == nil {
		runtimeConfig = config.NewRuntimeConfig()
	}

	return &Reconciler{
		client:            c,
		clusterName:       clusterName,
		namespace:         namespace,
		interval:          runtimeConfig.ReconcilerInterval(),
		intervalOnError:   runtimeConfig.ReconcilerIntervalOnError(),
		intervalOnWait:    runtimeConfig.ReconcilerIntervalOnWait(),
		runtimeConfig:     runtimeConfig,
		clusterReconciler: NewClusterReconciler(c, clusterName, namespace, runtimeConfig),
		logger:            slog.Default().With("component", "reconciler", "cluster", clusterName),
	}
}

// Start begins the polling reconciliation loop. It blocks until the context is cancelled.
func (r *Reconciler) Start(ctx context.Context) error {
	r.logger.Info(
		"Starting reconciliation loop",
		"interval", r.interval,
		"intervalOnError", r.intervalOnError,
		"intervalOnWait", r.intervalOnWait,
	)

	// Ensure cached Redis clients (e.g. health-check connections) are closed on shutdown
	// to avoid leaking connections and file descriptors.
	defer r.clusterReconciler.Close()

	// Run an initial reconciliation immediately
	r.logger.Info("Starting reconciliation cycle")
	schedule, onError := r.reconcile(ctx)
	r.logger.Info("Finished reconciliation cycle", "nextSchedule", schedule, "onError", onError)

	// Enter the reconciliation loop, scheduling the next run based on the result of the previous reconciliation.
	for {
		select {
		case <-ctx.Done():
			r.logger.Info("Context cancelled, stopping reconciler")
			return nil
		case <-time.After(r.nextWaitDuration(schedule, onError)):
			r.logger.Info("Starting reconciliation cycle")
			schedule, onError = r.reconcile(ctx)
			r.logger.Info("Finished reconciliation cycle", "nextSchedule", schedule, "onError", onError)
		}
	}
}

// Return the corresponding wait duration depending on the scheduling required and if we had any error.
func (r *Reconciler) nextWaitDuration(schedule reconcileSchedule, onError bool) time.Duration {
	if onError {
		return r.intervalOnError
	}

	switch schedule {
	case reconcileImmediately:
		return 0
	case reconcileAfterWaitInterval:
		return r.intervalOnWait
	default:
		return r.interval
	}
}

// reconcile performs a single reconciliation cycle.
// schedule controls whether the next loop should run immediately or after the
// idle/wait configured interval.
// onError applies the error interval regardless of the returned schedule.
func (r *Reconciler) reconcile(ctx context.Context) (schedule reconcileSchedule, onError bool) {
	previousConfig, targetConfig, forceScaleToZero, err := r.selectConfig(ctx)
	if err != nil {
		r.logger.Error("Failed to select RedkeyConfig", "error", err)
		return reconcileAfterInterval, true
	}
	if targetConfig == nil {
		r.logger.Info("No actionable RedkeyConfig found")
		return reconcileAfterInterval, false
	}

	// Remember the phase the target config had when this cycle started so we can
	// detect whether it transitioned to Applied during this cycle.
	initialConfigPhase := targetConfig.Status.ConfigPhase

	// Log information about the target and previous configurations.
	r.logger.Info("Selected configuration to apply",
		"name", targetConfig.Name,
		"sequence", targetConfig.Spec.Sequence,
		"configPhase", targetConfig.Status.ConfigPhase,
	)
	if previousConfig != nil {
		r.logger.Info("Previous configuration detected",
			"name", previousConfig.Name,
			"sequence", previousConfig.Spec.Sequence,
			"configPhase", previousConfig.Status.ConfigPhase,
		)
	} else {
		r.logger.Info("No previous configuration exists")
	}

	// Validate the phase of the target configuration before processing.
	// Required before handling Robin and monitoring configurations.
	if !validConfigPhase(targetConfig.Status.ConfigPhase, true) {
		r.logger.Error("Configuration with unknown or not valid phase detected. Skipping processing of this configuration and waiting for next cycle.",
			"name", targetConfig.Name,
			"sequence", targetConfig.Spec.Sequence,
			"configPhase", targetConfig.Status.ConfigPhase,
		)
		return reconcileAfterInterval, true
	}

	// From here, we start processing Redkey Cluster configuration and health checks.
	// We decide what to do based on the phase of the target configuration.
	var scheduleRequired reconcileSchedule
	var onReconcilingError bool
	switch targetConfig.Status.ConfigPhase {
	case redisv1.ConfigPhasePending:
		if forceScaleToZero {
			// Reached through the scale-to-zero lookahead: go straight to ScalingToZero.
			if err := r.startScaleToZero(ctx, targetConfig, previousConfig); err != nil {
				r.logger.Error("Failed to start scale-to-zero configuration",
					"name", targetConfig.Name, "error", err)
				scheduleRequired = reconcileAfterInterval
				onReconcilingError = true
				break
			}
			schedule, err := r.clusterReconciler.ReconcileCluster(ctx, targetConfig, previousConfig)
			if err != nil {
				r.logger.Error("Cluster reconciliation error", "error", err)
				scheduleRequired = schedule
				onReconcilingError = true
				break
			}
			scheduleRequired = schedule
			onReconcilingError = false
			break
		}

		// Start applying a new config.
		if err := r.setConfigPhaseInProgress(ctx, targetConfig); err != nil {
			r.logger.Error("Failed to set ConfigPhase to InProgress",
				"name", targetConfig.Name, "error", err)
			// We'll retry on the next cycle.
			scheduleRequired = reconcileAfterInterval
			onReconcilingError = true
			break
		}
		r.logger.Info("Starting configuration",
			"name", targetConfig.Name,
			"sequence", targetConfig.Spec.Sequence,
		)

		// Copy previousConfig cluster status to targetConfig so that we have the latest status available when applying Robin configuration.
		if err := r.copyPreviousClusterStatus(ctx, targetConfig, previousConfig); err != nil {
			r.logger.Error("Failed to persist copied status from previous configuration",
				"name", targetConfig.Name,
				"error", err,
			)
			scheduleRequired = reconcileAfterInterval
			onReconcilingError = true
			break
		}

		// For an existing cluster with a new config, detect changes and determine
		// the appropriate status transition before entering the state machine.
		// This must happen after copyPreviousClusterStatus (which sets Status to Ready)
		// because ReconcileCluster dispatches on Status.Status.
		if previousConfig != nil {
			// Standalone deployments use a dedicated config-change handler that
			// never issues CLUSTER commands. It fully drives the transition (it
			// sets the next status and ensures objects when needed), so the next
			// reconcile cycle resumes via ReconcileCluster.
			if targetConfig.Spec.IsStandalone() {
				schedule, err := r.clusterReconciler.handleStandaloneConfigChange(ctx, targetConfig, previousConfig)
				if err != nil {
					r.logger.Error("Standalone config change error", "error", err)
					scheduleRequired = schedule
					onReconcilingError = true
					break
				}
				scheduleRequired = schedule
				onReconcilingError = false
				break
			}

			schedule, err := r.clusterReconciler.handleConfigChange(ctx, targetConfig, previousConfig)
			if err != nil {
				r.logger.Error("Config change detection error", "error", err)
				scheduleRequired = schedule
				onReconcilingError = true
				break
			}
			// If handleConfigChange marked as Applied (no cluster op needed), we're done.
			if targetConfig.Status.ConfigPhase == redisv1.ConfigPhaseApplied {
				scheduleRequired = schedule
				onReconcilingError = false
				break
			}
			// Status was updated (ScalingUp/Down/Upgrading), fall through to ReconcileCluster.
		}

		schedule, err := r.clusterReconciler.ReconcileCluster(ctx, targetConfig, previousConfig)
		if err != nil {
			r.logger.Error("Cluster reconciliation error", "error", err)
			scheduleRequired = schedule
			onReconcilingError = true
			break
		}
		scheduleRequired = schedule
		onReconcilingError = false
	case redisv1.ConfigPhaseInProgress:
		// Resume an already in progress config.
		r.logger.Info("Resuming in-progress configuration",
			"name", targetConfig.Name,
			"sequence", targetConfig.Spec.Sequence,
		)
		schedule, err := r.clusterReconciler.ReconcileCluster(ctx, targetConfig, previousConfig)
		if err != nil {
			r.logger.Error("Cluster reconciliation error", "error", err)
			scheduleRequired = schedule
			onReconcilingError = true
			break
		}
		scheduleRequired = schedule
		onReconcilingError = false
	case redisv1.ConfigPhaseApplied:
		// No new config to apply, do a full check of the Redkey Cluster.
		r.logger.Info("Configuration already applied, performing full cluster check")
		schedule, err := r.clusterReconciler.ReconcileCluster(ctx, targetConfig, previousConfig)
		if err != nil {
			r.logger.Error("Cluster health check error", "error", err)
			scheduleRequired = schedule
			onReconcilingError = true
			break
		}
		scheduleRequired = schedule
		onReconcilingError = false
	default:
		// This should never happen due to the earlier validation, but we check again just in case.
		r.logger.Error("Configuration with unknown phase detected",
			"name", targetConfig.Name,
			"sequence", targetConfig.Spec.Sequence,
			"configPhase", targetConfig.Status.ConfigPhase,
		)
		scheduleRequired = reconcileAfterInterval
		onReconcilingError = true
	}

	// Apply Robin configuration from the target or actually applied config.
	// This is the right place to do it, after we've operated the cluster. The config phase could have been
	// updated to Applied, so we can apply the new Robin configuration.
	if !onReconcilingError {
		r.applyRobinConfig(targetConfig, previousConfig)
	}

	// If the target config was just applied during this cycle and there are still
	// other configurations waiting to be processed (Pending or not yet initialised),
	// reconcile immediately instead of waiting for the next interval, so the next
	// configuration is picked up without delay.
	if !onReconcilingError &&
		initialConfigPhase != redisv1.ConfigPhaseApplied &&
		targetConfig.Status.ConfigPhase == redisv1.ConfigPhaseApplied &&
		r.hasMoreActionableConfigs(ctx, targetConfig) {
		r.logger.Info("Configuration applied and more configurations are pending, scheduling immediate reconciliation",
			"name", targetConfig.Name,
			"sequence", targetConfig.Spec.Sequence,
		)
		scheduleRequired = reconcileImmediately
	}

	return scheduleRequired, onReconcilingError
}

// hasMoreActionableConfigs reports whether any RedkeyConfig other than the
// just-applied one still needs processing (i.e. it has not reached a terminal
// phase of Applied or Superseded). It is used to decide whether to reconcile
// immediately after a configuration is applied instead of waiting a full interval.
func (r *Reconciler) hasMoreActionableConfigs(ctx context.Context, applied *redisv1.RedkeyConfig) bool {
	configs, err := r.listConfigs(ctx, false)
	if err != nil {
		r.logger.Warn("Failed to list configs while checking for pending configurations", "error", err)
		return false
	}
	for i := range configs {
		if applied != nil && configs[i].Name == applied.Name {
			continue
		}
		phase := configs[i].Status.ConfigPhase
		if phase != redisv1.ConfigPhaseApplied && phase != redisv1.ConfigPhaseSuperseded {
			return true
		}
	}
	return false
}

// startScaleToZero starts a scale-to-zero config reached through the
// scale-to-zero lookahead. It writes ConfigPhase=InProgress and
// Status=ScalingToZero in a single update, so a restart can never resume this
// config through handleNew/handleConfigChange, which would not delete the
// objects left by the superseded configs (e.g. when there is no previous Applied
// config, or the previous one already had zero primaries).
func (r *Reconciler) startScaleToZero(ctx context.Context, targetConfig, previousConfig *redisv1.RedkeyConfig) error {
	if previousConfig != nil {
		targetConfig.Status = *previousConfig.Status.DeepCopy()
	}
	if targetConfig.Status.Nodes == nil {
		targetConfig.Status.Nodes = map[string]*redisv1.RedisNode{}
	}
	targetConfig.Status.ConfigPhase = redisv1.ConfigPhaseInProgress
	targetConfig.Status.Status = redisv1.ClusterStatusScalingToZero
	targetConfig.Status.Substatus = redisv1.RedkeySubstatus{}
	if err := r.client.Status().Update(ctx, targetConfig); err != nil {
		return err
	}
	r.logger.Info("Starting scale-to-zero configuration",
		"name", targetConfig.Name,
		"sequence", targetConfig.Spec.Sequence,
	)
	return nil
}

// copyPreviousClusterStatus copies the previous Status into the target configuration, preserving the target ConfigPhase and persisting the result.
func (r *Reconciler) copyPreviousClusterStatus(ctx context.Context, targetConfig, previousConfig *redisv1.RedkeyConfig) error {
	if targetConfig == nil || previousConfig == nil {
		return nil
	}

	targetConfigPhase := targetConfig.Status.ConfigPhase
	targetConfig.Status = *previousConfig.Status.DeepCopy()
	targetConfig.Status.ConfigPhase = targetConfigPhase
	if err := r.client.Status().Update(ctx, targetConfig); err != nil {
		return err
	}
	r.logger.Debug("Copied status from previous configuration to target configuration",
		"targetName", targetConfig.Name,
		"previousName", previousConfig.Name,
		"statusPhase", targetConfig.Status.Status,
		"configPhase", targetConfig.Status.ConfigPhase,
	)
	return nil
}

// currentMetricsRuntimeSnapshot returns a snapshot of the current runtime configuration relevant to metrics collection, for comparison and logging purposes.
func (r *Reconciler) currentMetricsRuntimeSnapshot() metricsRuntimeSnapshot {
	return metricsRuntimeSnapshot{
		CollectionInterval: r.runtimeConfig.MetricsInterval(),
		RedisInfoKeys:      r.runtimeConfig.RedisInfoKeys(),
		MetricsLabels:      r.runtimeConfig.MetricsLabels(),
	}
}

// currentClusterRuntimeSnapshot returns a snapshot of the current runtime configuration relevant to cluster connection and topology, for comparison and logging purposes.
func (r *Reconciler) currentClusterRuntimeSnapshot() clusterRuntimeSnapshot {
	return clusterRuntimeSnapshot{
		Connection: r.runtimeConfig.ClusterConfig(),
		Topology:   r.runtimeConfig.AppliedTopology(),
		AuthSecret: r.runtimeConfig.AuthSecret(),
	}
}

// applyRobinConfig reads the RobinConfig from the effective configuration and
// updates the shared RuntimeConfig so that other components (metrics collector,
// reconciler interval) pick up the changes.
// It also stores the topology for node discovery by the metrics collector.
func (r *Reconciler) applyRobinConfig(target *redisv1.RedkeyConfig, previous *redisv1.RedkeyConfig) {
	if r.runtimeConfig == nil {
		return
	}

	// Determine which config holds the effective Robin settings:
	// - If the target is Applied, use it (it's the current active config).
	// - If the target is InProgress or Pending, use the previous (last Applied) if available,
	//   since the new config hasn't been fully applied yet. But we also update topology
	//   from the target so node discovery reflects the desired state.
	var effectiveConfig *redisv1.RedkeyConfig
	switch target.Status.ConfigPhase {
	case redisv1.ConfigPhaseApplied:
		effectiveConfig = target
	default:
		if previous != nil {
			effectiveConfig = previous
		} else {
			// No previous applied config — use target's RobinConfig even though it's not yet applied.
			effectiveConfig = target
		}
	}

	// Keep a snapshot of the current runtime configuration for comparison and logging purposes after we update it from the RobinConfig.
	oldMetricsConfig := r.currentMetricsRuntimeSnapshot()
	oldClusterConfig := r.currentClusterRuntimeSnapshot()

	// Update the runtime configuration from the effective RobinConfig.
	r.runtimeConfig.SetFromRobinConfig(effectiveConfig.Spec.RobinConfig)

	// Apply reconciler intervals so that they take effect immediately.
	newInterval := r.runtimeConfig.ReconcilerInterval()
	if newInterval != r.interval {
		r.logger.Info("Updating reconciler interval", "old", r.interval, "new", newInterval)
		r.interval = newInterval
	}
	newIntervalOnError := r.runtimeConfig.ReconcilerIntervalOnError()
	if newIntervalOnError != r.intervalOnError {
		r.logger.Info("Updating reconciler error interval", "old", r.intervalOnError, "new", newIntervalOnError)
		r.intervalOnError = newIntervalOnError
	}
	newIntervalOnWait := r.runtimeConfig.ReconcilerIntervalOnWait()
	if newIntervalOnWait != r.intervalOnWait {
		r.logger.Info("Updating reconciler wait interval", "old", r.intervalOnWait, "new", newIntervalOnWait)
		r.intervalOnWait = newIntervalOnWait
	}

	// Update topology for node discovery (always from the effective config).
	r.runtimeConfig.SetTopology(effectiveConfig.Spec.Primaries, effectiveConfig.Spec.ReplicasPerPrimary)

	// Track whether this is a standalone (single-node, non-clustered) deployment
	// so the metrics collector can skip cluster-level collection.
	r.runtimeConfig.SetStandalone(effectiveConfig.Spec.IsStandalone())

	// Update auth secret name so the metrics collector can read the password.
	r.runtimeConfig.SetAuthSecret(effectiveConfig.Spec.Auth.SecretName)

	// Log any changes in Metrics and Cluster configuration for observability.
	newMetricsConfig := r.currentMetricsRuntimeSnapshot()
	if !reflect.DeepEqual(oldMetricsConfig, newMetricsConfig) {
		r.logger.Info("Metrics configuration changed",
			"config", effectiveConfig.Name,
			"sequence", effectiveConfig.Spec.Sequence,
			"old", oldMetricsConfig,
			"new", newMetricsConfig,
		)
	}
	newClusterConfig := r.currentClusterRuntimeSnapshot()
	if !reflect.DeepEqual(oldClusterConfig, newClusterConfig) {
		r.logger.Info("Cluster configuration changed",
			"config", effectiveConfig.Name,
			"sequence", effectiveConfig.Spec.Sequence,
			"old", oldClusterConfig,
			"new", newClusterConfig,
		)
	}
}
