// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	redisv1 "github.com/inditextech/redkey-operator/api/v1beta1"
	robinconfig "github.com/inditextech/redkey-robin/internal/config"
	"github.com/inditextech/redkey-robin/internal/metrics"
	"github.com/inditextech/redkey-robin/internal/reconciler"
)

const (
	defaultReconcileInterval        = time.Duration(robinconfig.DefaultReconcilerIntervalSeconds) * time.Second
	defaultReconcileIntervalOnError = time.Duration(robinconfig.DefaultReconcilerIntervalOnErrorSeconds) * time.Second
	defaultReconcileIntervalOnWait  = time.Duration(robinconfig.DefaultReconcilerIntervalOnWaitSeconds) * time.Second
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(redisv1.AddToScheme(scheme))
}

func main() {
	var clusterName string
	var namespace string
	var metricsAddr string
	var logLevel string
	var enablePprof bool
	var reconcileInterval time.Duration
	var reconcileIntervalOnError = defaultReconcileIntervalOnError
	var reconcileIntervalOnWait = defaultReconcileIntervalOnWait

	flag.StringVar(&clusterName, "cluster-name", "", "Name of the Redkey this Robin instance manages (required)")
	flag.StringVar(&namespace, "namespace", "", "Namespace of the Redkey (required)")
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metrics endpoint binds to")
	flag.StringVar(&logLevel, "log-level", "info", "Log level: debug, info, warn, error")
	flag.BoolVar(&enablePprof, "enable-pprof", false,
		"Enable pprof profiling endpoints on the metrics server (do not use in production unless debugging)")
	flag.DurationVar(&reconcileInterval, "reconcile-interval", defaultReconcileInterval,
		"Polling interval for the reconciliation loop")
	flag.DurationVar(&reconcileIntervalOnError, "reconcile-interval-on-error", defaultReconcileIntervalOnError,
		"Polling interval for the reconciliation loop when an error occurs")
	flag.DurationVar(&reconcileIntervalOnWait, "reconcile-interval-on-wait", defaultReconcileIntervalOnWait,
		"Polling interval for the reconciliation loop while waiting for convergence")
	flag.Parse()

	var slogLevel slog.Level
	if err := slogLevel.UnmarshalText([]byte(logLevel)); err != nil {
		slog.Error("Invalid log level", "value", logLevel, "error", err)
		os.Exit(1)
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slogLevel}))
	slog.SetDefault(logger)

	if clusterName == "" {
		logger.Error("--cluster-name is required")
		os.Exit(1)
	}
	if namespace == "" {
		logger.Error("--namespace is required")
		os.Exit(1)
	}

	logger.Info("Starting Redkey Robin",
		"cluster", clusterName,
		"namespace", namespace,
		"metricsAddr", metricsAddr,
		"enablePprof", enablePprof,
		"reconcileInterval", reconcileInterval,
		"reconcileIntervalOnError", reconcileIntervalOnError,
		"reconcileIntervalOnWait", reconcileIntervalOnWait,
	)

	// Build Kubernetes client
	config, err := ctrl.GetConfig()
	if err != nil {
		logger.Error("Unable to get Kubernetes config", "error", err)
		os.Exit(1)
	}

	k8sClient, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		logger.Error("Unable to create Kubernetes client", "error", err)
		os.Exit(1)
	}

	// Set up signal handler for graceful shutdown
	ctx, cancel := signal.NotifyContext(
		ctrl.SetupSignalHandler(),
		syscall.SIGINT, syscall.SIGTERM,
	)
	defer cancel()

	// Error channel for critical failures
	errChan := make(chan error, 3)

	// Create shared runtime configuration with defaults from CLI flags.
	runtimeConfig := robinconfig.NewRuntimeConfigWithReconcilerIntervals(
		reconcileInterval,
		reconcileIntervalOnError,
		reconcileIntervalOnWait,
	)

	// Set bootstrap profiling state from CLI flag. This will be overridden
	// by the RedkeyConfig CRD profiling.enabled field once the
	// reconciler reads the config, allowing hot-toggle without pod restart.
	runtimeConfig.SetProfilingEnabled(enablePprof)

	// Start the reconciliation loop
	rec := reconciler.NewReconciler(
		k8sClient,
		clusterName,
		namespace,
		runtimeConfig,
	)
	go func() {
		if err := rec.Start(ctx); err != nil {
			logger.Error("Error in reconciler", "error", err)
			errChan <- err
		}
	}()

	// Create the resettable metrics registry
	redkeyMetricsRegistry := metrics.NewResettableRegistry()
	// controller-runtime's Registry already includes go/process collectors and rest_client_requests_total
	metricsGatherer := prometheus.Gatherers{crmetrics.Registry, redkeyMetricsRegistry}

	// Start the metrics collector (Redis INFO polling)
	collector := metrics.NewCollector(runtimeConfig, clusterName, namespace, k8sClient, redkeyMetricsRegistry)
	go func() {
		if err := collector.Start(ctx); err != nil {
			logger.Error("Error in metrics collector", "error", err)
			errChan <- err
		}
	}()

	// Start the metrics HTTP server (Prometheus endpoint + hot-togglable pprof).
	// Pprof endpoints are gated by RuntimeConfig.ProfilingEnabled() which is
	// updated by the reconciler from the RedkeyConfig CRD. This allows
	// enabling/disabling profiling at runtime without restarting the pod.
	metricsSrv := metrics.NewServer(metricsAddr, metricsGatherer, runtimeConfig)
	go func() {
		if err := metricsSrv.Start(ctx); err != nil {
			logger.Error("Error in metrics server", "error", err)
			errChan <- err
		}
	}()

	// Wait for context cancellation or critical error
	select {
	case <-ctx.Done():
		logger.Info("Shutting down Redkey Robin")
	case err := <-errChan:
		logger.Error("Critical error, shutting down", "error", err)
		os.Exit(1)
	}
}
