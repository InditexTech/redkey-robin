// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"runtime"

	"github.com/prometheus/client_golang/prometheus"
)

// BuildInfoMetricName is the name of the gauge exposing Robin's build metadata.
const BuildInfoMetricName = "redkey_robin_build_info"

// RegisterBuildInfo registers a constant gauge (value 1) labelled with the Robin and Go versions.
func RegisterBuildInfo(reg prometheus.Registerer, version string) error {
	buildInfo := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: BuildInfoMetricName,
		Help: "Build information for Redkey Robin (value is always 1; see labels).",
		ConstLabels: prometheus.Labels{
			"version":    version,
			"go_version": runtime.Version(),
		},
	})
	buildInfo.Set(1)
	return reg.Register(buildInfo)
}
