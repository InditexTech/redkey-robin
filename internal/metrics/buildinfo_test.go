// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"runtime"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestRegisterBuildInfo(t *testing.T) {
	reg := prometheus.NewRegistry()
	if err := RegisterBuildInfo(reg, "1.2.3"); err != nil {
		t.Fatalf("RegisterBuildInfo: %v", err)
	}

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != BuildInfoMetricName {
			continue
		}
		m := f.GetMetric()[0]
		if got := m.GetGauge().GetValue(); got != 1 {
			t.Fatalf("expected value 1, got %f", got)
		}
		labels := make(map[string]string)
		for _, l := range m.GetLabel() {
			labels[l.GetName()] = l.GetValue()
		}
		if labels["version"] != "1.2.3" {
			t.Fatalf("expected version=1.2.3, got %q", labels["version"])
		}
		if labels["go_version"] != runtime.Version() {
			t.Fatalf("expected go_version=%s, got %q", runtime.Version(), labels["go_version"])
		}
		return
	}
	t.Fatalf("metric %s not found", BuildInfoMetricName)
}
