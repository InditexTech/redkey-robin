// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestVersionFlag(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the robin binary")
	}

	tests := []struct {
		name    string
		ldflags string
		want    string
	}{
		{name: "injected via ldflags", ldflags: "-X main.version=1.2.3-test", want: "1.2.3-test"},
		{name: "plain build falls back to dev", want: "dev"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bin := filepath.Join(t.TempDir(), "robin")
			args := []string{"build", "-o", bin}
			if tt.ldflags != "" {
				args = append(args, "-ldflags", tt.ldflags)
			}
			args = append(args, ".")
			if out, err := exec.Command("go", args...).CombinedOutput(); err != nil {
				t.Fatalf("go build: %v\n%s", err, out)
			}

			out, err := exec.Command(bin, "--version").CombinedOutput()
			if err != nil {
				t.Fatalf("robin --version: %v\n%s", err, out)
			}
			if got := strings.TrimSpace(string(out)); got != tt.want {
				t.Fatalf("version = %q, want %q", got, tt.want)
			}
		})
	}
}
