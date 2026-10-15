<!--
SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL S.A. (INDITEX S.A.)

SPDX-License-Identifier: CC-BY-SA-4.0
-->

# Change Log

All notable changes to this project will be documented in this file.

Format is based on [Keep a Changelog](http://keepachangelog.com/).

## [Unreleased] - dd.mm.yyyy
### Added
- Go native fuzz tests for the Redis output parsers (slot ranges, `CLUSTER NODES`, `--cluster check`), plus a `make fuzz` target.
### Fixed
### Changed
- Bump Go to 1.27.2, controller-runtime to v0.25.2, `k8s.io/*` to v0.37.1, go-redis to v9.23.0 and the remaining direct dependencies to their latest versions.
- Bump golangci-lint to v2.14.0 and add a `.golangci.yml` aligned with Redkey Operator.
- Bump the bundled `redis-cli` to 8.10.2.
- Pin envtest to Kubernetes 1.33 (production target).
