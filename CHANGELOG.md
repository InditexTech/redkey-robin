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
- Slot range parsing rejects slots outside `0-16383`, and `CountSlotsFromRanges` ignores malformed tokens instead of returning negative or inflated counts. The checks that protect slot owners from `CLUSTER FORGET` now treat any unparseable slot entry as ownership (#62).
- Bump `golang.org/x/net` to v0.61.0 to fix HTTP/2 vulnerabilities GO-2026-6603, GO-2026-6610, GO-2026-6611, GO-2026-6612 and GO-2026-6617.
### Changed
- Bump Go to 1.27.2, controller-runtime to v0.25.2, `k8s.io/*` to v0.37.1, go-redis to v9.23.0 and the remaining direct dependencies to their latest versions.
- Bump golangci-lint to v2.14.0 and add a `.golangci.yml` aligned with Redkey Operator.
- Bump the bundled `redis-cli` to 8.10.2.
- Pin envtest to Kubernetes 1.33 (production target).
### Removed
- Unused `redis.ParseOpenSlots` helper and its fuzz target.
