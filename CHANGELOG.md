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
- Scale-to-zero lookahead: when a later pending `RedkeyConfig` targets `primaries: 0`, Robin jumps straight to the last such config and marks the intermediate ones (including a wedged `InProgress` config) as `Superseded`, regardless of `skipIfSuperseded`. This lets a cluster blocked by a config that cannot complete (e.g. an invalid image) be recovered by scaling it to zero (#41, partially mitigates #42).
- Integration specs against real `redis-server` processes in the Ginkgo integration suite (label `redis`), plus a `make redis` target that builds `redis-server`/`redis-cli` at `REDIS_CLIENT_VERSION` into `bin/` (required by `make test-integration`).
### Fixed
- A second Rolling N+1 upgrade on a persistent cluster with `deletePVC: false` no longer stalls forever. The extra (N+1) pods remount the PVCs retained from the previous upgrade, whose `nodes.conf` left the extra primary as a replica of node 0; Robin now resets such stale pods to pristine empty masters before meeting them (recovering clusters already stuck), and leaves the extra pods pristine at the end of every upgrade.
- Scale-up of a persistent cluster with `deletePVC: false` resets re-created pods that remount a PVC retained by an earlier scale-down, instead of integrating them under their stale identity.
- Recycling a drained node during a Rolling N+1 upgrade on a persistent cluster no longer lets the restarted pod reload stale keys from the AOF and claim their slots: the dataset is flushed again after `CLUSTER RESET HARD`, when Redis had demoted the drained node to a replica.
- Slot range parsing rejects slots outside `0-16383`, and `CountSlotsFromRanges` ignores malformed tokens instead of returning negative or inflated counts. The checks that protect slot owners from `CLUSTER FORGET` now treat any unparseable slot entry as ownership (#62).
- Bump `golang.org/x/net` to v0.61.0 to fix HTTP/2 vulnerabilities GO-2026-6603, GO-2026-6610, GO-2026-6611, GO-2026-6612 and GO-2026-6617.
### Changed
- Bump Go to 1.27.2, controller-runtime to v0.25.2, `k8s.io/*` to v0.37.1, go-redis to v9.23.0 and the remaining direct dependencies to their latest versions.
- Bump golangci-lint to v2.14.0 and add a `.golangci.yml` aligned with Redkey Operator.
- Bump the bundled `redis-cli` to 8.10.2.
- Pin envtest to Kubernetes 1.33 (production target).
### Removed
- Unused `redis.ParseOpenSlots` helper and its fuzz target.
