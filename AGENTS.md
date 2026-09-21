<!--
SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)

SPDX-License-Identifier: Apache-2.0
-->

# AGENTS.md

Reference guide for AI agents and automated tools working on the Redkey Robin codebase.

---

## Project Summary

**Redkey Robin** is the companion runtime used by Redkey Operator to process configuration rollouts for a single `Redkey`.

It runs as a standalone Go binary that:

- connects to the Kubernetes API,
- watches the `RedkeyConfig` custom resources produced by Redkey Operator,
- selects the next actionable config revision for one cluster,
- advances config lifecycle state sequentially through the reconciliation loop, and
- exposes Prometheus metrics for the process.

Today, Robin focuses on orchestration of `RedkeyConfig` progression rather than cluster deployment. It operates per cluster instance, using `--cluster-name` and `--namespace` to scope its work.

### Technology Stack

| Layer | Technology |
| ----- | ---------- |
| Language | Go 1.26.8 |
| Runtime style | Standalone controller-style daemon |
| Kubernetes client | [controller-runtime](https://sigs.k8s.io/controller-runtime) v0.24.0 |
| API dependency | `github.com/inditextech/redkey-operator/api/v1beta1` via local `replace ../redkey-operator` |
| Metrics | [Prometheus client_golang](https://github.com/prometheus/client_golang) |
| Testing | Go `testing`, [Ginkgo v2](https://github.com/onsi/ginkgo) + [Gomega](https://github.com/onsi/gomega), [envtest](https://sigs.k8s.io/controller-runtime/tools/setup-envtest) |
| Linting | [golangci-lint](https://github.com/golangci/golangci-lint) v2.1.0 |

---

## Repository Layout

Key paths to understand before changing code:

- `cmd/main.go`: process entrypoint, flag parsing, startup wiring.
- `internal/config/`: shared runtime configuration consumed by the process.
- `internal/health/`: health/readiness endpoints and related plumbing.
- `internal/kubernetes/`: Kubernetes client helpers and cluster interactions.
- `internal/metrics/`: Prometheus collection and export logic.
- `internal/reconciler/`: config selection, state transitions, and reconciliation loop.
- `internal/redis/`: Redis connectivity, INFO/CLUSTER parsing, and helpers.
- `test/integration/`: envtest-based integration coverage.

Operational assumptions:

- Robin is a single-cluster runtime: one process instance is scoped to one `Redkey`.
- The sibling checkout at `../redkey-operator` is part of the expected local layout and provides the CRD/API types used by this module.
- Integration tests load CRDs from `../redkey-operator/config/crd/bases`.

---

## Build Commands

All common operations are driven by `make`. Tools that are not yet present are downloaded automatically into `bin/`.

This repository does not use Maven. There is no `pom.xml` or `mvnw` in Robin, so agents must not suggest `mvn` commands here. If an external workflow or template expects Maven goals/phases, use the following equivalents.

### Maven goal equivalents

| Maven goal or phase | Robin command | Notes |
| ------------------- | ------------- | ----- |
| `mvn validate` | `make tidy && make fmt && make vet && make lint` | Closest pre-test validation sequence. |
| `mvn test` | `make test` | Unit tests only. Generates `cover.out`. |
| `mvn failsafe:integration-test` | `make test-integration` | Runs envtest-based integration tests. |
| `mvn failsafe:verify` | `make test-integration` | Same integration suite; there is no separate Maven-style verify step. |
| `mvn verify` | `make lint && make test-all` | Preferred full local quality gate. |
| `mvn package` | `make build` | Produces `bin/robin`. |
| `mvn install` | not applicable | No Maven-style local artifact install phase exists. |

### Prerequisites (installed manually)

- Go 1.26+
- Make
- Docker or Podman (only needed for image build targets)
- Access to the sibling `../redkey-operator` checkout, because this module imports its API types through a local `replace`

### Dependency and formatting tasks

```shell
make tidy        # go mod tidy
make fmt         # go fmt ./...
make vet         # go vet ./...
make lint        # golangci-lint run
make lint-fix    # golangci-lint run --fix
make lint-config # validates golangci-lint configuration
```

### Binary build

```shell
make build       # go build -o bin/robin cmd/main.go
```

### Local execution

```shell
make run                                 # runs Robin locally
make run CLUSTER_NAME=mycluster NAMESPACE=mynamespace
```

Robin requires `--cluster-name` and `--namespace`. It also exposes `--metrics-bind-address`, `--reconcile-interval`, `--reconcile-interval-on-error`, and `--reconcile-interval-on-wait`.

### Container image

```shell
make docker-build              # builds image tagged as localhost:5005/redkey-robin:<VERSION>
make docker-push               # pushes the image
make docker-buildx             # cross-platform build (linux/amd64 + linux/arm64) and push
```

Override the image tag with `IMG=<registry>/<name>:<tag>`.

The image bundles a `redis-cli` built from a pinned Redis version, controlled by `REDIS_CLIENT_VERSION` in the `Makefile` (with a matching default `ARG REDIS_CLIENT_VERSION` in the `Dockerfile`). Whenever the Redis server version is bumped in the samples or Helm charts, align `REDIS_CLIENT_VERSION` to the same version so the bundled `redis-cli` matches the deployed Redis server. Likewise, keep the `defaultRedisImage` fallback in `internal/kubernetes/objects.go` (used when a cluster does not specify an image) aligned with that same Redis version.

### Fast verification

```shell
make verify      # tidy + fmt + vet + build + test (unit tests only)
```

`make verify` is useful as a quick preflight, but it does not execute `make test-integration`. For the full local gate, use `make lint && make test-all`.

---

## Testing Instructions

### Mandatory validation for every change

For code changes in this repository, before considering the task complete, run:

```shell
make lint
make test-all
```

Use `make verify` as a fast pre-check when iterating locally, but do not treat it as a replacement for `make test-all`.

If a change only touches documentation or agent instructions, executable validation may be skipped when there is nothing meaningful to compile or run; in that case, keep the edit limited and consistent with the Makefile and repository layout.

### Recommended validation flows

```shell
make test                     # fast unit-only loop while iterating
make test-integration         # envtest coverage for reconciler/runtime behavior
make lint && make test-all    # preferred full local gate before handing off
make build                    # optional final binary build check
```

### Unit tests

```shell
make test
```

Runs unit tests for all packages except `e2e` and `test/integration`. Generates a coverage profile at `cover.out`.

```shell
make coverage    # generates coverage.html from cover.out
```

### Integration tests (envtest)

Requires the envtest binaries to be present. The target installs them automatically.

```shell
make test-integration
```

Integration tests use CRDs from the sibling operator repository at `../redkey-operator/config/crd/bases` and run against envtest, not a real cluster.

### All tests (unit + integration)

```shell
make test-all
```

There is currently no dedicated e2e target in this repository. Treat `make test-all` as the highest-fidelity automated suite available inside Robin itself.

---

## Style Guidelines

### Code conventions

- Follow standard Go idioms and the [Effective Go](https://go.dev/doc/effective_go) guidelines.
- For every change, run `make lint` and `make test-all` before finishing the task.
- Runtime entrypoint lives in `cmd/main.go`.
- Core reconciliation logic lives in `internal/reconciler/`.
- Metrics HTTP server code lives in `internal/metrics/`.
- Tests mirror their subject file with a `_test.go` suffix in the same package for unit coverage.
- Integration tests live under `test/integration/` and depend on envtest plus CRDs from the sibling operator repository.
- Use structured logging via `log/slog`; do not use `fmt.Print*` for operational output.

### Architecture conventions

- A Robin process is scoped to one `Redkey`.
- The reconciliation loop polls `RedkeyConfig` objects in sequence and should remain idempotent.
- State transitions that can continue synchronously may trigger an immediate re-poll, but waiting states (for example pods becoming Ready or cluster convergence) must use the configured wait interval; idle and error states use their respective configured intervals.
- Changes to CRD types come from the operator repository, not from Robin directly; keep the local module replacement aligned with the sibling checkout.
- REUSE compliance is required: every source file must have an `SPDX-FileCopyrightText` and `SPDX-License-Identifier` header.

---

## Upgrade Reconciler — Critical Design Knowledge

The upgrade state machine in `internal/reconciler/upgrade_reconciler.go` is the most complex reconciler. This section documents hard-won invariants that must be preserved.

### Strategy Selection

```go
fastUpgradeEligible = Ephemeral && ReplicasPerPrimary == 0 && PurgeKeysOnRebalance == true
```

All three conditions are required. **Clusters with replicas always use Rolling N+1**, even if ephemeral and purgeKeys is true.

### Rolling N+1 State Machine

```
handleUpgradeStart → handleUpgradeScalingUp → handleUpgradeResharding ⟷ handleUpgradeRollingUpdate → handleUpgradeEnding → handleUpgradeScalingDown
```

Substatus values (from operator API):
- `AddingExtraNode` → `DrainingNode` ⟷ `RollingUpdate` → `MovingLastSlots` → `RemovingExtraNode`
### Update Strategy: OnDelete + Manual Pod Deletion

The upgrade uses **OnDelete** StatefulSet update strategy (not RollingUpdate with partition). This ensures:
- **No pods are recreated automatically** when the template is updated
- Only specifically targeted pods (drained primaries + their replicas) are deleted and recreated
- Replicas of primaries that still hold slots are **never disrupted**, preserving HA

The reconciler controls pod recreation by:
1. Calling `kubernetes.DeletePod()` on the drained primary
2. Calling `kubernetes.DeletePod()` on each replica of that specific primary (via `recycleReplicasForPrimary`)
3. At the end (`handleUpgradeScalingDown`), restoring `RollingUpdate` strategy with partition=0
### StatefulSet Layout (with replicas)

For `primaries=P`, `replicasPerPrimary=R`:
- Pods `0 .. P-1` → primaries
- Pods `P .. P+P*R-1` → replicas (pod `P+i*R+j` is replica `j` of primary `i`)
- Pods `P+P*R` → extra primary (new image)
- Pods `P+P*R+1 .. P+P*R+R` → extra replicas (new image)

**Key formula**: `totalMembers = primaries + primaries * replicasPerPrimary`. This is used everywhere — scale calculations, CLUSTER MEET ranges, CLUSTER FORGET iteration. Getting this wrong (e.g., using just `primaries`) was the root cause of 5 bugs fixed in June 2025.

### Pivot Pattern (destination logic)

Slots do NOT always go to/from the extra node. Instead:
- **First reshard** (partition = P-1): slots move TO the extra primary (ordinal = `P + P*R`)
- **Subsequent reshards** (partition < P-1): slots move to `partition + 1` (the previously recycled primary that already has the new image)
- **Ending phase**: slots move FROM the extra primary BACK to node 0

This ensures each slot migrates exactly twice and each recycled node is immediately productive.

### Common Bugs to Watch For (replica-aware clusters)

1. **Extra replica must be explicitly attached**: After `CLUSTER MEET` for the extra pods, run `CLUSTER REPLICATE` on the extra replica to attach it to the extra primary. Without this, the extra replica joins as a master (cluster gets P+2 masters instead of P+1).

2. **Ordinal calculations must use `P + P*R`**, not just `P`:
   - `handleUpgradeResharding` first dest: `primaries + primaries*replicasPerPrimary`
   - `handleUpgradeEnding` extra ordinal: `primaries + primaries*replicasPerPrimary`
   - `handleUpgradeRollingUpdate` seed: `primaries + primaries*replicasPerPrimary` (the extra primary is the seed for meeting recycled nodes back)

3. **`forgetReplicasOfNode`** must actually call `CLUSTER FORGET` via `forgetNodeFromAll` for each replica, not just log. **`forgetNodeFromAll` / `forgetFailedNodes` / `forgetExtraReplicas`** must iterate over ALL cluster members (primaries + replicas + extras), not just primaries. A FORGET issued only to primaries leaves stale entries in replica gossip tables.

4. **Seed node for CLUSTER MEET after rolling update**: Must be the extra primary (ordinal `P+P*R`), NOT one of the original primaries. Original primaries get recycled and temporarily lose their cluster state; the extra primary is the only node guaranteed to remain stable throughout the entire upgrade.

5. **`CLUSTER FIX` before every reshard**: Required to clear any stuck open/importing slots from a previous partial reshard that was interrupted.

6. **Only recycle drained pairs**: As per the upgrade design (section 6.2 of the technical analysis), only primaries with 0 slots AND their replicas may be recycled. Replicas of primaries that still hold slots must never be restarted. This is enforced by `recycleReplicasForPrimary(ctx, config, primaryOrdinal, password)` which targets a single shard.

7. **No `defer` in loops for Redis clients**: Functions that iterate over primaries/replicas (like `rebalanceReplicas`, `recycleReplicasForPrimary`) must use explicit `.Close()` calls, not `defer`, to avoid connection accumulation across loop iterations. The same applies to one-shot helpers invoked inside the reconcile loop such as `ensurePivotReplica`, which opens a temporary client and must close it manually before returning.

### Recycle Decision: controller-revision-hash, not image

`handleUpgradeRollingUpdate` decides whether a pod still needs recycling by comparing the StatefulSet's `Status.UpdateRevision` (desired revision) against each pod's `controller-revision-hash` label (the revision the pod was created with), via `kubernetes.GetStatefulSetUpdateRevision` and `kubernetes.GetPodControllerRevisionHash`. **Do not gate recycling on the container image alone** — any spec change (resources, labels, annotations, redisConfig, env, etc.) bumps the revision and must trigger a recycle. The `PodTemplateHashLabel` constant (`"controller-revision-hash"`) is the native StatefulSet mechanism; reusing it keeps Robin consistent with `kubectl rollout`.

### HA / Data-Safety Invariants in Rolling Update

These guard against data loss and split-brain during the rolling phase; never remove them:

1. **Wait for replica resync before advancing**: After `CLUSTER REPLICATE`, call `waitReplicaSynced` which polls `ReplicaLinkUp` (parses `INFO replication` → `master_link_status:up`). Advancing before the replica is in sync risks promoting an empty replica on failover.
2. **Cluster check per iteration**: Run `ClusterCheck` before moving to the next partition. A partition must not advance while slots are open/importing or gossip has not converged.
3. **Flush + persist on persistent clusters**: For non-ephemeral (PVC-backed) clusters, drained nodes are cleaned with `flushAndPersistNode` (`FLUSHALL` then synchronous `SAVE`) so the recycled pod does not reload stale slot data from its RDB on restart. Skipped for ephemeral clusters (nothing on disk).

### Shutdown / Close Convention

Resources that own background goroutines or pooled Redis connections expose an idempotent `Close()` that the parent calls in a chain: `reconciler.Start` defers `clusterReconciler.Close()` → `healthReconciler.Close()` → `health.Checker.CloseAll()`. When adding a component that holds clients or goroutines, wire its `Close()` into this chain rather than relying on GC; `Close()` must be safe to call more than once.

### Testing the Upgrade

```shell
# E2E with replicas (the critical path):
cd ../redkey-operator && go test ./test/e2e/ -v -ginkgo.v \
  -ginkgo.label-filter="upgrade" -ginkgo.focus="with replicas" -timeout 20m

# E2E without replicas:
cd ../redkey-operator && go test ./test/e2e/ -v -ginkgo.v \
  -ginkgo.label-filter="upgrade" -ginkgo.focus="without replicas" -timeout 15m
```

The E2E tests create a 3-primary cluster, write 50 keys, trigger an image upgrade, and verify all keys survive. The "with replicas" test uses `replicasPerPrimary: 1` (8 pods total during upgrade: 3P + 3R + 1 extra P + 1 extra R).

### Image Build & Load for E2E

```shell
make docker-build                           # builds localhost:5005/redkey-robin:<VERSION>
docker tag localhost:5005/redkey-robin:<VERSION> localhost:5005/redkey-robin:<VERSION>
docker push localhost:5005/redkey-robin:<VERSION>
```

The Kind cluster uses a local registry at `localhost:5005`. Both robin and operator images must be pushed there before E2E tests.

### Dependency management

- Use `go mod tidy` after adding or removing dependencies.
- Do not vendor dependencies; the project relies on the Go module cache.
- Preserve the local `replace github.com/inditextech/redkey-operator => ../redkey-operator` unless the repository layout is intentionally changed.

---

## Auth Hot-Reload via CONFIG SET — Design Knowledge

Auth changes (requirepass/masterauth) are applied via hot-reload (CONFIG SET) instead of triggering a cluster operation. This section documents the design invariants.

### Change Detection Split

Auth changes are detected separately from other Redis config changes in `config_changes.go`:

```go
type ChangeReport struct {
    HasRedisConfigChanges bool  // RedisConfig, Version (triggers rolling upgrade)
    HasAuthChanges        bool  // Auth only (hot-reloaded, no cluster op)
    // ...
}
```

- `detectRedisConfigChanges()` now only checks `RedisConfig` and `Version` — NOT `Auth`
- `HasAuthChanges` is set directly in `DetectChanges()` via `previous.Auth != target.Auth`
- `RequiresClusterOperation()` does NOT include `HasAuthChanges` → auth-only never triggers an upgrade
- `HasAnyChange()` DOES include `HasAuthChanges` so the reconciler knows there is work to do

### Status Transition for Auth-Only

In `DetermineStatusTransition()`:
- Auth-only changes fall through to the final `return ""` (no transition needed)
- Auth + Robin: `OnlyRobinChanges()` returns `true` (also no transition)
- Auth + image/config: auth is applied via CONFIG SET first, then the normal transition proceeds

### applyAuthToAllNodes — Execution Order

In `cluster_reconciler.go`, `handleConfigChange()` applies auth BEFORE `DetermineStatusTransition()`:

1. If `HasAuthChanges`: call `applyAuthToAllNodes()` which performs CONFIG SET on ALL running pods
2. Then determine and execute status transition

`applyAuthToAllNodes()`:
1. Gets the new password from the target config's auth secret
2. Gets the old password from the previous config's auth secret (for connecting to existing nodes)
3. Iterates over all pod addresses via `GetPodAddresses`
4. For each pod: `CONFIG SET requirepass <new>` + `CONFIG SET masterauth <new>`
5. Updates the ConfigMap so restarted pods get the correct auth
6. Updates the runtime config's auth secret reference

### Why Auth Goes First

Auth is applied before the cluster operation to ensure:
- New pods created during scaling/upgrade use `buildRedisConf()` with the new auth
- All nodes share the same masterauth during the upgrade (critical for replica reconnections)
- `redis-cli -a <password> PING` false positive: `redis-cli -a <wrong> PING` returns PONG even with wrong password (AUTH error goes to stderr, PONG to stdout). The `CheckAuthRequired`/`CheckAuthDisabled` helpers in the e2e framework avoid this by NEVER using `-a` flag — they run `redis-cli PING` without any password.

### Key Flow for Combined Changes

```
Auth + Image change:
  1. applyAuthToAllNodes() → CONFIG SET on all nodes
  2. DetermineStatusTransition() → returns "Upgrading"
  3. handleUpgradeStart → rolling/fast upgrade proceeds with auth already set

Auth-only change:
  1. applyAuthToAllNodes() → CONFIG SET on all nodes
  2. DetermineStatusTransition() → returns ""
  3. Mark config as Applied, restore Ready status
  4. No pods are recycled, no cluster operation occurs
```

### E2E Test Pattern for Auth Verification

```go
// Correct — no false positive:
Expect(framework.CheckAuthRequired(namespace, pod)).To(BeTrue())
Expect(framework.CheckAuthDisabled(namespace, pod)).To(BeTrue())

// Potentially false positive — avoid for auth verification:
// framework.PingRedis(namespace, pod, wrongPassword) // returns PONG even on wrong pass
```

---

## Commit and PR Management

### Commit format

Follow the [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) specification:

```text
<type>(<optional scope>): <short description>

[optional body]

[optional footers]
Signed-off-by: Name <email>
```

Common types: `feat`, `fix`, `docs`, `chore`, `refactor`, `test`, `ci`.

### Required commit properties

Every commit in a PR **must**:

1. Include a `Signed-off-by` trailer (`git commit -s`). This certifies agreement with the [CLA](./CLA.md).
2. Be GPG-signed with a verified key (`git commit -S`, or set `git config --local commit.gpgsign true`).
3. Use a verified email address associated with the GitHub account.

### Pull Request guidelines

- Open an issue before starting significant work and reference it in the PR (`Closes #<issue>`).
- Check existing issues and PRs to avoid duplicate work.
- Keep PRs focused; split unrelated changes into separate PRs.
- For every change, ensure `make lint` and `make test-all` pass locally before opening or updating a PR.
- Add or update tests for every code change.
- Document behaviour changes if they affect operator or runtime workflows.
- An automated check will validate commit signatures and CLA compliance on every PR.
