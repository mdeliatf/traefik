# Gateway API status — perf harness findings

Companion to [`gateway_status_async_spec.md`](./gateway_status_async_spec.md).
The spec describes the *plan*; this file holds the harness runbook,
measured results, and what the data confirms or refutes. Update this file
as new runs land — the spec stays stable.

Branch: `fix/gateway-api-hackathon`.
Package: `pkg/provider/kubernetes/gateway/`.
Tooling: `hack/perf/`.

## 1. What the upstream bench actually measures

`tests/attachedroutes/attachedroutes.go` at `origin/v1` of
`howardjohn/gateway-api-bench` defines the "Attached Routes" test:

- The clock starts when `pilot-load`'s `cluster.RunSimulation` begins firing
  HTTPRoute creates.
- The clock stops when an informer on the target Gateway observes
  `Status.Listeners[0].AttachedRoutes == N`.
- **"Setup time"** in the published table (README of `gateway-api-bench`)
  is defined as: *the time after the last route is created until the
  attachedRoutes is updated to the total route count*. Code:
  `topT = sampleTime - startTime - ready`, where `ready` is the
  create-phase duration. **It is the tail, not the total wall time.**
- **"Writes"** is the number of distinct `AttachedRoutes` values
  observed = the number of Gateway-status writes that changed the
  counter.

`pilot-load`'s create loop (`sims/cluster/namespace.go`,
`AggregateSimulation.Run`) is sequential, one apiserver Create per
iteration, with `GracePeriod=0`. **Critically**, it sets `rest.Config.QPS
= 100000` (`pkg/kube/kube.go:64`) — far above client-go's default of 5
— so the create loop is not bottlenecked by client-side throttling.

## 2. Harness overview

Two layers, both in this branch:

- **Layer 1** — in-process envtest (`pkg/provider/kubernetes/gateway/perf_test.go`
  + `perfharness_test.go`). The provider runs in the same process as the
  test; the apiserver+etcd are local binaries from
  `KUBEBUILDER_ASSETS`. envtest's default `rest.Config` already sets
  `QPS=1000/Burst=2000`, so the rate limiter is not in the way.
- **Layer 2** — kind cluster (`hack/perf/gateway-status-bench.sh`,
  `hack/perf/cmd/loadgen`, `hack/perf/{kind,values,audit}.yaml`).
  Traefik is the Helm chart from `traefik.github.io/charts`, built
  locally from HEAD via `make build-image` and loaded into kind. Helm
  values are a byte-for-byte copy of the bench's `install/basic.sh`.

Both layers compute the bench-aligned signals:

| Metric                | Meaning                                                                 |
|-----------------------|-------------------------------------------------------------------------|
| `createDuration`      | t0 → last `Create()` returns. Apiserver-bound.                          |
| `setupTime`           | last create → `AttachedRoutes == N`. **This is the bench's column.**    |
| `timeToAttachedN`     | t0 → `AttachedRoutes == N`. `createDuration + setupTime`.               |
| `gatewayStatusWrites` | distinct `AttachedRoutes` values seen. Comparable to bench's "writes".  |
| `attachedSeries`      | `[(offsetMs, attachedRoutes)]`. Plot to see the convergence curve.      |

Layer 1 additionally collects per-rebuild timing via `rebuildHook` and a
**status-I/O split**: `statusIOTimeNs` on `clientWrapper.metrics`
accumulates `time.Since` around each successful
`csGateway.*.UpdateStatus(...)` call. The summary's `ioShare%` is
`statusIOTime / (RebuildMean × RebuildCount)`. **This is the field that
settles "is status I/O the bottleneck".**

## 3. Running Layer 1 — in-process envtest

**Why:** Fast iteration loop. No Docker. The provider runs as a goroutine
in the test, so we get rebuild count, per-rebuild duration, and the
status-I/O split that Layer 2 cannot expose.

**One-time setup:**

```bash
go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest
export KUBEBUILDER_ASSETS="$(setup-envtest use 1.31.x -p path)"
```

**Sequential creates (matches `pilot-load`'s loop exactly):**

```bash
go test -v -run=TestStatusPerf ./pkg/provider/kubernetes/gateway/ \
  -perf -routes=1000
```

**Burst creates (synthesises the regime the bench observed on fast
hardware; see §5):**

```bash
go test -v -run=TestStatusPerf ./pkg/provider/kubernetes/gateway/ \
  -perf -routes=1000 -route-batch=16 -quiescence=10s
```

**What to read in the `perf summary:` line:**

- `setupTime` — bench-equivalent.
- `rebuilds=X (mean=… p99=… max=… totalRebuild=… statusIO=… ioShare=…)` —
  per-rebuild timing. The two numbers that matter:
  - `totalRebuild` = cumulative wall time spent inside
    `loadConfigurationFromGateways`.
  - `ioShare` = fraction of that spent waiting on the apiserver
    `UpdateStatus` round-trip. **Anything ≥80% means status I/O is the
    bottleneck.**
- `gatewayStatusSamples` — Gateway-status writes that changed the
  AttachedRoutes counter.

**Flags:**

- `-routes=N` (default 1000).
- `-route-batch=N` (default 1). When >1, route creates are fanned out
  across N goroutines. Use 16 to reproduce the bench's burst regime on
  slow hardware.
- `-cpuprofile=path` — `runtime/pprof` CPU profile.
- `-quiescence=DUR` (default 3s) — increase to 10s under burst load so
  late writes don't get cut off.

CRDs are vendored at
`pkg/provider/kubernetes/gateway/testdata/perf/crds/standard-install.yaml`
(Gateway API v1.5.1 release asset). Bump alongside `go.mod`.

## 4. Running Layer 2 — kind cluster

**Why:** Sanity-check that the in-process numbers track a real cluster.
Adds real network round-trips and the upstream Helm chart. Used once
before iterating on §2/§3 and again after each material change. **Not
part of the inner loop** — it builds the Traefik image and creates a
cluster.

**Prerequisites:** `docker`, `kind`, `helm`, `kubectl`, `go`.

**Sequential creates (matches `pilot-load` literally):**

```bash
hack/perf/gateway-status-bench.sh --routes 1000
```

**Burst creates (with the QPS limiter bumped up to match `pilot-load`):**

```bash
hack/perf/gateway-status-bench.sh --routes 1000 --concurrency 16
```

`--concurrency` fans creates across N goroutines. The loadgen sets
`rest.Config.QPS=1000/Burst=2000` by default (matching envtest), so
the workers actually run in parallel — without this fix, every worker
piles up behind client-go's default 5 QPS limiter and the wall time is
the same as `--concurrency=1`. The `-qps` / `-burst` flags on the
loadgen tune this further.

**Flags on the driver:**

- `--routes N` (default 1000).
- `--concurrency N` (default 1).
- `--cluster NAME` (default `traefik-perf`).
- `--quiescence DUR` (default 5s).
- `--keep` — leave the kind cluster running after the report is printed.
- `--no-build` — skip `make build-image`. Use after the first run.

**What to read in the JSON report:**

- `setupTime` — bench column. Compare against the bench's published 180s.
- `gatewayStatusWrites` — should be in the same order of magnitude as the
  bench's "writes" column (73 for Traefik on v1).
- `attachedSeries` — the convergence curve. Plot it; a long flat stretch
  followed by a single jump to N indicates the provider is stuck in a
  single rebuild.

The script also extracts `/var/log/kubernetes/audit/audit.log` from the
kind control-plane node and counts status writes on
`gateway.networking.k8s.io`. Cross-check against Traefik's own counter
(visible only in Layer 1 today).

## 5. Why concurrency matters: the burst-regime gotcha

The bench's published Setup-time differences (Cilium 1s, Traefik 180s)
look like they could be apiserver-bound, but they are not. The bench was
run on a **16-core AMD 9950x with 96 GB RAM** (`README.md:212` of
`gateway-api-bench`). On that machine, sequential `Create()` calls return
in milliseconds and the "create phase" finishes in seconds. On slower
hardware (macOS Docker Desktop), sequential creates return in ~200ms
each — stretching the create phase to ~3 minutes for N=1000.

This is a 40× slowdown of the create phase that changes *which regime
Traefik is in*:

- **Slow create rate (e.g., ~5/s):** every event has time to be fully
  processed before the next one lands. The provider keeps pace
  in-flight. Setup time is dominated by the few rebuilds left after
  the last create. **Observed: setupTime = 9.2s.**
- **Fast create rate (e.g., ~600/s):** events queue faster than the
  rebuild loop can drain them. After the last create, the provider
  still has to process the backlog. Setup time grows by ~N × per-write
  I/O cost. **Observed: setupTime = 197s with `--concurrency=16`.**

Layer 1 (envtest) is in the fast regime by default. Layer 2 on slow
hardware needs `--concurrency 16` to enter it.

## 6. Measured results (N=1000)

All on macOS arm64, Docker Desktop, kind single-node cluster, Helm chart
configured per `hack/perf/values.yaml`. Numbers are from this branch's
HEAD before any §2/§3 work.

**Layer 1, sequential creates (`-routes=1000`):**

```
routes=1000 concurrency=1 createDuration=1.598s setupTime=389ms
timeToAttachedN=1.987s gatewayStatusSamples=20
rebuilds=20 (mean=99ms p99=401ms max=401ms totalRebuild=1.98s)
writes={gateway=17 httpRoute=1000 …}
```

**Layer 1, burst creates (`-routes=1000 -route-batch=16`):**

```
routes=1000 concurrency=16 createDuration=431ms setupTime=1.57s
timeToAttachedN=2.001s gatewayStatusSamples=3
rebuilds=7 (mean=290ms p99=1.48s max=1.48s
  totalRebuild=2.027s statusIO=1.975s ioShare=97%)
writes={gateway=4 httpRoute=1000 …}
```

**Layer 2, sequential creates (`--routes 1000`):**

```json
{
  "routes": 1000,
  "concurrency": 1,
  "createDuration": "3m18.385s",
  "setupTime": "9.218s",
  "timeToAttachedN": "3m27.603s",
  "gatewayStatusWrites": 47,
  "httpRouteEvents": 2000
}
```
audit log: 1050 status updates.

**Layer 2, burst creates (`--routes 1000 --concurrency 16`):**

```json
{
  "routes": 1000,
  "concurrency": 16,
  "createDuration": "1.657s",
  "setupTime": "3m17.143s",
  "timeToAttachedN": "3m18.8s",
  "gatewayStatusWrites": 3,
  "httpRouteEvents": 2000,
  "attachedSeries": [
    {"offsetMs": 131,    "attachedRoutes": 1},
    {"offsetMs": 2591,   "attachedRoutes": 20},
    {"offsetMs": 198800, "attachedRoutes": 1000}
  ]
}
```
audit log: 1006 status updates.

**This reproduces the bench's regime.** 197s vs the bench's 180s, on
much slower hardware, with the same shape: a small initial burst, a
long flat-line, then a single jump to N at the very end.

## 7. Conclusion — what is confirmed

**Status I/O is the bottleneck under burst load.** The Layer 1 burst run
gives the definitive answer: 97% of `loadConfigurationFromGateways`
wall time is spent inside `csGateway.*.UpdateStatus(...)`. Route
translation, hashing, lister Gets, and equality compares together
account for ~3% (≈50ms in a 2s run).

The Layer 2 burst run shows the same shape against a real apiserver:
1000 status writes serialised behind a single goroutine, blocking the
rebuild loop for 197 seconds. Only 3 Gateway-status changes leak out
during that interval — the rest of the AttachedRoutes counter changes
are invisible because the provider never gets to write Gateway status
again until the route backlog is drained.

**This validates §2 (decouple status writes from the rebuild) and §3
(bounded parallelism in the writer)** of the spec:

- §2 moves the I/O off the rebuild critical path. The rebuild becomes
  ~50ms of in-memory work + an enqueue. The dynamic config gets reloaded
  almost immediately. The provider can keep up with the event rate.
- §3 lets the writer parallelise. With a worker pool of 8, the
  apiserver writes that today take 197s sequentially would take roughly
  25s. With 16 workers, ~12s — within an order of magnitude of Cilium's
  1s (the rest of the gap is hardware).

## 8. What is *not* validated

- **Steps 4 and 5 remain independent of the perf chain.** Step 4 (foreign-
  parent fix) is a correctness item; the harness doesn't cover it. Step 5
  (cheap rebuild cleanups) only matters once Step 2/3 have removed the I/O
  bulk — its return is currently ≈0.
- **Multi-replica HA leader election** (spec §8) is still out of scope;
  nothing here addresses it.
- **CI integration of Layer 1** (spec §8) is still deferred — envtest
  startup variance dominates at small N and would flake.

## 9. Recommended next steps

1. Implement §2 on a new branch. Re-run Layer 1 at `-route-batch=16` and
   confirm `ioShare` drops dramatically (target: <30%) and `setupTime`
   on Layer 1 stays sub-second.
2. Implement §3 (worker pool, default 8). Re-run Layer 2 with
   `--concurrency 16`. Target: `setupTime` ≤ 30s at N=1000 on the
   same kind cluster.
3. Implement §4 (foreign-parent filter). Independent of perf; gate
   on `make test-gateway-api-conformance`.
4. Re-run Layer 2 once after §2 lands and once after §3 lands. Don't
   re-tune; we want apples-to-apples deltas. Append the new numbers
   to §6 of this file under a fresh sub-heading; keep the pre-§2
   numbers as the baseline.