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

The harness is a kind-based end-to-end driver under `hack/perf/`:
`gateway-status-bench.sh`, `cmd/loadgen`, and
`{kind,values,audit}.yaml`. Traefik is installed from the
`traefik.github.io/charts` Helm chart, built locally from HEAD via
`make build-image` and loaded into the kind node. Helm values are a
byte-for-byte copy of the upstream bench's `install/basic.sh`.

The loadgen records the bench-aligned signals:

| Metric                | Meaning                                                                 |
|-----------------------|-------------------------------------------------------------------------|
| `createDuration`      | t0 → last `Create()` returns. Apiserver-bound.                          |
| `setupTime`           | last create → `AttachedRoutes == N`. **This is the bench's column.**    |
| `timeToAttachedN`     | t0 → `AttachedRoutes == N`. `createDuration + setupTime`.               |
| `gatewayStatusWrites` | distinct `AttachedRoutes` values seen. Comparable to bench's "writes".  |
| `attachedSeries`      | `[(offsetMs, attachedRoutes)]`. Plot to see the convergence curve.      |

The script also extracts `/var/log/kubernetes/audit/audit.log` from the
kind control-plane node and counts status writes on
`gateway.networking.k8s.io`. This is the ground-truth side-channel for
"how many writes did Traefik actually send" independent of what the
loadgen observed.

## 3. Running the harness

**Why:** Sanity-check against a real cluster, with real apiserver
round-trips and the upstream Helm chart. Used before iterating on §2/§3
of the spec and again after each material change. **Not part of the
inner loop** — it builds the Traefik image and creates a cluster, so
each run takes minutes.

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
`rest.Config.QPS=1000/Burst=2000` by default, so the workers actually
run in parallel — without this, every worker piles up behind client-go's
default 5 QPS limiter and the wall time is the same as
`--concurrency=1`. The `-qps` / `-burst` flags on the loadgen tune this
further.

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

## 4. Why concurrency matters: the burst-regime gotcha

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

On slow hardware the upstream bench's regime is only reproduced with
`--concurrency 16`.

## 5. Measured results (N=1000)

macOS arm64, Docker Desktop, kind single-node cluster, Helm chart
configured per `hack/perf/values.yaml`. Numbers are from this branch's
HEAD before any §2/§3 work.

**Sequential creates (`--routes 1000`):**

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

**Burst creates (`--routes 1000 --concurrency 16`):**

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

## 6. Conclusion — what is confirmed

**Status I/O serialised on the rebuild goroutine is the bottleneck
under burst load.** The burst run is the definitive evidence:

- The audit log records ~1000 status updates landing on the apiserver
  over the 197s setupTime window.
- The loadgen sees `AttachedRoutes` tick only 3 times in the same
  window (1 → 20 → 1000), with a flat stretch between offsetMs=2591
  and offsetMs=198800.

The only way to produce that shape is the Gateway-status write being
stuck at the end of a long, serialised drain of per-route status writes
on the single rebuild goroutine. Route translation, hashing, lister
Gets, and equality compares run *between* writes, so they cannot
account for the 197s gap on their own — the apiserver round-trips do.

**This validates §2 (decouple status writes from the rebuild) and §3
(bounded parallelism in the writer)** of the spec:

- §2 moves the I/O off the rebuild critical path. The rebuild becomes
  in-memory work + an enqueue. The dynamic config gets reloaded almost
  immediately. The provider can keep up with the event rate.
- §3 lets the writer parallelise. With a worker pool of 8, the
  apiserver writes that today take 197s sequentially would take
  roughly 25s. With 16 workers, ~12s — within an order of magnitude
  of Cilium's 1s (the rest of the gap is hardware).

## 7. What is *not* validated

- **Steps 4 and 5 remain independent of the perf chain.** Step 4
  (foreign-parent fix) is a correctness item; the harness doesn't
  cover it. Step 5 (cheap rebuild cleanups) only matters once
  Step 2/3 have removed the I/O bulk — its return is currently ≈0.
- **Multi-replica HA leader election** (spec §8) is still out of
  scope; nothing here addresses it.
- **CI integration of a perf-regression guard** (spec §8) is still
  deferred — the kind harness is too heavy for per-PR CI.

## 8. Recommended next steps

1. Implement §2 on a new branch. Re-run the kind harness at
   `--concurrency 16`. Target: the long AttachedRoutes plateau
   collapses; the provider keeps up with the event rate.
2. Implement §3 (worker pool, default 8). Re-run with
   `--concurrency 16`. Target: `setupTime` ≤ 30s at N=1000 on the
   same kind cluster.
3. Implement §4 (foreign-parent filter). Independent of perf; gate
   on `make test-gateway-api-conformance`.
4. Re-run the harness once after §2 lands and once after §3 lands.
   Don't re-tune; we want apples-to-apples deltas. Append the new
   numbers to §5 under a fresh sub-heading; keep the pre-§2 numbers
   as the baseline.