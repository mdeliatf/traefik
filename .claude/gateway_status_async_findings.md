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

### 5b. After the (now-dropped) async writer goroutine + Gateway-first ordering design, no QPS fix

```json
{
  "routes": 1000,
  "concurrency": 16,
  "createDuration": "1.783s",
  "setupTime": "6.806s",
  "timeToAttachedN": "8.589s",
  "timeToQuiescence": "3m16.999s",
  "gatewayStatusWrites": 3,
  "httpRouteEvents": 2000,
  "attachedSeries": [
    {"offsetMs": 44,   "attachedRoutes": 1},
    {"offsetMs": 154,  "attachedRoutes": 49},
    {"offsetMs": 8589, "attachedRoutes": 1000}
  ]
}
```
audit log: 1006 status updates.

**What this shows.** The bench column (`setupTime`) drops 29× because
the Gateway-status write is now the *first* thing in the post-rebuild
flush — but `timeToQuiescence` stays at 197s because the per-route
writes still drain at 1 per 200ms, throttled by client-go's default
QPS=5 token bucket. Step 2 was solving the wrong problem.

### 5c. With QPS fix alone (`QPS=-1`), async design reverted

```json
{
  "routes": 1000,
  "concurrency": 16,
  "createDuration": "1.605s",
  "setupTime": "3.973s",
  "timeToAttachedN": "5.578s",
  "timeToQuiescence": "3.974s",
  "gatewayStatusWrites": 4,
  "httpRouteEvents": 2000,
  "attachedSeries": [
    {"offsetMs": 161,  "attachedRoutes": 2},
    {"offsetMs": 1185, "attachedRoutes": 21},
    {"offsetMs": 4311, "attachedRoutes": 696},
    {"offsetMs": 5578, "attachedRoutes": 1000}
  ]
}
```
audit log: 1008 status updates.

**What this shows.** With the client-side rate limiter disabled, the
entire 1000-write drain finishes in ~4s on the same hardware where
the pre-fix baseline was 197s. The convergence curve has four ticks
across the burst (`attachedSeries`) rather than the long flat-line
pattern, confirming that throughput is now bounded by apiserver
round-trip latency, not by a client-side bucket. `setupTime` also
beats the §5b post-Step-2 number (3.97s vs 6.8s) even though Step 2's
Gateway-first ordering trick is *not* applied here — the absence of
throttling makes the ordering trick unnecessary at this N.

### 5d. With Step 2 (QPS fix) + Step 3 (config-before-status reorder) landed

```json
{
  "routes": 1000,
  "concurrency": 16,
  "createDuration": "1.406s",
  "setupTime": "98ms",
  "timeToAttachedN": "1.504s",
  "timeToQuiescence": "2.789s",
  "gatewayStatusWrites": 3,
  "httpRouteEvents": 2000,
  "attachedSeries": [
    {"offsetMs": 56,   "attachedRoutes": 2},
    {"offsetMs": 351,  "attachedRoutes": 86},
    {"offsetMs": 1504, "attachedRoutes": 1000}
  ]
}
```
audit log: 1006 status updates.

**What this shows.** `setupTime` — the bench column — drops from
3.97s (§5c) to **98ms**, a ~40× improvement on the same hardware,
same concurrency, same N. The cause is structural, not just noise:

- In §5c the rebuild walked routes and wrote each route's status
  inline, then wrote Gateway status *last*. The rebuild couldn't
  return — and Provide couldn't push the new config — until all ~1000
  per-route round-trips had drained. `AttachedRoutes` only ticked to N
  once that whole sequence finished.
- In §5d the rebuild runs in memory only (`statusReport` is populated
  but no API calls happen); `loadConfigurationFromGateways` returns in
  µs. `Provide` pushes the config to `configurationChan` immediately,
  then `flushStatusReport` walks the report in **GatewayClass → Gateway
  → routes → policies** order. The Gateway write — which the bench
  polls — is the *first* status I/O after the rebuild, not the last,
  so `AttachedRoutes==N` lands ~100ms after the final route create.

`timeToQuiescence` also improves (3.97s → 2.789s); part of that is
run-to-run noise on `createDuration` (1.6s → 1.4s) but the rest is
that the flush is no longer interleaved with rebuild work — it's a
straight sequential drain at uncapped QPS, bounded only by apiserver
round-trip latency. `gatewayStatusWrites=3` and the 3-tick
`attachedSeries` (2 → 86 → 1000) confirm the 1-slot eventCh drop and
the post-rebuild hash dedup are still coalescing the burst into a
small handful of rebuilds, each producing one Gateway write.

audit log holds at 1006 writes — basically `1000 HTTPRoutes + 3
Gateway + 2 GatewayClass + 1 BackendTLSPolicy` — so the lean
write count from §5c is preserved. We are not buying speed by
writing more.

## 6. Conclusion — what the rebuild-vs-IO split looked like

**Pre any fix:** Status I/O serialised on the rebuild goroutine is the
visible bottleneck. Audit log records ~1000 status updates landing on
the apiserver over the 197s setupTime window; `AttachedRoutes` ticks
only 3 times (1 → 20 → 1000) with a flat stretch between
offsetMs=2591 and offsetMs=198800.

What was wrong with the original framing: the *reason* the drain took
197s was not the rebuild-goroutine coupling per se — it was that each
write was rate-limited to 1 per 200ms by client-go's default token
bucket (`QPS=5/Burst=10`). At ~5 writes/s, 1000 writes = ~200s, which
matches the measurement to the second.

Validated by the two follow-up runs (§5b, §5c):

- **§5b — async writer-goroutine design alone, no QPS fix:**
  `setupTime=6.8s`, `timeToAttachedN=8.6s`, **`timeToQuiescence=197s`**.
  Moving the Gateway-status write to the front of the flush (which
  the bench polls) dropped `setupTime` 29×, but the per-route status
  drain — the work that closes `timeToQuiescence` — stayed at 197s,
  because each write was still rate-limited at the client.
- **§5c — QPS fix alone, async design reverted:** `setupTime=3.97s`,
  `timeToAttachedN=5.58s`, **`timeToQuiescence=3.97s`**. With the rate
  limiter out of the way, all 1000 writes drain in ~4s on macOS Docker
  Desktop, and there is no need for the Gateway-first reorder to win
  the bench column — the Gateway-status write naturally lands inside
  that 4-second window too.

This **refutes** the original "async decouple + writer parallelism"
framing as the right perf fix (now spec Steps 6 and 7, dropped):

- The async design was solving a downstream symptom of the
  rate-limiter cap. With the cap removed, the architectural cost
  (extra goroutine, lifecycle, latest-wins coalescing, test-only sync
  helpers) earns almost nothing — that design alone leaves
  `timeToQuiescence` at 197s, whereas QPS alone fixes it.
- The worker-pool plan was justified by extrapolating "1000
  sequential writes is unavoidable, so parallelise" — but the *only*
  reason sequential 1000 writes took 197s was the bucket; at uncapped
  QPS, 1000 sequential writes on a kind localhost cluster is ~4s,
  well below the "parallel writer earns its keep" threshold.

## 7. Recommended next steps (updated)

1. ~~**Land Step 2 (QPS fix).**~~ Landed (commit
   `ae1a20b51`); §5c → §5d confirm no regression.
2. ~~**Land Step 3 (synchronous config-before-status reorder).**~~
   Landed; §5d shows a 40× drop in `setupTime` vs §5c on the same
   harness, same hardware, same N. The rebuild is now in-memory only;
   `flushStatusReport` walks the report in `GatewayClass → Gateway →
   routes → policies` order after the config has been published, so
   `AttachedRoutes==N` lands inside ~100ms of the last route create.
3. **Land Step 4 (foreign-parent filter).** Independent of perf; gate
   on `make test-gateway-api-conformance`. Ships in its own PR per the
   spec's PR plan.
4. Re-run the harness if you touch anything in the rebuild or flush
   path. Append numbers as §5e, §5f. A regression vs §5d on the same
   hardware and concurrency is a red flag — `setupTime > ~200ms` or
   `timeToQuiescence > ~4s` indicates the data-plane / status-write
   ordering broke.

## 8. What is *not* validated

- **CI integration of a perf-regression guard** (spec §8) is still
  deferred — the kind harness is too heavy for per-PR CI.
- **Multi-replica HA leader election** (spec §8) is still out of
  scope; nothing here addresses it.