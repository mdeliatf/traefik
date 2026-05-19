# Gateway API status updates — async rewrite spec

Branch: `fix/gateway-api-hackathon`
Package: `pkg/provider/kubernetes/gateway/`
Status: Step 1 landed (Layer 1 + Layer 2 + instrumentation). Step 1
findings — measured results, harness runbook, what's confirmed vs not —
are in [`gateway_status_async_findings.md`](./gateway_status_async_findings.md).
Steps 2 and 3 are validated by the data and ready to implement.

## 1. Context

The Howard John Gateway API benchmark v1 ([README](https://github.com/howardjohn/gateway-api-bench/blob/v1/README.md))
measured how each implementation behaves when 1000 HTTPRoutes are created
against a single Gateway. The "Attached Routes" test produced these results:

| Metric        | Traefik (v35.3.0 chart) | Cilium | Istio | Kgateway | Kong  | Nginx | EnvoyGW |
|---------------|-------------------------|--------|-------|----------|-------|-------|---------|
| Setup time    | **180s**                | 1s     | 2s    | 12s      | 1s    | 29s   | 23s     |
| Teardown time | 16s                     | 18s    | 20s   | 16s      | 22s   | 15s   | 45s     |
| Writes        | 73                      | 193    | 311   | 323      | 1403  | 82    | 1383    |

Two distinct problems in the Traefik row:

1. **Setup time is ~100× the median.** The work is sequential and pinned to
   the event-loop goroutine that also produces the dynamic configuration.
2. **Foreign-parent ping-pong with Envoy Gateway.** Traefik writes a
   `RouteParentStatus{ControllerName: traefik.io/gateway-controller}` for
   *every* `parentRef` on a Route, including parents pointing at Gateways it
   doesn't own. Envoy Gateway sees a foreign entry on a Gateway it does own
   and overwrites it; Traefik rewrites it on its next rebuild; loop ensues.

Bench v2 dropped Traefik from testing — we have no newer external numbers,
so we are responsible for our own measurement. Nothing material has changed
in the status-update paths between v3.x (chart `v35.3.0`) and master
(`v3.7`), so the v1 results are taken as still representative.

## 2. Current implementation (what we're changing)

Status writes are inlined into the rebuild path:

```
Provide loop
└── loadConfigurationFromGateways(ctx)
    ├── for each GatewayClass:
    │       client.UpdateGatewayClassStatus(...)            ← API write
    ├── loadGatewayListeners() (per Gateway, in-memory)
    ├── loadHTTPRoutes() — for each route:
    │       client.UpdateHTTPRouteStatus(...)               ← API write
    ├── loadGRPCRoutes / loadTLSRoutes / loadTCPRoutes:
    │       client.UpdateGRPCRouteStatus(...) etc.          ← API writes
    ├── BackendTLSPolicy match (inside HTTPRoute loader):
    │       client.UpdateBackendTLSPolicyStatus(...)        ← API write
    └── for each Gateway:
            client.UpdateGatewayStatus(...)                 ← API write
```

Inside `clientWrapper.UpdateXxxStatus`:

1. `retry.RetryOnConflict(retry.DefaultRetry, fn)`.
2. `fn` does: lister `Get` → `xxxEqual(...)` short-circuit → `DeepCopy` →
   `csGateway.GatewayV1().Xxx().UpdateStatus(...)`.
3. The equality helpers (`conditionsEqual`, `routeParentStatusesEqual`,
   `gatewayStatusEqual`, `policyAncestorStatusesEqual` — `client.go:782-864`)
   correctly ignore `LastTransitionTime`, so no-op rebuilds don't hit the
   API. The "73 writes for 1000 routes" data point confirms the dedup
   works; the cost is upstream of the write.

### What we believe the cost actually is

> **Status:** this section is the **pre-measurement hypothesis** that
> motivated the harness in §4. The measured truth is recorded in
> [`gateway_status_async_findings.md`](./gateway_status_async_findings.md)
> — in short, status I/O (97% of rebuild wall time under burst load) is
> the dominant cost, not O(N²) iteration. The bullets below are kept as
> a record of the reasoning that led to the harness design.

Because dedup works, the API writes themselves can't account for 180s.
The hypothesis (to be verified by profiling, see §4) is:

- **Bench creates N routes one at a time.** Each `kubectl apply` is a
  separate informer event. By default `ThrottleDuration=0` in the provider
  (`kubernetes.go:71`), so each event triggers a full rebuild.
- **Per rebuild, we iterate every route.** The k-th route create produces a
  rebuild that visits all k previously-loaded routes plus the new one.
  Aggregate work over N creates is O(N²).
- **Inside the rebuild, every visited route does a lister `Get` and an
  equality compare.** Cheap individually, but the rebuild blocks both the
  dynamic-config send *and* the next event's processing.
- **All of this happens on a single goroutine.** No parallelism.

Profiling will confirm or refute this before we commit to the design.

### Reference implementation comparison

Three projects were cited:

- **kgateway** (`pkg/kgateway/proxy_syncer/status_syncer.go`): one
  `utils.AsyncQueue[reports.ReportMap]`, latest-wins. The translator
  enqueues a complete report. A dedicated goroutine in `Start(ctx)`
  dequeues and calls `syncGatewayStatus`, `syncRouteStatus`,
  `syncListenerSetStatus`, `syncPolicyStatus`. `retry-go` handles conflict
  retries. Manager-level leader election gate.

- **Contour** (`internal/status/cache.go`): the DAG processor builds a
  `Cache` via accessor functions (`RouteConditionsAccessor` etc.) that
  return a builder plus a commit closure. After DAG processing the cache
  is flushed to the API by a separate `StatusUpdater`. Computation and
  I/O are fully decoupled.

- **NGF** (`internal/controller/status/`,
  `internal/controller/handler.go:150`): a dedicated goroutine started
  with `go handler.waitForStatusUpdates(cfg.ctx)` is the only one that
  calls `Updater.Update` (which is internally synchronous). Event loop
  pushes to `statusQueue` and never blocks on the API. NGF's own
  `updater.go` doc-comment admits the within-writer sequential calls are
  a known issue (FIXME #1014).

All three share the **decouple-from-event-loop** step. They differ on
whether they also parallelise within the writer (kgateway and Contour:
yes; NGF: not yet).

## 3. Goals & non-goals

### Goals

- **Move status I/O off the rebuild critical path.** *Why:* 97% of
  rebuild wall time today is `UpdateStatus` round-trips (Layer 1
  measurement). The rebuild can't return — and the dynamic config
  can't ship — until they all finish.
- **Schedule Gateway/GatewayClass writes ahead of route writes
  inside a flush.** *Why:* the bench measures convergence on
  `Gateway.AttachedRoutes`. If that one tiny write sits behind
  1000 HTTPRoute writes, the bench number doesn't move even after
  the rebuild loop is fixed.
- **Stop writing foreign-controller `RouteParentStatus` entries.**
  *Why:* causes ping-pong with Envoy Gateway and inflates the
  apiserver write rate in steady state.
- **Cut "time-to-stable-status" for N=1000 to ≤30s** on Layer 2.
  *Why:* clears the next-worst bench peer (NGF, 29s) — we stop
  being the outlier.

### Correctness invariants (must not regress)

- Foreign entries on the *target object* are still merged
  (`client.go:566-570`).
- 16-ancestor cap on `BackendTLSPolicy` still enforced
  (`httproute.go:493`, `httproute.go:525`).
- `AttachedRoutes` value stays exact — reordering is at write
  time, not compute time. Counter increments at
  `httproute.go:75`, `grpcroute.go:72`, `tlsroute.go:69`,
  `tcproute.go:66` happen during the rebuild before Gateway
  status is built.
- Config-send happens before status-write. Today implicit; after
  the rewrite explicit (rebuild ends → config sent → writer
  flushes async).

### Test impact

One unit test reads gateway status synchronously after
`loadConfigurationFromGateways` returns:
`TestGatewayClassLabelSelector` (`kubernetes_test.go:71-110`).
Needs a `WaitForStatusFlush` helper or `require.Eventually` after
Step 2. The other eight callers of `loadConfigurationFromGateways`
only inspect the returned `*dynamic.Configuration` and are
unaffected.

### Non-goals

- Changing *what* status content we write (only when and how).
- Changing the "rebuild-everything-from-listers" approach.
- Multi-leader / HA election (see §8).
- Optimising the bench's other test categories.

## 4. Reproduction & measurement

Two layers; both built **before** any production code changes.

### 4.1 Layer 1 — in-process micro-bench (primary tool)

Location: `pkg/provider/kubernetes/gateway/perf_test.go` (and a small
shared harness in `pkg/provider/kubernetes/gateway/perfharness_test.go`).

Tech:

- `sigs.k8s.io/controller-runtime/pkg/envtest` — runs real `etcd` +
  `kube-apiserver` binaries on localhost. Test-only dep (already a
  transitive dep of the upstream Gateway API repo; verify before
  pulling in).
- The actual `kubernetesgateway.Provider` constructed exactly as
  production does, against the envtest config.
- Gateway API CRDs loaded from the `sigs.k8s.io/gateway-api` module's
  YAML (we already depend on the module).

What the harness does:

1. Start envtest, install CRDs, create a namespace.
2. Create a single `Gateway` + `GatewayClass` pointing at our controller.
3. Construct the provider; start `Provide` against a captured
   `configurationChan`.
4. In a separate goroutine, create N HTTPRoutes at a configurable rate
   (default: as fast as the apiserver accepts them, single-threaded).
5. Watch the apiserver via a separate informer for HTTPRoute status
   updates; record:
   - Timestamp of each Route's first `Accepted=True` parent status.
   - Total `UpdateStatus` API write count (counted by wrapping the
     `clientWrapper` with an `*atomic.Int64` per kind, or by an
     audit hook on the envtest apiserver — whichever is cleaner).
   - Rebuild count and per-rebuild duration (via a small instrument
     hook in `loadConfigurationFromGateways`).
6. Wait until the apiserver has not seen a status write for X seconds
   ("quiescence"). Report:
   - Time-to-stable from "last route created".
   - Total writes per kind.
   - Number of rebuilds.
   - Mean / p99 rebuild duration.
   - Optional: CPU profile via `runtime/pprof` if a flag is set.

What it is *not*:

- Not a Go `Benchmark` function — wall-clock-dominated and dependent
  on the apiserver, so `testing.B`'s iteration scaling is misleading.
  It's an ordinary `Test*` function gated by a `-perf` flag, with a
  configurable `N` (env var or test flag).
- Not gated in CI. Manual invocation only:
  `go test -run=TestStatusPerf -perf -routes=1000 ./pkg/provider/kubernetes/gateway/`

Configurability:

- `-routes=N` (default 1000) — total routes to create.
- `-route-batch=N` (default 1) — routes created per `kubectl apply`
  equivalent. Useful for separating "many events" from "many routes".
- `-cpuprofile=path` — write pprof CPU profile.
- `-quiescence=duration` (default 3s) — time-without-writes that
  defines "stable".

Instrumentation hooks needed (added in production code, but no-op when
not wired):

- `clientWrapper` exposes a `Metrics` struct with atomic counters per
  `Update*Status` method. Compiled in always; cheap.
- `Provider.loadConfigurationFromGateways` calls a `time.Now()` once at
  entry/exit if a `rebuildHook` field is set (nil by default).

### 4.2 Layer 2 — kind-based end-to-end script (final gate)

Location: `hack/perf/gateway-status-bench.sh` plus a small Go program
under `hack/perf/cmd/loadgen/`.

Script does:

1. `kind create cluster --config hack/perf/kind.yaml`.
2. `make image` to build a local Traefik image at the current HEAD,
   then `kind load docker-image traefik:dev`.
3. Install Gateway API CRDs (`experimental-install.yaml`, pinned).
4. `helm install traefik` with values from
   `hack/perf/values.yaml` — minimal: one replica, `kubernetesGateway`
   provider on, `ThrottleDuration=0` (matches the bench), default
   resources.
5. Run the loadgen: create one `GatewayClass`, one `Gateway`, then N
   HTTPRoutes. Watch all HTTPRoutes' status; record same metrics as
   Layer 1.
6. Report.

`hack/perf/kind.yaml` is a single-node kind config. Audit policy in
`hack/perf/audit.yaml` enables logging of `update` verbs on
`*.gateway.networking.k8s.io` for cross-checking the write count
against what Traefik thinks it sent.

Layer 2 is used **once before** the work starts (sanity-check that
Layer 1 numbers track real-cluster numbers) and **once after** each
material change to confirm the in-process numbers carry over. Not part
of the iteration loop.

### 4.3 Conformance gate

`make test-gateway-api-conformance` already exists. It runs the
upstream Gateway API conformance suite. It is correctness-only — won't
catch perf regressions — but it is the right safety net for the
foreign-parent fix and any change to status content. Run it as the
final gate before each PR in this series.

## 5. Plan of work

Ordered, each independently shippable as a PR. Every step starts with
"red test fails", except step 1 which is pure infrastructure.

### Step 1 — Build the harness, profile, agree on the bottleneck

Deliverables:

- `pkg/provider/kubernetes/gateway/perf_test.go` (Layer 1).
- `hack/perf/...` (Layer 2).
- `clientWrapper` metrics counters (always-on, cheap).
- `loadConfigurationFromGateways` timing hook (no-op unless set).

Exit criteria:

- Layer 1 reproduces a "Traefik is slow at N=1000" result against
  master, in seconds-to-minutes wall time on the developer's machine.
- pprof flame graph captured and shared. We agree on **where the time
  actually goes** before writing any production code.

If profiling shows the bottleneck is *not* status-update orchestration
(e.g., it's hashing the configuration, or some other rebuild work), we
revisit this whole spec.

### Step 2 — Decouple status writes from the rebuild

The heart of the perf rewrite. **One** status-writer goroutine fed by
**one** 1-slot latest-wins channel of "status reports". Same shape as
kgateway's `AsyncQueue[ReportMap]`.

**Design decision — single global queue, not per-kind:**

- The rebuild always reads complete state from listers and emits a
  full `statusReport` covering every kind. There is no scenario where
  a rebuild produces a partial report that needs to merge with an
  older partial report from a different kind.
- The K8s API treats each resource's `.status` as an independent
  subresource. There is no ordering requirement between
  `GatewayClass.status`, `Gateway.status`, and `*Route.status` — a
  later route status update does not depend on an earlier gateway
  status update having landed.
- NGF's group-level partitioning (`groupGateways` vs
  `groupAllExceptGateways`) exists because NGF runs multiple Gateway
  *deployments* and routes gateway-specific updates differently from
  the rest. Traefik has a single gateway provider with no such split,
  so the driver for partitioning doesn't exist here.
- A single queue keeps the latest-wins semantics trivially correct:
  if rebuild N+1 arrives while N is still being flushed, replacing
  the pending slot is safe because N+1 is a strict superset of N's
  intent (both are full snapshots).
- If profiling after Step 3 shows one slow kind starves the others
  inside a single flush, the right answer is to parallelise within
  the flush (already part of Step 3), not to split into per-kind
  queues. We revisit only if that proves insufficient.

**Coalescing & backpressure — latest-wins is per-key by construction:**

The 1-slot replace channel and the full-snapshot `statusReport`
together give per-key coalescing for free, with no separate
`map[key]closure` dedup layer. Why this matters and why it's
sufficient:

- A burst of M rebuilds in quick succession enqueues at most 1
  pending report at any time; all but the latest are dropped at the
  slot. This is also the **natural backpressure mechanism**: the
  queue can't grow unbounded because there is only ever one slot, so
  a slow/wedged apiserver cannot make the producer memory-spike.
- Each report is a complete snapshot computed from current lister
  state, so dropping intermediate reports never loses information.
  The next snapshot is the union of every dropped report's intent
  (and possibly more). "Latest wins" is *strict*: dropping is
  monotone in the staleness sense, not lossy.
- Because the writer is single-goroutine and the next flush only
  starts after the previous one returns, there are never two
  concurrent in-flight writes to the same object (even with the
  Step 3 worker pool, which parallelises *within* a flush). So
  `RetryOnConflict` inside `UpdateXxxStatus` only ever races
  against external writers (Envoy Gateway, manual `kubectl edit`)
  — the same conditions it handles today. No new conflict modes
  are introduced.
- The per-object diff inside `UpdateXxxStatus` (`client.go:573`)
  remains the final layer that suppresses no-op API calls when the
  computed status equals the stored status. Unchanged by this
  rewrite.

**Write ordering within a flush — Gateway/GatewayClass first:**

Within a single `flushStatusReport`, write `GatewayClass` and
`Gateway` statuses *before* the route statuses. Why this rule
exists:

- The bench's setupTime metric polls
  `Gateway.status.listeners[*].AttachedRoutes`. Current code writes
  Gateway status at the very end of `loadConfigurationFromGateways`
  (`kubernetes.go:430`), after all per-route writes — exactly why the
  counter converges last today.
- There is no API dependency in the reverse direction: a route's
  status write does not require the Gateway's status to have landed
  first. The K8s subresource API treats each as independent.
- Once Step 3 (worker pool) lands, "ordering" becomes "priority
  scheduling": Gateway/GatewayClass closures are submitted to the
  worker pool ahead of route closures so they occupy the first
  batch of workers, not the last. With a pool of 8 and K=1000
  routes, this keeps the Gateway-status write off the tail of the
  flush.

**Shutdown — drop pending writes, do not drain:**

When the writer's context is cancelled, any pending (un-flushed)
report is dropped and an in-flight flush returns at the next
per-resource closure boundary; in-flight apiserver calls are
abandoned. Why drop, not drain:

- Draining can block shutdown indefinitely if the apiserver is
  slow, throttled, or unreachable. `Provide` is expected to return
  promptly on context cancel; status I/O is best-effort and must
  not gate teardown.
- On restart, the provider re-runs
  `loadConfigurationFromGateways` from scratch and produces the
  same content from the listers — so dropping the in-flight report
  costs at most one rebuild cycle of staleness, never permanent
  loss.
- The diff inside `UpdateXxxStatus` (`client.go:573`) means that if
  a write *did* land before shutdown, the post-restart rebuild
  will detect it as a no-op and skip the redundant API call. So we
  don't pay for double-writes either.

Data model — a `statusReport` is a snapshot of *all* statuses we
intend to write after this rebuild:

```go
type statusReport struct {
    gatewayClasses    map[string]gatev1.GatewayClassStatus            // by name
    gateways          map[ktypes.NamespacedName]gatev1.GatewayStatus
    httpRoutes        map[ktypes.NamespacedName]gatev1.HTTPRouteStatus
    grpcRoutes        map[ktypes.NamespacedName]gatev1.GRPCRouteStatus
    tlsRoutes         map[ktypes.NamespacedName]gatev1.TLSRouteStatus
    tcpRoutes         map[ktypes.NamespacedName]gatev1alpha2.TCPRouteStatus
    backendTLSPolicies map[ktypes.NamespacedName]gatev1.PolicyStatus
}
```

Producer side (the rebuild):

- `loadConfigurationFromGateways` no longer calls `client.Update*Status`.
- The route loaders append to a `statusReport` instead. Existing
  signatures change to accept a `*statusReport`.
- After the rebuild ends and the configuration is hashed and queued,
  `Provider.statusQueue.Submit(report)` is called.
- `statusQueue` is a 1-slot replace channel: on submit, drop any
  pending report and replace it with the new one. (Implementation:
  `chan *statusReport` of buffer 1 with non-blocking send + drain.)

Consumer side:

```go
func (p *Provider) runStatusWriter(ctx context.Context) {
    for {
        select {
        case <-ctx.Done():
            return
        case r := <-p.statusQueue:
            p.flushStatusReport(ctx, r)
        }
    }
}
```

`flushStatusReport` walks the report and calls the existing
`clientWrapper.Update*Status` methods. Per-resource sequential at
first (matches NGF today). The existing equality dedup and conflict
retry stay where they are.

Lifecycle:

- The writer goroutine is started from `Provider.Provide`, scoped to
  the same `safe.Pool` / `ctx` as the rebuild loop.
- Shutdown: closing `ctx` drains and exits. A pending report at exit
  is dropped — restart will produce the same content from listers.

Tests:

- Existing `kubernetes_test.go` tests that assert specific
  `UpdateXxxStatus` call sequences will need to wait for the
  writer goroutine to drain. Add a `WaitForStatusFlush(t)` helper.
- A new test: enqueue two reports back-to-back; assert the second
  overwrites the first (only the second's content lands).
- Layer 1 perf harness re-run: time-to-stable drops to comparable
  numbers with NGF (29s for 1000 routes is the next-worst peer; aim
  to clear that bar).

### Step 3 — Bounded parallelism within the writer

Once Step 2 is in, the per-resource writes are still sequential
inside the writer. Parallelise with a small worker pool. Default
8 workers, configurable via a private const for now (no public
API).

Implementation — **two-phase flush, not one flat pool**:

1. **Phase 1 — GatewayClass and Gateway statuses.** Written first,
   before any route status. Count is small (handful of objects);
   sequential or a 2-worker pool, either is fine.
2. **Phase 2 — route + BackendTLSPolicy statuses.** Started only
   after Phase 1 returns. Fanned out via `errgroup.WithContext`
   (or `safe.Pool`) bounded to N workers (default 8).

Why phased, not "flat pool with submission ordering": O2 in §3
("AttachedRoutes converges ahead of route status") is a
*guarantee* the design has to make, not a behaviour we hope the
scheduler delivers. In a flat 8-worker pool, submission ordering
puts Gateway/GatewayClass in the first batch — but a slow Gateway
write that hits a 409-retry, or a future change that adds more
Gateway-level work, can let route closures grab workers and
starve the Gateway write back into the tail. A two-phase split
makes the ordering structural: Phase 2 cannot start until Phase 1
returns, so AttachedRoutes is always visible before any route
status lands. No measurement required to know this holds.

Conflict retry stays inside each closure. Because the writer is
single-goroutine across flushes (see Step 2's coalescing notes),
two workers never race on the same object; the only 409s seen are
from external writers, which `RetryOnConflict` already handles.

Observability — per-resource failure must be visible:

Today, when a status write fails, the error is logged on the
rebuild goroutine right next to the route it relates to, so a
human reading the logs can immediately see which object is
broken. After decoupling, the writer is the only goroutine with
that context — but the per-resource closure has `(kind,
namespace, name)` in scope, so we can log a structured
diagnostic with no extra plumbing:

```go
log.Ctx(ctx).Warn().
    Err(err).
    Str("kind", "HTTPRoute").
    Str("namespace", ns).
    Str("name", name).
    Msg("status write failed")
```

Why this matters as part of Step 3, not a follow-up:

- Without per-object logging, a status write that fails
  repeatedly leaves the object's status stale forever and the
  failure is invisible — the rebuild loop happily continues, the
  data plane is fine, and the only symptom is "the
  conformance/test suite sees stale status weeks later".
- The retry-conflict path inside `UpdateXxxStatus` already
  swallows transient 409s; the log line above only fires for
  the terminal error after retries are exhausted, so it isn't
  spammy.
- A counter metric
  (`traefik_gateway_status_write_failures_total{kind=…}`) is the
  obvious Prometheus follow-up — deferred to §8 — but the log
  line is the minimum bar and is free given the closure's scope.

Tests:

- Add a race condition test: two reports with overlapping resources
  flushed back-to-back; assert no apiserver conflicts surface and
  final state matches the second report.
- Layer 1 perf harness: time-to-stable improves further at N=1000,
  and AttachedRoutes timestamp lands within the first 1/8 of the
  flush wall time (i.e., is not pinned to the tail).
- Failure-path test: stub `UpdateHTTPRouteStatus` to return a
  permanent error for one specific route; assert the warn log line
  is emitted with `kind=HTTPRoute name=that-route`, and no other
  routes are affected.

### Step 4 — Foreign-parent correctness fix

Independent of the perf rewrite, deliberately landed after it so the
perf numbers are judged against current (buggy) behaviour first.
Filtering foreign parents only reduces the status footprint further,
so it can't degrade the post-rewrite metrics.

Algorithm:

```
At the top of loadConfigurationFromGateways, after we collect
`gateways` (the slice of *gatev1.Gateway we own), build:

  ourGateways := map[ktypes.NamespacedName]struct{}{}
  for _, gw := range gateways {
      ourGateways[ktypes.NamespacedName{Namespace: gw.Namespace, Name: gw.Name}] = struct{}{}
  }

Pass it down to each route loader (loadHTTPRoutes, loadGRPCRoutes,
loadTLSRoutes, loadTCPRoutes) — either via the existing context or
by adding a field to `gatewayListener` so the loader can resolve
"is this parentRef one of ours".

In each loader's `for _, parentRef := range route.Spec.ParentRefs`
loop, skip the parentRef if it does NOT resolve to a Gateway in
`ourGateways`. Resolution:
  - parentRef.Group defaults to "gateway.networking.k8s.io"
  - parentRef.Kind defaults to "Gateway"
  - parentRef.Namespace defaults to route.Namespace
If Group/Kind aren't (gateway.networking.k8s.io, Gateway), skip too.
```

Strict filter, as agreed — no leniency for "Gateway we'd watch but
haven't seen yet". A parentRef we don't recognise is a parentRef
some other controller owns; we leave it alone.

Tests:

- `httproute_test.go` (and the other three): a route with two
  parentRefs, one ours one foreign; assert exactly one
  `RouteParentStatus` is produced and its `ParentRef` matches our
  Gateway.
- A route with only a foreign parentRef: assert we don't even call
  `UpdateHTTPRouteStatus`.
- Existing tests pass unchanged.
- `make test-gateway-api-conformance` passes.

### Step 5 — Cheap rebuild cleanups (optional, low-priority)

- Defer `metav1.Now()` for `LastTransitionTime` until after the
  equality check has decided we're actually writing. Today it's
  called eagerly. Cheap to change.
- Audit each `Update*Status` call site for redundant work that the
  equality check ends up throwing away.

Tests: regression-only. Status content unchanged.

## 6. Risks & rollback

- **Risk:** decoupling status from rebuild changes the timing
  contract — an external observer (a conformance test, a CI script)
  that polls "config applied implies status written" might race.
  *Mitigation:* the conformance suite tolerates eventual consistency;
  any in-tree test that asserts immediate status visibility uses a
  poll/wait helper.

- **Risk:** the latest-wins queue means an intermediate rebuild's
  status is never written. *Mitigation:* every rebuild reads the
  full state from listers; the next report is always self-contained,
  so dropping an earlier report only delays convergence by one tick.

- **Risk:** the writer goroutine outlives the rebuild loop on panic.
  *Mitigation:* both are scoped to the same `safe.OperationWithRecover`
  pool used by `Provide`.

- **Rollback:** each step is a single PR.
  - If Step 2 (decouple) destabilises, revert that PR — Step 1
    (harness) still gives us a repeatable measurement.
  - If Step 3 (parallelism) destabilises, revert it — Step 2 keeps
    the bulk of the perf win.
  - Step 4 (foreign-parent fix) is independent of the perf chain;
    revertable on its own.

## 7. Open questions

Resolved findings are recorded inline; only items still requiring
work-time investigation remain "open".

### Resolved

1. **Throttle default in the bench: OFF (resolved).** Pulled
   `origin/v1:install/basic.sh` from the bench repo. The Traefik
   install is:

   ```yaml
   providers:
     kubernetesGateway:
       enabled: true
   gateway:
     enabled: false
   ports:
     web:
       port: 80
   podSecurityContext:
     sysctls:
     - name: net.ipv4.ip_unprivileged_port_start
       value: "0"
   ```

   No `throttleDuration` is set, so the provider runs with the
   zero-value default from `kubernetes.go:71` — i.e., no throttling.
   The O(N²) "one rebuild per route create" hypothesis is preserved.
   Layer 2 must reproduce this: install Traefik with throttling off.

2. **Bench produces one informer event per route create (resolved).**
   Pulled `origin/v1:tests/attached-routes.sh` and
   `tests/attachedroutes/attachedroutes.go`. The test drives
   `github.com/howardjohn/pilot-load` to create HTTPRoutes via a
   cluster simulator. Each route is a separate apply with a
   `--gracePeriod` delay between them (unset by default = 0). So:
   - Creates are sequential, one apiserver request per route.
   - No batching; each create produces its own informer event.

   Layer 1 should default to `-route-batch=1` to match. The flag
   stays in the harness so we can later test "what if creates were
   batched" as a separate experiment.

### Still open

3. **Does the foreign-parent fix interact with `ReferenceGrant` paths?**
   `parentRef` is always a Gateway, never a cross-kind ref, so the
   ReferenceGrant logic in the route loaders (which guards
   `backendRef` and listener `certificateRef`) is not affected. To
   double-check during Step 4 implementation.

### Process

4. **pprof artifacts are not committed.** Profiles captured from
   Layer 1 are shared out-of-band (e.g., attached to the PR review
   or pasted via flamegraph screenshot). Do not add `.pb.gz` files
   to the branch. `hack/perf/` and any envtest harness output paths
   should be gitignored.

## 8. Deferred (TODO list, do not pursue now)

- Wiring Layer 1 into CI as a perf budget. Today the budget would
  be flaky (envtest startup variance dominates). Revisit if/when
  envtest startup gets predictable, or when we have a stable
  baseline for the post-rewrite numbers.
- **Multi-replica Traefik (HA) leader election for status writes.**
  *What:* elect one replica to own status writes for a given
  Gateway; others compute reports but don't flush. *Why deferred:*
  Traefik's gateway provider isn't deployed multi-replica with
  status-conflict awareness today, so the symptom doesn't bite. But
  once it is: every replica watches the same resources, every
  replica rebuilds, and every replica enqueues writes.
  `RetryOnConflict` masks the correctness issue — the slower writer
  just retries on 409 — but apiserver write load is multiplied by
  replica count, the audit log fills with redundant updates, and
  every transition between replicas writes "Accepted" again with a
  fresh `LastTransitionTime`. The data plane stays active-active;
  only status writes need a leader. *Trigger to revisit:* the chart
  adds an HA default, or a user reports apiserver-load issues from
  running ≥2 replicas.
- Generalising the queue/writer to the `kubernetescrd` and
  `kubernetesingress` providers. Their status footprints are smaller
  and not under benchmark pressure; leave alone until they are.
- **Informer resync amplification.** Every `resyncPeriod` the
  shared informers re-fire one event per cached object, and the
  provider runs a full rebuild per event. Today this is invisible
  — rebuild compute is ~52ms at N=1000 and the diff inside
  `UpdateXxxStatus` suppresses the redundant API writes, so neither
  the apiserver nor the user notices. Pre-existing behaviour,
  unchanged by this spec. Worth fixing only if N grows large enough
  (~50k+ objects) that the recompute itself becomes noticeable,
  or if a CRD change makes per-object compute more expensive.
- **Per-kind / per-object failure metrics**
  (`traefik_gateway_status_write_failures_total{kind=…}`). Step 3
  adds the structured log line so a human reading logs can spot
  failing objects. The Prometheus counter is the natural follow-up
  so dashboards can alert without grepping logs; deferred only
  because the log line covers the immediate observability gap and
  the metric needs the usual review on naming/cardinality before it
  ships.

## 9. References

- Howard John bench v1 — `~/dev/others/gateway-api-bench/README.md`
  (this repo on disk only; the `main` branch of the upstream repo has
  removed Traefik). Use `origin/v1` for the historical install
  scripts.
- NGF status package —
  `~/dev/others/nginx-gateway-fabric/internal/controller/status/`
  (queue.go, updater.go, leader_aware_group_updater.go);
  event-handler glue at
  `~/dev/others/nginx-gateway-fabric/internal/controller/handler.go`
  (`waitForStatusUpdates`, `updateStatuses`).
- Contour status cache —
  `~/dev/others/contour/internal/status/cache.go`.
- kgateway status syncer —
  `~/dev/others/kgateway/pkg/kgateway/proxy_syncer/status_syncer.go`.
- Internal: `.claude/gateway_api_provider.md`, `.claude/kubernetes_providers.md`.