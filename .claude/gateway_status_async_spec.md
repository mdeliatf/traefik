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

- Move status I/O off the rebuild critical path. The rebuild's job ends
  at "configuration hashed and sent on `configurationChan`; status
  report enqueued".
- Stop writing foreign-controller `RouteParentStatus` entries.
- Reduce wall-clock "time-to-stable-status" for N routes well below the
  current baseline (target: at least an order of magnitude at N=1000).
- Preserve all existing correctness:
  - foreign-controller status entries on the *target object* are still
    merged (already handled in `client.go`).
  - The 16-ancestor cap on `BackendTLSPolicy` is still enforced.
  - The "config applied before status" ordering Traefik currently
    relies on **must not change**. This is by design and is unrelated
    to the perf work.

### Non-goals

- Changing what status content we write (only when and how).
- Changing the rebuild's "rebuild-everything-from-listers" approach.
  Hash-dedup of the dynamic config stays.
- Multi-leader / HA election logic. Traefik's gateway provider has no
  per-Gateway leader election today; we don't add it.
- Optimising the bench's other test categories ("Route Probe", "Route
  Scale", "Traffic Performance"). Scope is "Attached Routes" only.

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

Implementation:

- `flushStatusReport` partitions the report into a flat slice of
  per-resource closures, fans out via `errgroup.WithContext` (or
  `safe.Pool`) bounded to N workers.
- Conflict retry stays inside each closure.

Tests:

- Add a race condition test: two reports with overlapping resources
  flushed back-to-back; assert no apiserver conflicts surface and
  final state matches the second report.
- Layer 1 perf harness: time-to-stable improves further at N=1000.

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
- Multi-replica Traefik (HA) leader election for status writes.
  Not on the table; Traefik's gateway provider isn't multi-leader
  today.
- Per-kind queues (one writer per resource kind). kgateway uses a
  single queue with a single consumer; NGF uses group-level
  partitioning for its multi-deployment model. We have neither
  driver — single queue is the right default. Reconsider only if
  Step 4 fan-out turns out insufficient.
- Generalising the queue/writer to the `kubernetescrd` and
  `kubernetesingress` providers. Their status footprints are smaller
  and not under benchmark pressure; leave alone until they are.

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