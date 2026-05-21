# Gateway API status updates — rewrite spec

Branch: `fix/gateway-api-hackathon`
Package: `pkg/provider/kubernetes/gateway/`

**Headline finding (overrides earlier framing of this spec).** The
dominant cause of Traefik's "100× the median" setup time on the
upstream Gateway API bench is *not* the status-write ordering and not
the rebuild architecture — it is **client-go's default rate limiter
(QPS=5, Burst=10) throttling apiserver writes to ~5/s**. With both
Step 2 (client-side rate limiter disabled, `QPS=-1`) and Step 3
(synchronous config-before-status reorder) landed, the bench numbers
for N=1000 are `setupTime=98ms`, `timeToAttachedN=1.504s`,
**`timeToQuiescence=2.789s`** on macOS Docker Desktop — better than
every prior result here, ~1800× faster than the upstream bench's
published Traefik row (180s), and squarely inside the median of the
upstream peers on much slower hardware. See
[`gateway_status_async_findings.md`](./gateway_status_async_findings.md)
§5d for the run output and the regression story across §5a–§5d.

Implications for this spec:

- **The QPS fix (Step 2) is the dominant lever.** Landed in commit
  `ae1a20b51`; collapses `timeToQuiescence` from 197s to ~4s by
  itself. Everything else builds on top of it.
- **The synchronous config-before-status reorder (Step 3) is the
  bench-column win.** Landed. The rebuild now runs in memory only
  (`statusReport` collects writes; no API calls) and `flushStatusReport`
  drains in `GatewayClass → Gateway → routes → policies` order after
  the config has been pushed to `configurationChan`. The Gateway
  write — which the bench polls — is now the *first* status I/O after
  the rebuild, so `AttachedRoutes==N` lands ~100ms after the last
  route create instead of ~4s (§5c → §5d, a ~40× drop).
- **Step 3 follow-up — `statusReport` now accumulates per-parent and
  per-ancestor entries instead of clobbering by resource key.** Landed
  on the same branch. The initial Step 3 implementation stored
  `map[NamespacedName]HTTPRouteStatus` (and the same shape for the
  other route kinds plus `BackendTLSPolicy`), so when the rebuild
  visited the same (route, parent) or (policy, ancestor) from more
  than one call path in a single pass — most notably BackendTLSPolicy
  written from inside `loadHTTPServers`, which is reached once per
  (route × parent × listener × backend) combination — every write
  overwrote the previous entry's single-element slice. Net effect:
  only the *last* iteration's parent/ancestor survived in the
  persisted status. The upstream conformance test
  `BackendTLSPolicy/HTTP_request_sent_to_Service_with_valid_BackendTLSPolicy_should_succeed`
  failed because of this: the policy was referenced from two
  HTTPRoutes (HTTP listener `web` and HTTPS listener `websecure`),
  the HTTPS iteration ran last and clobbered the HTTP ancestor, the
  conformance harness was looking for the HTTP-listener ancestor with
  `Accepted=True`, never found it, and gave up after its 60 s poll
  deadline (the trailing rate-limiter error in the failure message
  was a polling side-effect, not the cause). Fix: report entries are
  now `map[NamespacedName][]RouteParentStatus` (for the four route
  kinds) and `map[NamespacedName][]PolicyAncestorStatus` (for
  BackendTLSPolicy), with `record*` helpers that upsert by
  `ParentRef`/`AncestorRef` identity — last-write-wins applies *per
  distinct ref*, never across refs. Gateway and GatewayClass are still
  struct-valued because each gets exactly one atomic write per
  rebuild.
- **The async writer-goroutine design (Step 6) is dropped.** It was
  solving a downstream symptom of the rate-limiter cap; once Step 2
  lifts the cap, the architecture earns nothing. `timeToQuiescence`
  with that step alone was 197s; with Steps 2 + 3 it is 2.8s.
- **Bounded writer parallelism (Step 7) is dropped.** No peer
  implementation does this; with QPS uncapped the floor is already
  `apiserver_round_trip × N`, which is a few seconds on a localhost
  cluster — well below the threshold where a worker pool would earn
  its keep.

Status: Steps 1, 2, 3 landed (Step 3 includes the slice-accumulation
follow-up above). `make test-gateway-api-conformance` passes on the
current branch tip. Step 4 (foreign-parent fix) and Step 5 (cheap
rebuild cleanups) still pending.

**PR plan:** Steps 1, 2, 3, 5 ship together in one PR; Step 4
(foreign-parent fix) ships separately in a follow-up PR. Steps 6 and 7
are dropped — kept in this spec only as a historical record of what
we tried and why we backed it out.

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

> **Status:** the measured truth is recorded in
> [`gateway_status_async_findings.md`](./gateway_status_async_findings.md).
> In short: **status I/O serialised behind the rebuild goroutine is the
> dominant cost** (1000 status writes drained over 197s while the
> AttachedRoutes counter only ticked 3 times). The original "O(N²)
> per-event rebuild" framing was **wrong** — the 1-slot input channel
> at `client.go:130` does a non-blocking send (`k8s/event_handler.go:34-39`)
> and silently drops events that arrive while the consumer is mid-rebuild,
> so a burst of N route creates does not produce N rebuilds. The
> `attachedSeries` from the burst run shows only 3 ticks (1 → 49 →
> 1000), consistent with a small handful of rebuilds, each visiting
> the routes then-present in the lister cache.

Confirmed contributors to the pre-Step-2 setupTime:

- Status writes are sequential on the rebuild goroutine — the apiserver
  round-trips dominate, not in-process work.
- Gateway status is written **last** in `loadConfigurationFromGateways`
  (`kubernetes.go:430`), behind the per-route writes — the bench
  measures `AttachedRoutes`, so it sees the tail.
- Backend resolution inside `loadHTTPRoute` (`httproute.go:82`,
  `kubernetes.go:881-968`) reads from the shared-informer indexed
  caches; sub-µs per call. Not a meaningful contributor at N=1000.

### Reference implementation comparison

Three projects were cited:

- **kgateway** (`pkg/kgateway/proxy_syncer/status_syncer.go`): one
  `utils.AsyncQueue[reports.ReportMap]`, latest-wins. A dedicated
  goroutine dequeues and calls `syncGatewayStatus`,
  `syncListenerSetStatus`, `syncRouteStatus`, `syncPolicyStatus` in a
  fixed Gateway-first order. **Each phase is a sequential `for` loop
  over its resources** — no per-resource parallelism. `retry-go` handles
  conflict retries.

- **Contour** (`internal/status/cache.go`): the DAG processor builds a
  per-kind cache flushed by a separate `StatusUpdater` over a buffered
  channel. **Single consumer goroutine; writes are sequential.** Routes
  before gateways in the flush order. Computation and I/O are decoupled
  but not parallelised.

- **NGF** (`internal/controller/status/`,
  `internal/controller/handler.go:150`): a dedicated goroutine started
  with `go handler.waitForStatusUpdates(cfg.ctx)` is the only one that
  calls `Updater.Update`. **Internally sequential** (`for`-loop with
  exponential backoff). Routes-then-Gateway group ordering, motivated by
  an IP-only fast path, not by AttachedRoutes-latency. NGF's
  `updater.go` doc-comment acknowledges the sequential calls as a known
  issue (FIXME #1014) but they have not changed it.

All three share the **decouple-from-event-loop** pattern. **None
parallelise per-resource writes within the writer.** The earlier
characterisation that "kgateway and Contour parallelise" was incorrect.
This is the basis for parking Step 3 (see §5).

## 3. Goals & non-goals

### Goals

- **Move status I/O off the rebuild critical path.** *Why:* under
  burst load the rebuild loop is dominated by `UpdateStatus`
  round-trips serialised on the event-loop goroutine (the kind harness
  shows 1000 status writes draining over 197s while AttachedRoutes
  only ticks 3 times). The rebuild can't return — and the dynamic
  config can't ship — until they all finish.
- **Schedule Gateway/GatewayClass writes ahead of route writes
  inside a flush.** *Why:* the bench measures convergence on
  `Gateway.AttachedRoutes`. If that one tiny write sits behind
  1000 HTTPRoute writes, the bench number doesn't move even after
  the rebuild loop is fixed.
- **Stop writing foreign-controller `RouteParentStatus` entries.**
  *Why:* causes ping-pong with Envoy Gateway and inflates the
  apiserver write rate in steady state.
- **Cut "time-to-stable-status" for N=1000 to ≤30s** on the kind
  harness. *Why:* clears the next-worst bench peer (NGF, 29s) — we
  stop being the outlier. **Achieved with margin:** post-Steps-2+3
  burst-mode (c=16, macOS Docker Desktop) `setupTime=98ms`,
  `timeToAttachedN=1.504s`, **`timeToQuiescence=2.789s`** — ~10×
  under target, ahead of every reported peer on the bench's table.
  See [`gateway_status_async_findings.md`](./gateway_status_async_findings.md)
  §5d.

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
- **`statusReport` preserves every distinct parent/ancestor.** A
  resource visited from multiple call paths within one rebuild ends
  up with one entry per distinct `ParentRef`/`AncestorRef` in its
  persisted status — never collapsed to whichever loop iteration
  ran last. Enforced by `upsertRouteParent` and
  `recordBackendTLSPolicyAncestor` in `status.go`. The Gateway API
  spec requires this for BackendTLSPolicy (one ancestor per
  `(Gateway, listener)` pair the policy applies to); it is also the
  natural shape for `RouteParentStatus` when a route has multiple
  `parentRefs`.

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

One harness, built **before** any production code changes: a
kind-based end-to-end script that drives Traefik installed from the
upstream Helm chart with the bench's `install/basic.sh` values.

### 4.1 Kind harness

Location: `hack/perf/gateway-status-bench.sh` plus a small Go program
under `hack/perf/cmd/loadgen/`.

Script does:

1. `kind create cluster --config hack/perf/kind.yaml`.
2. `make build-image` to build a local Traefik image at the current
   HEAD, then `kind load docker-image traefik:dev`.
3. Install Gateway API CRDs (`experimental-install.yaml`, pinned).
4. `helm install traefik` with values from `hack/perf/values.yaml` —
   minimal: one replica, `kubernetesGateway` provider on,
   `ThrottleDuration=0` (matches the bench), default resources.
5. Run the loadgen: create one `GatewayClass`, one `Gateway`, then N
   HTTPRoutes. Watch all HTTPRoutes' status; record time-to-stable,
   per-kind write counts, and the AttachedRoutes convergence series.
6. Report (JSON).

`hack/perf/kind.yaml` is a single-node kind config. Audit policy in
`hack/perf/audit.yaml` enables logging of `update` verbs on
`*.gateway.networking.k8s.io` for cross-checking the write count
against what Traefik thinks it sent.

Run before the work starts (to establish a baseline matching the
upstream bench's regime) and again after each material change to
confirm the rewrite holds up. It builds an image and creates a
cluster, so it isn't part of the inner iteration loop.

### 4.2 Conformance gate

`make test-gateway-api-conformance` already exists. It runs the
upstream Gateway API conformance suite. It is correctness-only — won't
catch perf regressions — but it is the right safety net for the
foreign-parent fix and any change to status content. Run it as the
final gate before each PR in this series.

## 5. Plan of work

Ordered. PR grouping is in the headline. Every step starts with "red
test fails", except Step 1 which is pure infrastructure.

### Step 1 — Build the harness, profile, agree on the bottleneck

Deliverables:

- `hack/perf/...` — kind harness and loadgen.

Exit criteria:

- The harness reproduces a "Traefik is slow at N=1000" result against
  master on the developer's machine, in the same shape as the upstream
  bench's published row (long flat AttachedRoutes plateau, then a
  single jump to N at the very end).
- We agree on **where the time actually goes** — status I/O serialised
  on the rebuild goroutine — before writing any production code.

If the harness shows the bottleneck is *not* status-update
orchestration (e.g., it's hashing the configuration, or some other
rebuild work), we revisit this whole spec.

### Step 2 — Disable client-go's default rate limiter (THE perf fix)

**Single-line change, dominant perf win, prerequisite for everything
else.** All other steps in earlier drafts of this spec combined moved
`setupTime` from 197s to 6.8s; this one moves it from 197s to 3.97s
*with the old async-rewrite reverted*, and collapses
`timeToQuiescence` from 197s to 3.97s.

What:

```go
func createClientFromConfig(c *rest.Config) (*clientWrapper, error) {
    c.QPS = -1
    c.RateLimiter = nil
    // ...rest unchanged
}
```

Why `-1` and not "a big number":

- client-go's `rest.New` skips creating a token-bucket rate limiter
  entirely when `QPS < 0` (see
  `staging/src/k8s.io/client-go/rest/config.go`). Any positive value
  still imposes bucket bookkeeping and a finite ceiling.
- This is what controller-runtime does by default
  (`pkg/client/config/config.go:101-104`): "Disable client-side
  ratelimer by default, we can rely on API priority and fairness."
  NGF inherits this through `ctlr.GetConfigOrDie()`; we got
  client-go's stock defaults (`QPS=5/Burst=10`) by virtue of building
  the `rest.Config` by hand and never setting these fields.
- `c.RateLimiter = nil` is defensive: if `InClusterConfig` or
  `BuildConfigFromFlags` ever stashes a default limiter on the
  config, clearing it lets the `QPS=-1` setting actually take effect.

Why force, not guard with `if c.QPS == 0`:

- Traefik exposes no surface for an operator to set `QPS` on the
  gateway provider's `rest.Config`. A guard would be dead code.
- We are a controller; we want APF-backed backpressure, not a
  hardcoded client-side bucket. If the apiserver is in trouble, APF
  will throttle us on the server side — that's the right place.

Tests:

- No unit test needed beyond the existing client construction tests
  (which will continue to pass).
- Kind harness regression check: `setupTime` and `timeToQuiescence`
  for N=1000 must remain in the single-digit seconds.

### Step 3 — Publish config before status writes (synchronous reorder)

**The one good idea from the earlier async-rewrite draft.** No
goroutine, no queue, no latest-wins. Same goroutine as today, just
one statement reordered after a small refactor.

Current shape: `loadConfigurationFromGateways` writes every status
*inline* during the rebuild walk (GatewayClass at the top, route
statuses interleaved with route loading, Gateway status at the end —
`kubernetes.go:355,420` and inside each route loader). After all of
that returns, `Provide` hashes the config and sends it to
`configurationChan`. **Result:** the data plane only learns about new
routes after every status write has landed.

New shape:

1. `loadConfigurationFromGateways` collects status writes into an
   in-memory `*statusReport` (a struct of `map[NamespacedName]Status`
   per kind, populated as the rebuild walks listers) and returns
   `(*dynamic.Configuration, *statusReport)`. No `client.Update*Status`
   calls during the rebuild.
2. `Provide` does, in order, on the same goroutine:
   - `conf, report := p.loadConfigurationFromGateways(ctx)`
   - hash + dedup as today
   - on change: `configurationChan <- dynamic.Message{...}` *first*
   - then `p.flushStatusReport(ctx, report)` — sequential `for` loop
     over the report, calling the existing `client.Update*Status`
     methods, in the order GatewayClass → Gateway → routes → policies.

Why synchronous (not async):

- The bench shows the floor is already `apiserver_round_trip × N`
  (~4s for N=1000 on kind) once the client-side rate limiter is out
  of the way. There is no headroom for an async writer to claw back.
- Async introduces a writer-goroutine lifecycle, latest-wins
  coalescing, shutdown semantics, test-only sync helpers — none of
  which earn their keep at current perf.
- Synchronous keeps the test contract intact: tests that read status
  immediately after `loadConfigurationFromGateways` returns continue
  to see the written status, because the flush ran on the same
  goroutine before `Provide` loops back to the next event.

Why config-first inside `Provide`:

- The data plane should start serving the new routes the moment the
  configuration is ready. Today it can't, because the rebuild
  goroutine is still doing ~1000 sequential status writes before
  `Provide` reaches the `configurationChan <- ...` line.
- Status writes are observation, not authorisation: K8s does not
  require Gateway status to be written before traffic flows.

Refactor scope (one PR):

- New file `status.go`: `statusReport` struct, `newStatusReport()`,
  `(p *Provider).flushStatusReport(ctx, report)`. Pure data + a flush
  function; no goroutine, no channel.
- `kubernetes.go`: `loadConfigurationFromGateways` returns the report;
  status-write calls inside it become map assignments; `Provide`
  publishes the config then calls `flushStatusReport`.
- `httproute.go`, `grpcroute.go`, `tlsroute.go`, `tcproute.go`: route
  loaders take `*statusReport` and append to it instead of calling
  `client.UpdateXxxRouteStatus`. Same change inside `loadHTTPServers`
  for BackendTLSPolicy.

Tests:

- Existing assertions on `UpdateXxxStatus` call sequences keep working
  because the flush is still synchronous within `Provide`. The unit
  tests that drive `loadConfigurationFromGateways` directly need to
  call `flushStatusReport` themselves (or be updated to assert against
  the returned report).
- Kind harness re-run: `setupTime` and `timeToQuiescence` should not
  regress; the primary improvement is perceived-latency on data-plane
  updates, which the harness does not currently observe.

#### Step 3 follow-up — accumulate per-(parent, ancestor) entries

Caught by `make test-gateway-api-conformance` after the initial
Step 3 commit landed: `BackendTLSPolicy/HTTP_request_sent_to_Service_with_valid_BackendTLSPolicy_should_succeed`
timed out at the conformance harness's 60 s polling deadline.

Root cause: the first `statusReport` design keyed the route and
BackendTLSPolicy maps by `NamespacedName` and stored the full
`*RouteStatus` / `PolicyStatus` (each containing a slice of
parents/ancestors) as the value. `loadHTTPServers` is called once per
`(route × parent × listener × backend)` combination, and on each call
it computed a single-entry `Ancestors` slice and assigned it to the
map — i.e., each write threw away whatever the previous iteration had
recorded for that policy. In the failing test the `normative-test`
policy was referenced by two HTTPRoutes (`backendtlspolicy` on the
`web` listener, `backendtlspolicy-reencrypt` on the `websecure`
listener); the websecure iteration ran last, the persisted status
listed only that ancestor, the conformance harness expected the
`web`-listener ancestor with `Accepted=True`, never found it, and the
rate-limiter error in the failure message was just the harness's
client running out of polling budget — symptom, not cause.

(In the pre-Step-3 inline-write world the same logic existed inside
`loadHTTPServers`, but each call hit the apiserver directly, and the
test happened to pass because the harness sometimes raced the
intermediate write where the `web` ancestor was the latest. Step 3
made the last-write-loses semantics deterministic — every time, only
the last iteration survived — which is what surfaced the bug.)

Fix (one commit):

- `statusReport.{http,grpc,tcp,tls}Routes` are now
  `map[NamespacedName][]RouteParentStatus`.
- `statusReport.backendTLSPolicies` is now
  `map[NamespacedName][]PolicyAncestorStatus`.
- New helpers `record{HTTP,GRPC,TCP,TLS}RouteParent` and
  `recordBackendTLSPolicyAncestor`, plus a shared `upsertRouteParent`
  / `parentRefEquals`, append or replace entries by
  `ParentRef`/`AncestorRef` identity (last-write-wins applies only
  *within* the same ref, never across refs).
- The four route loaders no longer accumulate a local
  `parentStatuses` slice and write the whole route at the end of the
  parent loop — each parent is recorded into the report directly.
- `flushStatusReport` wraps each slice into the appropriate
  `*RouteStatus` / `PolicyStatus` before calling the existing
  `client.UpdateXxxStatus`.
- Gateway and GatewayClass are left struct-valued (one atomic write
  per rebuild; their internal slices — `Listeners`, `Conditions`,
  `SupportedFeatures` — are built in one shot inside the rebuild
  itself, not from multiple call paths).

Verification: `make test-gateway-api-conformance` passes on the
current branch tip.

### Step 4 — Foreign-parent correctness fix

Independent of the perf chain. Filtering foreign parents only reduces
the status footprint further, so it can't degrade the post-Step-2/3
metrics.

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

### Step 6 — Decouple status writes from the rebuild (DROPPED)

> **Dropped.** The async writer-goroutine + 1-slot latest-wins queue +
> Gateway-first write ordering design was built to amortise a per-write
> cost that turned out to be artificial (client-go default `QPS=5/Burst=10`).
> Once Step 2 removed the rate limiter, the same N=1000 bench that
> motivated this step closes `timeToQuiescence` in ~4s on the same
> hardware where this step alone left it at 197s. The added complexity
> (extra goroutine, lifecycle, test-only sync helpers, latest-wins
> coalescing) buys nothing in this regime.
>
> The one good idea — push the dynamic configuration to the
> configuration channel *before* the status writes — survives in
> Step 3, done synchronously on the rebuild goroutine.
>
> Full prior design (writer goroutine, latest-wins semantics, shutdown
> handling, two-phase worker pool) is in git history at commit
> `4184ef6a8` if we ever need to revive it (e.g., if a future
> apiserver-side bottleneck makes inline writes block the rebuild loop
> measurably).

### Step 7 — Bounded parallelism within the writer (DROPPED)

> **Dropped** for the same reason as Step 6. None of the three
> reference implementations (kgateway, Contour, NGF) parallelise
> per-resource status writes; they are all single-writer-goroutine
> sequential. Once the client-side rate limiter is out of the way
> (Step 2), the wall time of N sequential writes is
> `N × apiserver_round_trip`, which on a kind cluster is already a few
> seconds at N=1000. A worker pool would put us ahead of the field
> without a measured need.

## 6. Risks & rollback

- **Risk (Step 2):** uncapping client-side QPS pushes more load onto
  the apiserver in pathological cases (e.g., a misconfigured cluster
  with no APF, or a route-status thrash loop). *Mitigation:* the
  apiserver's own API Priority and Fairness (APF) is the right place
  to backpressure controllers; client-side caps mask the real problem.
  If a real incident materialises, an operator-tunable QPS knob can
  be added then.

- **Risk (Step 3):** reordering config-send ahead of status-write
  changes the timing contract — an external observer (a conformance
  test, a CI script) that polls "config applied implies status
  written" might race. *Mitigation:* the conformance suite tolerates
  eventual consistency; in-tree tests that assert immediate status
  visibility either drive `loadConfigurationFromGateways` + call
  `flushStatusReport` directly, or use a poll/wait helper.

- **Rollback:** each step lands as its own commit, so a single
  destabilising step can be reverted without touching the others.
  Steps 6 and 7 are dropped — nothing to roll back.

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
   The kind harness must reproduce this: install Traefik with
   throttling off.

2. **Bench produces one informer event per route create (resolved).**
   Pulled `origin/v1:tests/attached-routes.sh` and
   `tests/attachedroutes/attachedroutes.go`. The test drives
   `github.com/howardjohn/pilot-load` to create HTTPRoutes via a
   cluster simulator. Each route is a separate apply with a
   `--gracePeriod` delay between them (unset by default = 0). So:
   - Creates are sequential, one apiserver request per route.
   - No batching; each create produces its own informer event.

   The kind harness defaults `--concurrency=1` to match. The flag
   stays in the loadgen so we can later test "what if creates were
   batched" as a separate experiment.

### Still open

3. **Does the foreign-parent fix interact with `ReferenceGrant` paths?**
   `parentRef` is always a Gateway, never a cross-kind ref, so the
   ReferenceGrant logic in the route loaders (which guards
   `backendRef` and listener `certificateRef`) is not affected. To
   double-check during Step 4 implementation.


### Process

4. **Profiling artifacts are not committed.** Any profile captured
   from a one-off run against the kind harness is shared out-of-band
   (e.g., attached to the PR review or pasted via flamegraph
   screenshot). Do not add `.pb.gz` files to the branch.

## 8. Deferred (TODO list, do not pursue now)

- **Referenced-set predicate on informer events.** NGF skips wake-up
  entirely when an event lands on a Service/EndpointSlice/Secret/
  ConfigMap/Namespace that the current graph does not reference
  (`internal/controller/state/change_processor.go:182-211`,
  `store.go:284-336`). Traefik's equivalent today is the post-build
  hash compare (`kubernetes.go:228-240`), which suppresses the
  downstream config push but still pays for the rebuild. The 1-slot
  drop on `eventCh` (`client.go:130`, `k8s/event_handler.go:34-39`)
  already absorbs bursts, so the practical pre-Step-2 cost was bounded
  — but in steady-state churn on unrelated resources we still rebuild
  needlessly. Worth considering only if profiling shows steady-state
  rebuild cost matters; deferred until we have data.
- Wiring a perf-regression guard into CI. The kind harness is too
  heavy for per-PR CI (builds an image, brings up a cluster). The
  natural shape is a focused envtest unit test that asserts rebuild
  wall time at N=small stays under a threshold after Step 2 lands;
  defer until there's a stable post-rewrite baseline to gate on.
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