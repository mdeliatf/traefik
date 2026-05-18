# Kubernetes Gateway API provider

Package: `pkg/provider/kubernetes/gateway/`.
Provider name: `kubernetesgateway`. Controller name: `traefik.io/gateway-controller`.

This provider watches Kubernetes [Gateway API](https://gateway-api.sigs.k8s.io/)
resources and produces a Traefik `dynamic.Configuration`. The lifecycle,
client construction, throttling, hash-dedup and status-update conventions are
the same as the other K8s providers — see [`kubernetes_providers.md`](./kubernetes_providers.md).
What follows is what's specific to Gateway API.

## The big picture

Every event on any watched resource triggers a full rebuild via
`loadConfigurationFromGateways`. The rebuild is deterministic — it reads only
listers, never the API directly — and is shaped by three loops:

1. **GatewayClass loop.** Accept classes with our `ControllerName`, write
   `Accepted=True` + the supported-features list (`features.go` /
   `SupportedFeatures`) back to status.
2. **Gateway loop.** For each Gateway pointing at an accepted class,
   `loadGatewayListeners` produces a flat `[]gatewayListener` from
   `spec.listeners`, resolves TLS certs into `conf.TLS.Certificates`, and
   pre-fills per-listener `ListenerStatus`.
3. **Route loops.** `loadHTTPRoutes`, `loadGRPCRoutes`, `loadTLSRoutes`, and
   (only when `ExperimentalChannel` is on) `loadTCPRoutes` each walk their
   routes, attach to matching listeners, translate the route into routers and
   services, and write `RouteParentStatus` back.

A final pass calls `makeGatewayStatus` per Gateway to synthesise the
gateway-level `Accepted` / `Programmed` conditions from listener conditions
and persist them.

The two structures that tie everything together:

- `gatewayListener` — a flattened, normalised view of one `spec.listeners[i]`:
  port, protocol, TLS, hostname, the entry point it resolved to, its allowed
  namespaces and route kinds, and an `Attached` boolean set once everything
  about the listener checks out. Route loaders see this view, not the raw
  Gateway resource.
- `dynamic.Configuration` — the output. HTTP and GRPC routes contribute to its
  HTTP section; TLS and TCP routes contribute to its TCP section; listener
  certs contribute to its TLS section.

## Listener attachment

The interesting part of `loadGatewayListeners` is what can disqualify a
listener. Each failure path writes a `ListenerCondition` and leaves `Attached
= false`, which prevents any route from attaching but still lets the listener
appear in status:

- **No matching entry point.** `entryPointName` does a suffix match of the
  listener's `:port` against the configured `EntryPoints[*].Address`. For
  `HTTPProtocolType` it skips entry points that already have a TLS config —
  HTTP and HTTPS must live on different entry points. Failure → `PortUnavailable`.
- **Bad TLS combination.** TLS must be **absent** for HTTP/TCP listeners and
  **present** for HTTPS/TLS ones. `Passthrough` is allowed for `TLS` but not
  for `HTTPS` (you can't terminate at L7 without terminating TLS first).
  Passthrough with `CertificateRefs` is tolerated with a warning — the certs
  are simply ignored.
- **Unsatisfied cert refs.** Each `CertificateRef` must be a `core/Secret`.
  Cross-namespace refs require a `ReferenceGrant` (see below). The Secret must
  carry non-empty `tls.crt` / `tls.key`.
- **Conflict with another listener on this gateway.**
  `makeListenerKey(listener) = protocol|hostname|port`; duplicates emit
  `Conflicted=True / DuplicateListener`.
- **`TCPProtocolType` without `ExperimentalChannel`** — the only place where
  the experimental flag affects listener acceptance.

Allowed routes are computed in two steps. `supportedRouteKinds` returns the
hard-coded set per protocol:

- HTTP / HTTPS → HTTPRoute + GRPCRoute
- TLS → TLSRoute
- TCP → TCPRoute (gated)

`allowedRouteKinds` then intersects that with the listener's
`AllowedRoutes.Kinds` (if any) and surfaces unsupported entries as
`InvalidRouteKinds`. Allowed *namespaces* come from `allowedNamespaces`:
`Same` (default), `All`, or `Selector` (label-selecting via
`client.ListNamespaces`).

A listener that survives all of the above flips `Attached = true`, and its
loaded certs are merged into `conf.TLS.Certificates` (sorted by
`namespace/name` so the hash is stable).

## ReferenceGrant

`isReferenceGranted(fromKind, fromNs, toGroup, toKind, toName, toNs)` short-
circuits to "granted" when the from/to namespaces match. Otherwise it lists
`ReferenceGrants` in the **target** namespace and looks for at least one whose
`spec.from` mentions our `(group=gateway.networking.k8s.io, kind=fromKind, namespace=fromNs)`
**and** whose `spec.to` matches `(group, kind)` (plus `name` when the grant
narrows it). Empty intersection → "missing ReferenceGrant" error which becomes
a `RefNotPermitted` condition on the offending listener or route.

This guards:

- Listener `CertificateRef` resolution.
- Every cross-namespace `BackendRef` in HTTPRoute / GRPCRoute / TLSRoute /
  TCPRoute. Cross-*provider* refs (`TraefikService` with an `@provider` suffix)
  bypass this on purpose — those aren't K8s objects.

## How a route attaches

For every route kind the pattern is the same — they all use the helpers
`matchingGatewayListeners`, `matchListener`, `allowRoute`, and
`findMatchingHostnames`.

`matchingGatewayListeners(listeners, routeNs, parentRefs)` is the first cut:
keep only listeners whose parent Gateway (by group/kind/namespace/name)
appears in the route's `parentRefs`. The default `Group` is
`gateway.networking.k8s.io` and the default `Kind` is `Gateway`.

For each `parentRef`, the loader then walks those candidate listeners and
applies three increasingly specific checks:

1. `matchListener` — does the `parentRef`'s `SectionName` / `Port` (if set)
   match this listener?
2. `allowRoute` — is this route's kind in `AllowedRouteKinds`, and is its
   namespace in `AllowedNamespaces`?
3. `findMatchingHostnames` — intersect the route hostnames with the listener
   hostname, treating `*.foo.com` as a wildcard that swallows `bar.foo.com`.

Two subtleties matter:

- Acceptance bumps `Listener.Status.AttachedRoutes` **even when** the listener
  is not `Attached` (i.e. it has unresolved refs). The spec requires accurate
  attachment counts regardless of programming state.
- The translated configuration is only merged into the final `conf` when both
  `accepted == true` **and** `listener.Attached == true`. So a route can show
  `Accepted=True` while contributing nothing to routing — that's intentional,
  status reflects intent, configuration reflects what's safe to program.

Conditions are layered: every `parentRef` starts at
`Accepted=False / NoMatchingParent` and gets upgraded by `updateRouteConditionAccepted`
as listeners check out. `upsertRouteConditionResolvedRefs` is sticky — a
single `False` wins over later `True`s for the same `parentRef`.

## HTTPRoute translation

The output is HTTP routers + services + middlewares. Each `spec.rules[ri]`
maps to one router per `match`, plus one weighted service per rule (unless the
backend is a single `@internal` `TraefikService`, in which case the router
points at it directly).

**Rule building.** `buildMatchRule` composes the Traefik v3 rule and a
priority. The components and their priority contributions:

- Path: exact `Path("v")` → `+100000`; prefix `(Path("/v") || PathPrefix("/v/"))`
  → `+10000 + len(path)*100`; regex `PathRegexp("v")` →
  `+10000 + len(path)*100`. Catch-all `PathPrefix("/")` is forced to priority
  `1` so it loses against everything.
- Method → `+1000`; each header → `+100`; each query param → `+10`.
- Host (`buildHostRule`): plain hostname → `Host("h")`; wildcard `*.foo.com`
  → a `HostRegexp` that matches one label. Adds `len(longest hostname)` to
  priority.
- Final tie-breaker: `+ (len(rules) - ri)` so earlier rules in the same
  HTTPRoute win.

`RuleSyntax` is hard-coded to `"default"` (v3). HTTPS listeners attach an
empty `RouterTLSConfig{}` to the router (terminate, no passthrough). The CRD
provider can register a `RouterTransform` via `Provider.SetRouterTransform`;
when present it gets a final pass over each built router to add
Traefik-specific fields the spec doesn't cover.

**Filter mapping.** `loadMiddlewares` walks `routeRule.Filters`:

- `RequestHeaderModifier`, `ResponseHeaderModifier` — straight maps to the
  same-named Traefik middleware (`Set`/`Add`/`Remove`).
- `RequestRedirect` — `RequestRedirect` middleware. Default status code is
  302; `replacePrefixMatch` uses the rule's path match value as the prefix.
- `URLRewrite` — `URLRewrite` middleware; same prefix-from-path-match rule.
- `ExtensionRef` — resolved through `groupKindFilterFuncs`. The CRD provider
  plugs `traefik.io/Middleware` into this registry, so HTTPRoutes can
  reference Traefik `Middleware` CRDs by name.
- Anything else — error. The rule's service is replaced with a 500-response
  WRR (`invalid-httproute-filter`), so the rest of the route still loads.

**Backend mapping.** `loadService` handles each `backendRef`:

- Non-`core/Service` kind ⇒ `loadHTTPBackendRef`. Recognises
  `TraefikService` with `@<provider>` (cross-provider, e.g. `api@internal`),
  otherwise dispatches via `groupKindBackendFuncs` (again, the CRD provider
  registers `TraefikService` here).
- `core/Service` ⇒ build a `ServersLoadBalancer` via `loadHTTPServers`. Port
  is mandatory. Backend addresses come from `getBackendAddresses` (see below).
- A matching `BackendTLSPolicy` (`ListBackendTLSPoliciesForService`, sorted by
  creation timestamp then name) produces a `ServersTransport`. Only the first
  match wins; further policies get `PolicyConditionAccepted=False / Conflicted`
  written to their status. With a `ServersTransport` the backend protocol is
  forced to `https`.
- Without a `ServersTransport`, `getHTTPServiceProtocol` picks the scheme
  from the service port: port 443 or name `https…` → `https`; `appProtocol`
  `http` / `kubernetes.io/ws` → `http`, `https` / `kubernetes.io/wss` →
  `https`, `kubernetes.io/h2c` → `h2c`.
- Multiple backends are wrapped in a `WeightedRoundRobin` named
  `<routerName>-wrr`. Backends that errored out stay in the WRR with
  `Status=500` so the WRR keeps its weight distribution.

**`BackendTLSPolicy` → `ServersTransport`.** `loadServersTransport` builds
the transport from `policy.Spec.Validation`: `ServerName` is the policy's
hostname, `RootCAs` are the contents of the referenced ConfigMap/Secret's
`ca.crt`, and `WellKnownCACertificates=true` skips the CA refs entirely.

## Endpoint discovery

`getBackendAddresses` is shared by all route kinds:

1. Reject `Spec.Type == ExternalName` — Traefik doesn't follow CNAMEs for K8s
   services.
2. Locate the `ServicePort` by `ref.Port`.
3. Read `traefik.io/service.nativeLB` from the **service's** annotations (the
   provider's `NativeLBByDefault` sets the default). When true, skip
   EndpointSlices and use `service.Spec.ClusterIP` as the single backend
   (rejects empty / `None`).
4. Otherwise list `EndpointSlices` and pick Ready endpoint addresses from
   slice ports whose `Name` matches the `ServicePort` name. Duplicates are
   removed.

This is the only place in the provider where annotations matter — see the
section below.

## GRPCRoute translation

Same skeleton as HTTPRoute, but:

- Rule is built from `buildGRPCMatchRule`: method + headers + host, no path
  priorities. `GRPCMethodMatch` becomes `PathRegexp("/<service>/<method>")`
  with `[^/]+` placeholders for the unset side. Empty `Matches` defaults to
  `PathPrefix("/")`.
- Filter support is narrower: `RequestHeaderModifier` and `ExtensionRef` only.
  Unsupported filters produce a WRR with a synthetic
  `GRPCStatus{Code: codes.Unavailable}` server named `invalid-grpcroute-filter`
  — gRPC clients prefer that to an HTTP 500.
- Backend kinds are limited to `core/Service` — there's no
  `groupKindBackendFuncs` indirection here.
- Backend protocol defaults to `h2c`; `https` is the only override
  (`getGRPCServiceProtocol`).

## TLSRoute translation

TLSRoutes produce **TCP** routers because the request is SNI-routed before any
HTTP framing exists. The rule is `hostSNIRule(hostnames)` (regex-wrapped
wildcards) and `router.TLS.Passthrough` mirrors the listener's TLS mode —
terminate or pass through.

The notable detail: when at least one TLSRoute attaches, the loader appends a
catch-all `deny-unknown-host` TCP router with rule

```text
HostSNI(`*`) && !ALPN(`h2`) && !ALPN(`http/1.1`)
```

priority `1`, and an empty-servers service. Without it, unrouted SNIs on a
shared entry point would fall through to HTTPS listeners (whose routers also
listen on TCP under the hood). The ALPN guards make sure the deny-rule only
catches genuine TLS-only traffic, not HTTP traffic that happens to hit the
same entry point.

## TCPRoute translation

Gated behind `ExperimentalChannel`. TCPRoutes produce TCP routers with rule
`HostSNI("*")` — there's nothing in TCP to match on. When the listener is
`TLS`, the TCP router gets `RouterTCPTLSConfig{Passthrough: ...}` from the
listener's TLS mode; for raw `TCP` listeners no TLS config is set.

Backend kinds are limited to `core/Service` and cross-provider
`TraefikService@<provider>`. Errored backends become empty
`TCPServersLoadBalancer`s — at the TCP layer there's no "return 500"
equivalent, the connection simply closes.

## Annotations

Only one annotation is recognised, and only on backend **Services**:

- `traefik.io/service.nativeLB=true` — use the service ClusterIP as the
  single backend (see "Endpoint discovery").

`Provider.NativeLBByDefault` flips the default for services that don't set
the annotation. `convertAnnotations` rewrites `traefik.io/...` keys before
decoding through `label.Decode` with prefix `traefik.service.`, which is why
the annotation looks like a "service.nativeLB" sub-key. No route- or
gateway-level annotations exist.

## Extension hooks

Two registries let other providers (in practice the CRD provider, via
`FillExtensionBuilderRegistry`) plug their kinds in:

- `RegisterFilterFuncs(group, kind, BuildFilterFunc)` — called from
  `HTTPRoute.spec.rules[i].filters[j].extensionRef`. The CRD provider
  registers `traefik.io/Middleware` here, returning a fully-qualified
  middleware name (`<ns>-<name>@kubernetescrd`).
- `RegisterBackendFuncs(group, kind, BuildBackendFunc)` — called from
  HTTPRoute backends with a non-`core/Service` kind. The CRD provider
  registers `traefik.io/TraefikService`.

The hooks return a name plus an optional `*dynamic.Middleware` /
`*dynamic.Service`; when the dynamic object is nil the route just references
an existing object by name (the common case for both CRD-registered hooks).

## Feature reporting

`SupportedFeatures` (a `sync.OnceValue`) is the canonical list of Gateway API
conformance features Traefik advertises. It's reported in every accepted
`GatewayClass`'s status. The composition:

- **Core** — all of `GatewayCoreFeatures`, `HTTPRouteCoreFeatures`,
  `ReferenceGrantCoreFeatures`, `BackendTLSPolicyCoreFeatures`,
  `GRPCRouteCoreFeatures`, `TLSRouteCoreFeatures`.
- **Extended Gateway** — `GatewayPort8080` only.
- **Extended HTTPRoute** — query-param & method matching, port/scheme/path
  redirects, host/path rewrites, response header modification, backend
  protocol h2c & websocket, destination port matching, backend request header
  modification.
- **Extended TLSRoute** — `TLSRouteModeTerminate`, `TLSRouteModeMixed`.

When adding support for a new Gateway API feature, add the corresponding
constant here so conformance tooling notices.

## Editing checklist

- Use **symbol names**, not line numbers, when documenting future changes —
  line numbers in this directory rot quickly.
- `ExperimentalChannel` gates both the informer (`client.go`) **and** the
  loader call (`kubernetes.go`). Flipping one without the other will silently
  miss TCPRoute events.
- Don't switch `RuleSyntax` away from `"default"` — the v2 syntax was removed
  from Traefik v3.
- Status-update code paths preserve foreign-controller entries on
  `RouteParentStatus` and `PolicyAncestorStatus`. If you add a new status
  type, copy that pattern; the multi-controller story breaks otherwise.
- `BackendTLSPolicy` ancestor statuses are spec-capped at 16. If you add a
  new ancestor source, merge or drop accordingly.
- Fixtures under `fixtures/<routekind>/*.yml` are the authoritative test
  inputs; expected configurations are built inline in `*_test.go`. Add a
  fixture before changing translation behaviour so regressions are caught.