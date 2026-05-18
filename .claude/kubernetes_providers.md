# Kubernetes providers — shared patterns

The three Kubernetes providers — `kubernetesingress`, `kubernetescrd`,
`kubernetesgateway` — share the same skeleton. This file documents the parts
that are repeated across all of them so the per-provider docs can focus on
what's actually different. When in doubt, read `pkg/provider/kubernetes/k8s/`
and one neighbouring provider before adding new patterns.

## Client construction

`newK8sClient` (each provider has its own copy) picks a `rest.Config` from one
of three sources, in this order:

1. **In-cluster** — when both `KUBERNETES_SERVICE_HOST` and
   `KUBERNETES_SERVICE_PORT` are set. Uses `rest.InClusterConfig()`. The
   provider's `Endpoint` field, if set, overrides the host.
2. **External via kubeconfig** — when `KUBECONFIG` env var is set. Uses
   `clientcmd.BuildConfigFromFlags`.
3. **External via explicit endpoint** — uses the provider's `Endpoint`,
   `CertAuthFilePath`, `Token` (bearer) config. `Endpoint` is required in this
   mode.

A `LabelSelector` is parsed and validated up front; an invalid selector fails
`Init` rather than the watch loop.

## Watch and event flow

`WatchAll(namespaces, stop)` returns a single `<-chan any` that all informers
write into through a shared `k8s.ResourceEventHandler` (`pkg/provider/kubernetes/k8s`).
The handler is type-agnostic — providers don't differentiate on event kind, only
on "something changed, rebuild."

When `namespaces` is empty, the provider switches to `metav1.NamespaceAll`
mode and sets `isNamespaceAll=true`. Two helpers bridge the difference between
the empty-string factory index and concrete namespace names:

- `lookupNamespace(ns)` — returns `""` in all-namespaces mode, otherwise `ns`.
- `isWatchedNamespace(ns)` — guards getters against namespaces the provider
  isn't watching, to avoid lister panics.

Each provider creates one or more `SharedInformerFactory` per watched namespace
(kube, gateway, secret factories are kept separate so secrets can have stricter
list options — e.g. excluding Helm-owned secrets). After registering handlers,
`Start(stop)` is called on every factory and `WaitForCacheSync` blocks until
caches are populated; a timeout aborts the watch with an error.

Resync period is 10 minutes (`resyncPeriod`), the same in every provider.

## Provide loop

`Provide(configurationChan, pool)` runs the watch under a backoff: it calls
`client.WatchAll`, optionally wraps the event channel with `throttleEvents`,
then loops — on every event it rebuilds the configuration, hashes it, and
sends on `configurationChan` only if the hash changed. Read the concrete
implementation in any provider's `kubernetes.go` (`Provider.Provide`); the
three providers are line-for-line near-identical here.

Three building blocks come from `pkg/safe` and `pkg/job`:

- `safe.OperationWithRecover` wraps the operation in a panic recovery.
- `job.NewBackOff(backoff.NewExponentialBackOff())` resets the backoff once the
  operation has been running long enough — so a fresh transient failure starts
  from a short delay rather than the long one we ended on last time.
- `backoff.RetryNotify` retries the whole `WatchAll → loop` cycle on any
  returned error, logging via the `notify` callback.

## Hash-based dedup

The loaded `dynamic.Configuration` is hashed with
`github.com/mitchellh/hashstructure` and stored in `Provider.lastConfiguration`
(`safe.Safe`). The new configuration is sent on `configurationChan` only if
its hash differs from the previous one — so cache resyncs and idempotent edits
don't trigger downstream reloads.

## Event throttling

When `ThrottleDuration > 0`, `throttleEvents(ctx, duration, pool, ch)` wraps
the event channel:

- A goroutine reads from the upstream channel and does a **non-blocking** write
  to a 1-buffer downstream channel.
- If the buffer already holds an event, the new one is **dropped** and a debug
  log line is emitted. This is safe because the loop doesn't differentiate
  events — it always rebuilds the full configuration from listers.
- After dispatching an event, the main loop also `time.Sleep`s
  `ThrottleDuration` — that's the floor between two reloads.

## Status updates

Resources with a `.Status` subresource (Gateway, GatewayClass, *Routes,
BackendTLSPolicy, IngressRoute, etc.) follow the same `Update*Status` pattern:

1. Run inside `retry.RetryOnConflict(retry.DefaultRetry, ...)` so concurrent
   writes resolve via re-fetch.
2. Get the current object from the **lister** (cached, fast).
3. Compare the new status against the cached one with a provider-specific
   `*Equal` helper; skip the API call when nothing changed.
4. Deep-copy, mutate `.Status`, call `csXxx.UpdateStatus(...)`.

When the resource type can carry per-controller status entries (`RouteParentStatus`,
`PolicyAncestorStatus`), the update **preserves** entries written by other
controllers — only entries whose `ControllerName` matches the current provider
are overwritten. This is what lets multiple gateway controllers coexist on the
same cluster.

## Conventions worth knowing

- Provider name constants live in each provider's `kubernetes.go` (`ProviderName`).
- Cross-provider service references use `<name>@<providerName>` in the backend
  name and are handled specially — they bypass cross-namespace
  ReferenceGrant/AllowExternalNameServices checks since the target isn't a K8s
  object.
- All getters validate `isWatchedNamespace` before touching the lister; calling
  them with an unwatched namespace is a bug, not an authz issue.
- Generated typed clients live in `pkg/provider/kubernetes/crd/generated/` and
  the upstream `sigs.k8s.io/gateway-api` module. Don't hand-edit either.