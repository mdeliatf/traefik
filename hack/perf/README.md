# Gateway-status perf benchmark

End-to-end reproduction of the Howard John Gateway API bench v1 "Attached
Routes" scenario against a real Kubernetes cluster, used to measure
"time-to-stable AttachedRoutes" at N HTTPRoutes attached to a single
Gateway.

See `.claude/gateway_status_async_spec.md` §4.1 for the design.

## Prerequisites

- `docker`
- `kind` ≥ 0.20
- `helm` ≥ 3.10
- `kubectl`
- `go` (matching `go.mod`)

The script builds the current HEAD into a local Docker image via
`make build-image`, then loads it into the kind cluster — there is no
network pull of `traefik/traefik:latest`. If you've already built the
image, pass `--no-build` to skip the rebuild.

## One-command run

From the repo root:

```bash
hack/perf/gateway-status-bench.sh --routes 1000
```

What it does:

1. Creates a single-node kind cluster (`traefik-perf` by default) with the
   audit policy from `audit.yaml` mounted into the apiserver.
2. Builds the local Traefik image (`make build-image` → `traefik/traefik:latest`)
   and loads it into the cluster.
3. Installs Gateway API CRDs (v1.5.1, matching the version in `go.mod`).
4. Installs Traefik via the official Helm chart with
   [`values.yaml`](./values.yaml) — `kubernetesGateway.enabled=true`,
   `throttleDuration=0`, image `pullPolicy=Never`.
5. Runs [`cmd/loadgen`](./cmd/loadgen): creates one `GatewayClass`, one
   `Gateway`, one backend `Service`, then N HTTPRoutes sequentially.
   Watches HTTPRoute status writes via an informer and waits for
   quiescence.
6. Prints a JSON report and saves it to a tempfile. Cross-checks the
   apiserver audit log for status-write counts.
7. Deletes the kind cluster.

## Flags

- `--routes N` — number of HTTPRoutes to create (default `1000`).
- `--cluster NAME` — kind cluster name (default `traefik-perf`).
- `--quiescence DUR` — no-status-write window that defines "stable"
  (default `5s`).
- `--keep` — leave the kind cluster running after the report is printed,
  for follow-up exploration (`kubectl` against the saved kubeconfig).
- `--no-build` — skip `make build-image`. Use after the first run when
  you haven't changed code, to save a few minutes.

## Output

The loadgen prints a human-readable report to stdout, e.g.:

```
Gateway-status perf benchmark report
====================================

Run
  Routes               1000
  Concurrency          1
  Namespace            traefik-perf
  GatewayClass         traefik-perf-class
  Gateway              traefik-perf-gateway

Timings
  Create duration      32.4s       (create all 1000 HTTPRoutes)
  Setup time           86.3s       (last create → AttachedRoutes=1000)
  Time to AttachedRoutes=N 118.7s  (start → AttachedRoutes=1000)
  Time to quiescence   123.7s      (start → no HTTPRoute status writes for the quiescence window)

Counters
  Gateway status writes 42
  HTTPRoute events      1042

AttachedRoutes progression (12 samples)
…
```

When `-out=<path>` is passed (the bench script always does), the same
report is also serialized as JSON to that file for machine consumption.

The script also extracts `/var/log/kubernetes/audit/audit.log` from the
kind control-plane node and counts status updates on
`*.gateway.networking.k8s.io` as an independent cross-check against
Traefik's own write counter.

Both reports land in `$TMPDIR` and are printed at the end of the run.