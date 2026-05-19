# Gateway-status perf benchmark (Layer 2)

End-to-end reproduction of the Howard John Gateway API bench v1 "Attached
Routes" scenario against a real Kubernetes cluster. Used as a sanity check
that Layer 1's in-process envtest numbers (see
`pkg/provider/kubernetes/gateway/perf_test.go`) track real-cluster numbers.

See `.claude/gateway_status_async_spec.md` §4.2 for the design.

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
3. Installs Gateway API CRDs (v1.5.1, same version as Layer 1).
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

The loadgen prints a JSON report like:

```json
{
  "routes": 1000,
  "createDuration": "32.4s",
  "timeToStable": "118.7s",
  "statusEventCount": 1042,
  "namespace": "traefik-perf",
  "gatewayClass": "traefik-perf-class",
  "gateway": "traefik-perf-gateway"
}
```

The script also extracts `/var/log/kubernetes/audit/audit.log` from the
kind control-plane node and counts status updates on
`*.gateway.networking.k8s.io` as an independent cross-check against
Traefik's own write counter.

Both reports land in `$TMPDIR` and are printed at the end of the run.

## Comparing with Layer 1

Layer 1 (`go test -run=TestStatusPerf -perf -routes=1000 ./pkg/provider/kubernetes/gateway/`)
runs the provider in-process against `envtest`. envtest's apiserver is
much faster than a kind apiserver, so route creates land back-to-back
faster than the provider can rebuild — informer events coalesce, and
the time-to-stable reflects only the provider's internal throughput.

Layer 2 introduces realistic apiserver round-trip latency, which spaces
creates out enough that the provider rebuilds once per event. This is
the regime the upstream bench measured (180s at N=1000). If Layer 2's
time-to-stable matches the bench's order of magnitude, Layer 1 is
trustworthy for iterating on the rebuild path.