#!/usr/bin/env bash
#
# Gateway-status perf benchmark driver.
#
# Spins up a single-node kind cluster with audit logging, builds the
# current Traefik HEAD into a Docker image, loads it into the cluster,
# installs the Traefik Helm chart configured like the upstream Howard
# John bench (kubernetesGateway on, throttleDuration=0), then runs the
# Go loadgen at hack/perf/cmd/loadgen against the cluster and prints the
# JSON report.
#
# Tears the kind cluster down at the end unless --keep is passed.
#
# Usage:
#   hack/perf/gateway-status-bench.sh [--routes N] [--keep] [--cluster NAME]
#                                     [--no-build]
#
# Prerequisites: docker, kind, helm, kubectl, go.

set -euo pipefail

ROUTES="1000"
CONCURRENCY="1"
CLUSTER="traefik-perf"
KEEP="false"
SKIP_BUILD="false"
QUIESCENCE="5s"
GATEWAY_API_VERSION="v1.5.1"
# Semver-shaped tag for the locally built image. Must match hack/perf/values.yaml.
# The Traefik Helm chart's templates use semverCompare: they reject "latest"
# and enforce >= v3.6.0, so the major.minor here must track master's
# release line.
IMAGE_TAG="v3.7.0-perf"

usage() {
  sed -n 's/^# \{0,1\}//;3,17p' "$0"
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --routes)        ROUTES="$2"; shift 2 ;;
    --routes=*)      ROUTES="${1#*=}"; shift ;;
    --concurrency)   CONCURRENCY="$2"; shift 2 ;;
    --concurrency=*) CONCURRENCY="${1#*=}"; shift ;;
    --cluster)       CLUSTER="$2"; shift 2 ;;
    --cluster=*)     CLUSTER="${1#*=}"; shift ;;
    --quiescence)    QUIESCENCE="$2"; shift 2 ;;
    --keep)          KEEP="true"; shift ;;
    --no-build)      SKIP_BUILD="true"; shift ;;
    -h|--help)       usage ;;
    *) echo "unknown flag: $1" >&2; usage ;;
  esac
done

# Resolve repo root from this script's location so the rest of the
# pipeline works from any cwd.
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." >/dev/null 2>&1 && pwd)"
cd "${REPO_ROOT}"

cleanup() {
  if [[ "${KEEP}" == "true" ]]; then
    echo ">>> --keep set: leaving kind cluster '${CLUSTER}' running"
    return
  fi
  echo ">>> tearing down kind cluster '${CLUSTER}'"
  kind delete cluster --name "${CLUSTER}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

for bin in docker kind helm kubectl go; do
  if ! command -v "${bin}" >/dev/null 2>&1; then
    echo "error: ${bin} not found in PATH" >&2
    exit 1
  fi
done

echo ">>> using kind cluster '${CLUSTER}', routes=${ROUTES}, quiescence=${QUIESCENCE}"

if kind get clusters | grep -qx "${CLUSTER}"; then
  echo ">>> kind cluster '${CLUSTER}' already exists; reusing"
else
  echo ">>> creating kind cluster '${CLUSTER}'"
  kind create cluster --config hack/perf/kind.yaml --name "${CLUSTER}"
fi

KIND_NODE="${CLUSTER}-control-plane"
KUBECONFIG_FILE="$(mktemp)"
trap 'rm -f "${KUBECONFIG_FILE}"; cleanup' EXIT
kind get kubeconfig --name "${CLUSTER}" > "${KUBECONFIG_FILE}"
export KUBECONFIG="${KUBECONFIG_FILE}"

if [[ "${SKIP_BUILD}" == "true" ]]; then
  echo ">>> --no-build set: skipping image build"
else
  echo ">>> building Traefik image (make build-image)"
  make build-image
fi

echo ">>> tagging traefik/traefik:latest as traefik/traefik:${IMAGE_TAG}"
docker tag traefik/traefik:latest "traefik/traefik:${IMAGE_TAG}"

echo ">>> loading traefik/traefik:${IMAGE_TAG} into kind"
kind load docker-image "traefik/traefik:${IMAGE_TAG}" --name "${CLUSTER}"

echo ">>> installing Gateway API CRDs ${GATEWAY_API_VERSION}"
kubectl apply -f "https://github.com/kubernetes-sigs/gateway-api/releases/download/${GATEWAY_API_VERSION}/standard-install.yaml"

if ! helm repo list 2>/dev/null | grep -q '^traefik\s'; then
  echo ">>> adding traefik helm repo"
  helm repo add traefik https://traefik.github.io/charts
fi
helm repo update >/dev/null

echo ">>> installing Traefik via helm"
helm upgrade --install traefik traefik/traefik \
  --namespace traefik --create-namespace \
  -f hack/perf/values.yaml

echo ">>> waiting for Traefik deployment to be ready"
kubectl -n traefik rollout status deploy/traefik --timeout=2m

REPORT_PATH="$(mktemp -t gateway-status-bench.XXXXXX.json)"
echo ">>> running loadgen"
go run ./hack/perf/cmd/loadgen \
  -kubeconfig="${KUBECONFIG_FILE}" \
  -routes="${ROUTES}" \
  -concurrency="${CONCURRENCY}" \
  -quiescence="${QUIESCENCE}" \
  -out="${REPORT_PATH}"

echo ">>> JSON report saved to ${REPORT_PATH}"

AUDIT_OUT="$(mktemp -t gateway-status-bench-audit.XXXXXX.log)"
if docker cp "${KIND_NODE}:/var/log/kubernetes/audit/audit.log" "${AUDIT_OUT}" 2>/dev/null; then
  STATUS_WRITES=$(grep -c '"verb":"update".*"subresource":"status"' "${AUDIT_OUT}" || true)
  echo ">>> apiserver audit: ${STATUS_WRITES} status updates on gateway.networking.k8s.io"
  echo ">>> full audit log: ${AUDIT_OUT}"
else
  echo ">>> could not extract audit log from ${KIND_NODE} (skipping cross-check)"
  rm -f "${AUDIT_OUT}"
fi