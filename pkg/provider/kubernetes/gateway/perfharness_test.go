package gateway

// Shared envtest-based harness for the Gateway provider perf test
// (see perf_test.go). Test-only — no production code calls into this file.
//
// Prerequisites:
//   - Install setup-envtest:
//       go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest
//   - Export the binary path (etcd, kube-apiserver, kubectl) for a Kubernetes
//     version matching go.mod's controller-runtime release:
//       export KUBEBUILDER_ASSETS="$(setup-envtest use 1.31.x -p path)"
//
// CRDs are vendored under testdata/perf/crds/standard-install.yaml from the
// Gateway API v1.5.1 release:
//   https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.5.1/standard-install.yaml
// Bump this YAML alongside the sigs.k8s.io/gateway-api module bump.

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/traefik/traefik/v3/pkg/config/dynamic"
	"github.com/traefik/traefik/v3/pkg/safe"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kclientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	gatev1 "sigs.k8s.io/gateway-api/apis/v1"
	gateclientset "sigs.k8s.io/gateway-api/pkg/client/clientset/versioned"
	gateinformers "sigs.k8s.io/gateway-api/pkg/client/informers/externalversions"
)

// crdDir is the on-disk location of the vendored Gateway API CRDs relative
// to this test file (Go test working directory is always the package dir).
const crdDir = "testdata/perf/crds"

// startEnvTest boots a local kube-apiserver+etcd, installs the vendored
// Gateway API CRDs, and returns its rest.Config. The Cleanup closure stops
// the apiserver and removes its data dir.
func startEnvTest(t *testing.T) *rest.Config {
	t.Helper()

	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is not set; run `setup-envtest use ... -p path` (see perfharness_test.go).")
	}

	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{crdDir},
		ErrorIfCRDPathMissing: true,
	}

	cfg, err := env.Start()
	require.NoError(t, err, "starting envtest")
	require.NotNil(t, cfg)

	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Logf("stopping envtest: %v", err)
		}
	})

	return cfg
}

// createNamespace creates the given namespace via the kube clientset.
// Idempotent: returns nil if the namespace already exists.
func createNamespace(t *testing.T, cfg *rest.Config, name string) {
	t.Helper()

	kc, err := kclientset.NewForConfig(cfg)
	require.NoError(t, err)

	_, err = kc.CoreV1().Namespaces().Create(t.Context(), &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: name},
	}, metav1.CreateOptions{})
	require.NoError(t, err, "creating namespace %s", name)
}

// createNoopService creates a Service that the perf routes point at, so the
// provider's backend resolver finds an object and does not log
// "service not found" errors during the run. The nativeLB annotation makes
// the provider use the Service's ClusterIP directly instead of listing
// EndpointSlices (which envtest does not populate without kube-controller-
// manager).
func createNoopService(t *testing.T, cfg *rest.Config, namespace, name string) {
	t.Helper()

	kc, err := kclientset.NewForConfig(cfg)
	require.NoError(t, err)

	_, err = kc.CoreV1().Services(namespace).Create(t.Context(), &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Annotations: map[string]string{"traefik.io/service.nativeLB": "true"},
		},
		Spec: corev1.ServiceSpec{
			Type:      corev1.ServiceTypeClusterIP,
			ClusterIP: "10.0.0.10",
			Ports: []corev1.ServicePort{
				{Name: "http", Port: 80, Protocol: corev1.ProtocolTCP},
			},
		},
	}, metav1.CreateOptions{})
	require.NoError(t, err, "creating noop service %s/%s", namespace, name)
}

// perfHarness packages the Provider + its event-loop pool + the
// configurationChan it writes to.
type perfHarness struct {
	Provider *Provider
	Pool     *safe.Pool
	ConfChan chan dynamic.Message

	// rebuildDurations is appended-to from rebuildHook. Tests read this
	// after Pool.Stop() so the slice does not need its own mutex (Stop
	// joins the writer goroutine).
	rebuildDurations *[]time.Duration
}

// newPerfHarness constructs a Provider bound to the envtest apiserver,
// configures it like the upstream Howard John bench (ThrottleDuration=0,
// HTTP entrypoint on :80), and starts the Provide loop.
//
// The harness watches only the given namespace so kube-system noise does
// not pollute timings.
func newPerfHarness(t *testing.T, cfg *rest.Config, namespace string) *perfHarness {
	t.Helper()

	client, err := createClientFromConfig(cfg)
	require.NoError(t, err, "constructing clientWrapper")

	rebuilds := make([]time.Duration, 0, 1024)

	p := &Provider{
		EntryPoints: map[string]Entrypoint{
			"web": {Address: ":80"},
		},
		Namespaces:       []string{namespace},
		ThrottleDuration: 0,
		client:           client,
		rebuildHook: func(d time.Duration) {
			rebuilds = append(rebuilds, d)
		},
	}

	pool := safe.NewPool(t.Context())
	confChan := make(chan dynamic.Message, 1)

	require.NoError(t, p.Provide(confChan, pool))

	t.Cleanup(func() {
		pool.Stop()
	})

	return &perfHarness{
		Provider:         p,
		Pool:             pool,
		ConfChan:         confChan,
		rebuildDurations: &rebuilds,
	}
}

// drainConfigChan keeps the Provider's send on ConfChan non-blocking by
// receiving from it in a goroutine until ctx is done. The dynamic config
// itself is irrelevant to the perf measurements.
func (h *perfHarness) drainConfigChan(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-h.ConfChan:
			}
		}
	}()
}

// waitForQuiescence blocks until the given signal channel has not received
// for window duration, or ctx is done. Returns the time at which the last
// event was observed.
func waitForQuiescence(ctx context.Context, signal <-chan struct{}, window time.Duration) (time.Time, error) {
	last := time.Now()
	timer := time.NewTimer(window)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return last, ctx.Err()
		case <-signal:
			last = time.Now()
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(window)
		case <-timer.C:
			return last, nil
		}
	}
}

// summary aggregates per-kind write counts and rebuild stats for reporting.
type perfSummary struct {
	Routes         int
	Concurrency    int
	CreateDuration time.Duration
	// SetupTime mirrors the bench's "Setup time" column: time from
	// "last route created" to "Gateway.AttachedRoutes == N".
	SetupTime           time.Duration
	TimeToAttachedN     time.Duration
	TimeToStable        time.Duration
	GatewayStatusWrites int
	Writes              clientMetricsSnapshot
	RebuildCount        int
	RebuildMean         time.Duration
	RebuildP99          time.Duration
	RebuildMax          time.Duration
}

func (s perfSummary) String() string {
	var totalRebuild time.Duration
	if s.RebuildCount > 0 {
		totalRebuild = s.RebuildMean * time.Duration(s.RebuildCount)
	}
	ioShare := "n/a"
	if totalRebuild > 0 {
		ioShare = fmt.Sprintf("%.0f%%", float64(s.Writes.StatusIOTime)/float64(totalRebuild)*100)
	}
	return fmt.Sprintf(
		"routes=%d concurrency=%d createDuration=%s setupTime=%s timeToAttachedN=%s timeToStable=%s gatewayStatusSamples=%d rebuilds=%d (mean=%s p99=%s max=%s totalRebuild=%s statusIO=%s ioShare=%s) writes={gatewayClass=%d gateway=%d httpRoute=%d grpcRoute=%d tcpRoute=%d tlsRoute=%d backendTLSPolicy=%d}",
		s.Routes, s.Concurrency,
		s.CreateDuration.Round(time.Millisecond), s.SetupTime.Round(time.Millisecond),
		s.TimeToAttachedN.Round(time.Millisecond), s.TimeToStable.Round(time.Millisecond), s.GatewayStatusWrites,
		s.RebuildCount, s.RebuildMean.Round(time.Millisecond), s.RebuildP99.Round(time.Millisecond), s.RebuildMax.Round(time.Millisecond),
		totalRebuild.Round(time.Millisecond), s.Writes.StatusIOTime.Round(time.Millisecond), ioShare,
		s.Writes.GatewayClassUpdates, s.Writes.GatewayUpdates, s.Writes.HTTPRouteUpdates,
		s.Writes.GRPCRouteUpdates, s.Writes.TCPRouteUpdates, s.Writes.TLSRouteUpdates,
		s.Writes.BackendTLSPolicyUpdates,
	)
}

// gatewayAttachedRoutesWatcher mirrors what the upstream Howard John bench
// records: every distinct value of Gateway.Status.Listeners[0].AttachedRoutes
// observed via an informer after a configurable start time. The bench's
// primary signal — time to reach AttachedRoutes==N — is read off this
// watcher in perf_test.go.
type gatewayAttachedRoutesWatcher struct {
	mu      sync.Mutex
	start   time.Time
	last    int
	samples []attachedSample
	reached chan int
}

type attachedSample struct {
	Offset         time.Duration
	AttachedRoutes int
}

func newGatewayAttachedRoutesWatcher(t *testing.T, gw gateclientset.Interface, namespace, name string) *gatewayAttachedRoutesWatcher {
	t.Helper()
	ctx := t.Context()

	w := &gatewayAttachedRoutesWatcher{
		reached: make(chan int, 1),
	}

	factory := gateinformers.NewSharedInformerFactoryWithOptions(gw, 0, gateinformers.WithNamespace(namespace))
	informer := factory.Gateway().V1().Gateways().Informer()

	onEvent := func(obj any) {
		gateway, ok := obj.(*gatev1.Gateway)
		if !ok || gateway.Name != name {
			return
		}
		if len(gateway.Status.Listeners) == 0 {
			return
		}
		w.observe(int(gateway.Status.Listeners[0].AttachedRoutes))
	}
	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    onEvent,
		UpdateFunc: func(_, obj any) { onEvent(obj) },
	})
	require.NoError(t, err)

	factory.Start(ctx.Done())
	for typ, ok := range factory.WaitForCacheSync(ctx.Done()) {
		require.Truef(t, ok, "Gateway informer cache failed to sync: %s", typ)
	}
	return w
}

// SetStart resets the watcher: from this point on every distinct
// AttachedRoutes value is appended as a sample, with an offset relative to
// `t`.
func (w *gatewayAttachedRoutesWatcher) SetStart(t time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.start = t
	w.last = -1
	w.samples = nil
}

// WaitForN blocks until AttachedRoutes is observed to be >= want or the
// timeout elapses. Returns the offset (relative to SetStart) of the
// qualifying sample.
func (w *gatewayAttachedRoutesWatcher) WaitForN(ctx context.Context, want int, timeout time.Duration) (time.Duration, error) {
	w.mu.Lock()
	if w.last >= want && len(w.samples) > 0 {
		off := w.samples[len(w.samples)-1].Offset
		w.mu.Unlock()
		return off, nil
	}
	w.mu.Unlock()

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-deadline.C:
			return 0, fmt.Errorf("AttachedRoutes did not reach %d within %s", want, timeout)
		case v := <-w.reached:
			if v >= want {
				w.mu.Lock()
				off := w.samples[len(w.samples)-1].Offset
				w.mu.Unlock()
				return off, nil
			}
		}
	}
}

// SampleCount returns the number of distinct AttachedRoutes values
// recorded — i.e. the number of Gateway-status writes the apiserver
// observed since SetStart.
func (w *gatewayAttachedRoutesWatcher) SampleCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.samples)
}

func (w *gatewayAttachedRoutesWatcher) observe(attached int) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.start.IsZero() || attached == w.last {
		return
	}
	w.last = attached
	w.samples = append(w.samples, attachedSample{
		Offset:         time.Since(w.start),
		AttachedRoutes: attached,
	})

	select {
	case w.reached <- attached:
	default:
	}
}
