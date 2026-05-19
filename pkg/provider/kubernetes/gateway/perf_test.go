package gateway

// TestStatusPerf reproduces the Howard John Gateway API bench v1
// "Attached Routes" scenario in-process, so we can profile where time goes
// while N HTTPRoutes attach to a single Gateway.
//
// This is a Test, not a Benchmark — wall-clock time is dominated by an
// external apiserver, which makes testing.B's iteration scaling
// misleading. The test is skipped unless -perf is passed.
//
// Invocation:
//
//	export KUBEBUILDER_ASSETS="$(setup-envtest use 1.31.x -p path)"
//	go test -run=TestStatusPerf -perf -routes=1000 \
//	  -cpuprofile=/tmp/gateway-perf.pprof \
//	  ./pkg/provider/kubernetes/gateway/
//
// See perfharness_test.go for the shared envtest bootstrap.

import (
	"flag"
	"fmt"
	"os"
	"runtime/pprof"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	gatev1 "sigs.k8s.io/gateway-api/apis/v1"
	gateclientset "sigs.k8s.io/gateway-api/pkg/client/clientset/versioned"
	gateinformers "sigs.k8s.io/gateway-api/pkg/client/informers/externalversions"
)

var (
	perfEnabled    = flag.Bool("perf", false, "enable TestStatusPerf (otherwise skipped)")
	perfRoutes     = flag.Int("routes", 1000, "number of HTTPRoutes to create")
	perfRouteBatch = flag.Int("route-batch", 1, "routes per concurrent creator goroutine (1 = single-threaded, matches the bench)")
	perfQuiescence = flag.Duration("quiescence", 3*time.Second, "no-status-write window that defines a stable apiserver state")
	perfCPUProfile = flag.String("cpuprofile", "", "if set, write a runtime/pprof CPU profile to this path")
)

const (
	perfNamespace      = "traefik-perf"
	perfGatewayClass   = "traefik-perf-class"
	perfGateway        = "traefik-perf-gateway"
	perfNoopService    = "noop"
	perfControllerName = controllerName
)

func TestStatusPerf(t *testing.T) {
	if !*perfEnabled {
		t.Skip("perf gated; pass -perf to run TestStatusPerf")
	}

	if *perfRoutes <= 0 {
		t.Fatalf("invalid -routes=%d, must be > 0", *perfRoutes)
	}

	if *perfCPUProfile != "" {
		f, err := os.Create(*perfCPUProfile)
		require.NoError(t, err)
		require.NoError(t, pprof.StartCPUProfile(f))
		t.Cleanup(pprof.StopCPUProfile)
		t.Cleanup(func() { _ = f.Close() })
	}

	cfg := startEnvTest(t)
	createNamespace(t, cfg, perfNamespace)
	createNoopService(t, cfg, perfNamespace, perfNoopService)

	gwClient, err := gateclientset.NewForConfig(cfg)
	require.NoError(t, err)

	createPerfGatewayClass(t, gwClient)
	createPerfGateway(t, gwClient)

	harness := newPerfHarness(t, cfg, perfNamespace)
	harness.drainConfigChan(t.Context())

	// Let the provider settle: wait until the GatewayClass we just created
	// reports Accepted=True, i.e. the provider has done at least one full
	// rebuild against the initial cluster state.
	waitForGatewayClassAccepted(t, gwClient, perfGatewayClass)

	// Reset counters: we only want to measure work caused by the N creates.
	preWrites := harness.Provider.client.snapshotMetrics()
	preRebuilds := len(*harness.rebuildDurations)

	// Wire an HTTPRoute status watcher. Every status-update event from the
	// apiserver kicks the signal channel; we use it for quiescence detection.
	statusSignal := newHTTPRouteStatusSignal(t, gwClient, perfNamespace)

	// Create N routes one at a time.
	tCreateStart := time.Now()
	createPerfRoutes(t, gwClient, *perfRoutes, *perfRouteBatch)
	tCreateDone := time.Now()
	t.Logf("created %d routes in %s", *perfRoutes, tCreateDone.Sub(tCreateStart).Round(time.Millisecond))

	// Wait for the apiserver to go quiet on HTTPRoute status writes.
	lastWrite, err := waitForQuiescence(t.Context(), statusSignal, *perfQuiescence)
	require.NoError(t, err, "quiescence")

	// Stop the provider so rebuildDurations is safe to read.
	harness.Pool.Stop()

	postWrites := harness.Provider.client.snapshotMetrics()
	postRebuilds := *harness.rebuildDurations

	summary := perfSummary{
		Routes:       *perfRoutes,
		TimeToStable: lastWrite.Sub(tCreateDone),
		Writes:       diffMetrics(preWrites, postWrites),
	}
	summary.RebuildCount, summary.RebuildMean, summary.RebuildP99, summary.RebuildMax = rebuildStats(postRebuilds[preRebuilds:])

	t.Logf("perf summary: %s", summary)
}

func createPerfGatewayClass(t *testing.T, gw gateclientset.Interface) {
	t.Helper()
	_, err := gw.GatewayV1().GatewayClasses().Create(t.Context(), &gatev1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: perfGatewayClass},
		Spec: gatev1.GatewayClassSpec{
			ControllerName: perfControllerName,
		},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
}

func createPerfGateway(t *testing.T, gw gateclientset.Interface) {
	t.Helper()
	_, err := gw.GatewayV1().Gateways(perfNamespace).Create(t.Context(), &gatev1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: perfGateway, Namespace: perfNamespace},
		Spec: gatev1.GatewaySpec{
			GatewayClassName: perfGatewayClass,
			Listeners: []gatev1.Listener{
				{
					Name:     "web",
					Port:     80,
					Protocol: gatev1.HTTPProtocolType,
				},
			},
		},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
}

// createPerfRoutes creates n HTTPRoutes against perfGateway. When batch <= 1
// the creates are serial (matches the upstream bench: one apiserver request
// per route, no batching). When batch > 1, n goroutines create their share
// in parallel — useful for separating "many events" from "many routes".
func createPerfRoutes(t *testing.T, gw gateclientset.Interface, n, batch int) {
	t.Helper()

	if batch <= 1 {
		for i := range n {
			createOneRoute(t, gw, i)
		}
		return
	}

	done := make(chan struct{}, batch)
	for w := range batch {
		go func(worker int) {
			for i := worker; i < n; i += batch {
				createOneRoute(t, gw, i)
			}
			done <- struct{}{}
		}(w)
	}
	for range batch {
		<-done
	}
}

func createOneRoute(t *testing.T, gw gateclientset.Interface, idx int) {
	t.Helper()
	name := fmt.Sprintf("route-%05d", idx)
	host := gatev1.Hostname(fmt.Sprintf("perf-%05d.test", idx))
	_, err := gw.GatewayV1().HTTPRoutes(perfNamespace).Create(t.Context(), &gatev1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: perfNamespace},
		Spec: gatev1.HTTPRouteSpec{
			CommonRouteSpec: gatev1.CommonRouteSpec{
				ParentRefs: []gatev1.ParentReference{
					{Name: perfGateway},
				},
			},
			Hostnames: []gatev1.Hostname{host},
			Rules: []gatev1.HTTPRouteRule{
				{
					BackendRefs: []gatev1.HTTPBackendRef{
						{
							BackendRef: gatev1.BackendRef{
								BackendObjectReference: gatev1.BackendObjectReference{
									Name: "noop",
									Port: ptrTo(gatev1.PortNumber(80)),
								},
							},
						},
					},
				},
			},
		},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
}

// waitForGatewayClassAccepted polls until the named GatewayClass has an
// Accepted=True condition or the test context expires.
func waitForGatewayClassAccepted(t *testing.T, gw gateclientset.Interface, name string) {
	t.Helper()
	ctx := t.Context()
	deadline := time.Now().Add(30 * time.Second)
	for {
		got, err := gw.GatewayV1().GatewayClasses().Get(ctx, name, metav1.GetOptions{})
		require.NoError(t, err)
		for _, c := range got.Status.Conditions {
			if c.Type == string(gatev1.GatewayClassConditionStatusAccepted) && c.Status == metav1.ConditionTrue {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("GatewayClass %s never reached Accepted=True", name)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context canceled waiting for GatewayClass Accepted")
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// newHTTPRouteStatusSignal returns a channel that receives a tick every time
// an HTTPRoute in ns is added or updated. The harness uses this for
// quiescence detection — once the channel goes quiet for the configured
// window, the apiserver is considered stable.
func newHTTPRouteStatusSignal(t *testing.T, gw gateclientset.Interface, ns string) <-chan struct{} {
	t.Helper()
	ctx := t.Context()

	factory := gateinformers.NewSharedInformerFactoryWithOptions(gw, 0, gateinformers.WithNamespace(ns))
	informer := factory.Gateway().V1().HTTPRoutes().Informer()

	signal := make(chan struct{}, 1024)
	tick := func() {
		select {
		case signal <- struct{}{}:
		default:
		}
	}
	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(any) { tick() },
		UpdateFunc: func(any, any) { tick() },
	})
	require.NoError(t, err)

	factory.Start(ctx.Done())
	for typ, ok := range factory.WaitForCacheSync(ctx.Done()) {
		require.Truef(t, ok, "informer cache failed to sync: %s", typ)
	}
	return signal
}

func diffMetrics(before, after clientMetricsSnapshot) clientMetricsSnapshot {
	return clientMetricsSnapshot{
		GatewayClassUpdates:     after.GatewayClassUpdates - before.GatewayClassUpdates,
		GatewayUpdates:          after.GatewayUpdates - before.GatewayUpdates,
		HTTPRouteUpdates:        after.HTTPRouteUpdates - before.HTTPRouteUpdates,
		GRPCRouteUpdates:        after.GRPCRouteUpdates - before.GRPCRouteUpdates,
		TCPRouteUpdates:         after.TCPRouteUpdates - before.TCPRouteUpdates,
		TLSRouteUpdates:         after.TLSRouteUpdates - before.TLSRouteUpdates,
		BackendTLSPolicyUpdates: after.BackendTLSPolicyUpdates - before.BackendTLSPolicyUpdates,
	}
}

func rebuildStats(durations []time.Duration) (count int, mean, p99, mx time.Duration) {
	count = len(durations)
	if count == 0 {
		return
	}

	var sum time.Duration
	for _, d := range durations {
		sum += d
		if d > mx {
			mx = d
		}
	}
	mean = sum / time.Duration(count)

	sorted := make([]time.Duration, count)
	copy(sorted, durations)
	slices.Sort(sorted)
	idx := (count*99 + 99) / 100
	if idx >= count {
		idx = count - 1
	}
	p99 = sorted[idx]
	return
}

// ptrTo is a tiny helper for inline pointer values in spec structs.
func ptrTo[T any](v T) *T { return &v }
