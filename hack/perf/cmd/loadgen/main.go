// Command loadgen drives the gateway-status perf benchmark against a real
// Kubernetes cluster (typically a kind cluster set up by
// hack/perf/gateway-status-bench.sh).
//
// One GatewayClass + one Gateway in the target namespace, then N
// HTTPRoutes created sequentially (one apiserver request each — no
// batching, matching the upstream Howard John bench's `pilot-load`
// AggregateSimulation.Run loop).
//
// The primary measurement is the same as the bench: time from the start of
// the create phase to when Gateway.Status.Listeners[0].AttachedRoutes
// reaches N. A secondary "time to quiescence" measurement (no HTTPRoute
// status writes for `-quiescence`) is included as a sanity check.
//
// Invocation (from the repo root, against an already-running cluster):
//
//	go run ./hack/perf/cmd/loadgen \
//	  -kubeconfig=$HOME/.kube/config \
//	  -namespace=traefik-perf -routes=1000
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"text/tabwriter"
	"time"

	corev1 "k8s.io/api/core/v1"
	kerror "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kclientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	gatev1 "sigs.k8s.io/gateway-api/apis/v1"
	gateclientset "sigs.k8s.io/gateway-api/pkg/client/clientset/versioned"
	gateinformers "sigs.k8s.io/gateway-api/pkg/client/informers/externalversions"
)

const traefikControllerName = "traefik.io/gateway-controller"

// sample mirrors the bench's Sample type: a snapshot of
// Gateway.Status.Listeners[0].AttachedRoutes at a moment in time.
type sample struct {
	OffsetMs       int64 `json:"offsetMs"`
	AttachedRoutes int   `json:"attachedRoutes"`
}

type report struct {
	Routes           int           `json:"routes"`
	Concurrency      int           `json:"concurrency"`
	CreateDurationNs time.Duration `json:"createDurationNs"`
	CreateDuration   string        `json:"createDuration"`
	// SetupTime mirrors the bench's "Setup time" column literally
	// (README.md:244): "the time after the last route is created until the
	// attachedRoutes is updated to the total route count".
	SetupTimeNs         time.Duration `json:"setupTimeNs"`
	SetupTime           string        `json:"setupTime"`
	TimeToAttachedNNs   time.Duration `json:"timeToAttachedNNs"`
	TimeToAttachedN     string        `json:"timeToAttachedN"`
	TimeToQuiescenceNs  time.Duration `json:"timeToQuiescenceNs"`
	TimeToQuiescence    string        `json:"timeToQuiescence"`
	GatewayStatusWrites int           `json:"gatewayStatusWrites"`
	HTTPRouteEvents     int           `json:"httpRouteEvents"`
	Namespace           string        `json:"namespace"`
	GatewayClass        string        `json:"gatewayClass"`
	Gateway             string        `json:"gateway"`
	AttachedSeries      []sample      `json:"attachedSeries"`
}

func main() {
	var (
		kubeconfig   = flag.String("kubeconfig", defaultKubeconfig(), "path to kubeconfig")
		namespace    = flag.String("namespace", "traefik-perf", "namespace to create routes in (created if missing)")
		gatewayClass = flag.String("gateway-class", "traefik-perf-class", "GatewayClass name (created if missing)")
		gateway      = flag.String("gateway", "traefik-perf-gateway", "Gateway name (created if missing)")
		serviceName  = flag.String("service", "noop", "backend Service name (created if missing)")
		routes       = flag.Int("routes", 1000, "number of HTTPRoutes to create")
		concurrency  = flag.Int("concurrency", 1, "number of goroutines firing creates concurrently (1 = bench-equivalent sequential; bump to synthesize burst conditions on slower hardware)")
		qps          = flag.Float64("qps", 1000, "client-side QPS limit on the apiserver. The Go client defaults to 5 — leave it that low and -concurrency does nothing. pilot-load uses 100000.")
		burst        = flag.Int("burst", 2000, "client-side burst limit; defaults to 2× -qps if you leave both alone")
		quiescence   = flag.Duration("quiescence", 5*time.Second, "no-status-write window that defines a stable apiserver state")
		out          = flag.String("out", "", "if set, write the JSON report to this path in addition to stdout")
		timeout      = flag.Duration("timeout", 30*time.Minute, "overall deadline")
	)
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	_ = cancel

	cfg, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		log.Fatalf("building rest config: %v", err)
	}

	// client-go's default is QPS=5/Burst=10. With -concurrency>1 every
	// extra worker just piles up behind that token bucket, producing the
	// same wall time as -concurrency=1. pilot-load sets QPS=100000.
	cfg.QPS = float32(*qps)
	cfg.Burst = *burst

	kc, err := kclientset.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("building kube clientset: %v", err)
	}
	gc, err := gateclientset.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("building gateway-api clientset: %v", err)
	}

	mustEnsureNamespace(ctx, kc, *namespace)
	mustEnsureService(ctx, kc, *namespace, *serviceName)
	mustEnsureGatewayClass(ctx, gc, *gatewayClass)
	mustEnsureGateway(ctx, gc, *namespace, *gateway, *gatewayClass)

	log.Printf("waiting for GatewayClass %q to be Accepted=True…", *gatewayClass)
	mustWaitForGatewayClassAccepted(ctx, gc, *gatewayClass, 2*time.Minute)

	routeSignal, routeEvents := newHTTPRouteStatusSignal(ctx, gc, *namespace)
	gwWatcher := newGatewayAttachedRoutesWatcher(ctx, gc, *namespace, *gateway)

	workers := max(*concurrency, 1)
	log.Printf("creating %d HTTPRoutes (concurrency=%d)…", *routes, workers)
	createStart := time.Now()
	gwWatcher.SetStart(createStart)
	createRoutes(ctx, gc, *namespace, *gateway, *serviceName, *routes, workers)
	createDone := time.Since(createStart)
	log.Printf("created %d routes in %s", *routes, createDone.Round(time.Millisecond))

	log.Printf("waiting for Gateway.AttachedRoutes to reach %d…", *routes)
	attachedAt, err := gwWatcher.WaitForN(ctx, *routes, *timeout)
	if err != nil {
		log.Fatalf("waiting for AttachedRoutes=%d: %v", *routes, err)
	}

	log.Printf("waiting %s of HTTPRoute status quiescence…", *quiescence)
	lastWrite, err := waitForQuiescence(ctx, routeSignal, *quiescence)
	if err != nil {
		log.Fatalf("quiescence: %v", err)
	}

	gwSamples := gwWatcher.Samples()
	timeToAttachedN := attachedAt.Sub(createStart)
	setupTime := max(timeToAttachedN-createDone, 0)
	r := report{
		Routes:              *routes,
		Concurrency:         workers,
		CreateDurationNs:    createDone,
		CreateDuration:      createDone.Round(time.Millisecond).String(),
		SetupTimeNs:         setupTime,
		TimeToAttachedNNs:   timeToAttachedN,
		TimeToQuiescenceNs:  lastWrite.Sub(createStart.Add(createDone)),
		GatewayStatusWrites: len(gwSamples),
		HTTPRouteEvents:     int(*routeEvents),
		Namespace:           *namespace,
		GatewayClass:        *gatewayClass,
		Gateway:             *gateway,
		AttachedSeries:      gwSamples,
	}
	r.SetupTime = r.SetupTimeNs.Round(time.Millisecond).String()
	r.TimeToAttachedN = r.TimeToAttachedNNs.Round(time.Millisecond).String()
	r.TimeToQuiescence = r.TimeToQuiescenceNs.Round(time.Millisecond).String()

	printReport(os.Stdout, r)

	if *out != "" {
		b, err := json.MarshalIndent(r, "", "  ")
		if err != nil {
			log.Fatalf("marshaling report: %v", err)
		}
		if err := os.WriteFile(*out, b, 0o644); err != nil {
			log.Fatalf("writing report to %s: %v", *out, err)
		}
		log.Printf("JSON report written to %s", *out)
	}
}

// printReport renders the benchmark report in a human-readable form.
// The JSON form is still available via -out.
func printReport(w io.Writer, r report) {
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Gateway-status perf benchmark report")
	fmt.Fprintln(w, "====================================")
	fmt.Fprintln(w)
	section := func(header string, write func(tw *tabwriter.Writer)) {
		fmt.Fprintln(w, header)
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		write(tw)
		if err := tw.Flush(); err != nil {
			log.Printf("flushing report: %v", err)
		}
		fmt.Fprintln(w)
	}
	section("Run", func(tw *tabwriter.Writer) {
		fmt.Fprintf(tw, "  Routes\t%d\n", r.Routes)
		fmt.Fprintf(tw, "  Concurrency\t%d\n", r.Concurrency)
		fmt.Fprintf(tw, "  Namespace\t%s\n", r.Namespace)
		fmt.Fprintf(tw, "  GatewayClass\t%s\n", r.GatewayClass)
		fmt.Fprintf(tw, "  Gateway\t%s\n", r.Gateway)
	})
	section("Timings", func(tw *tabwriter.Writer) {
		fmt.Fprintf(tw, "  Create duration\t%s\t(create all %d HTTPRoutes)\n", r.CreateDuration, r.Routes)
		fmt.Fprintf(tw, "  Setup time\t%s\t(last create → AttachedRoutes=%d)\n", r.SetupTime, r.Routes)
		fmt.Fprintf(tw, "  Time to AttachedRoutes=N\t%s\t(start → AttachedRoutes=%d)\n", r.TimeToAttachedN, r.Routes)
		fmt.Fprintf(tw, "  Time to quiescence\t%s\t(start → no HTTPRoute status writes for the quiescence window)\n", r.TimeToQuiescence)
	})
	section("Counters", func(tw *tabwriter.Writer) {
		fmt.Fprintf(tw, "  Gateway status writes\t%d\n", r.GatewayStatusWrites)
		fmt.Fprintf(tw, "  HTTPRoute events\t%d\n", r.HTTPRouteEvents)
	})
	printAttachedSeries(w, r.AttachedSeries)
}

// printAttachedSeries renders the AttachedRoutes progression as a compact
// table. Long series are folded to head + tail with an elision marker so a
// 1000-route run is still legible on one screen.
func printAttachedSeries(w io.Writer, samples []sample) {
	fmt.Fprintf(w, "AttachedRoutes progression (%d samples)\n", len(samples))
	if len(samples) == 0 {
		fmt.Fprintln(w, "  (none)")
		return
	}
	fmt.Fprintln(w, "      offset   attachedRoutes")

	const head, tail = 8, 4
	print := func(s sample) {
		fmt.Fprintf(w, "  %10s   %14d\n",
			(time.Duration(s.OffsetMs)*time.Millisecond).String(), s.AttachedRoutes)
	}
	if len(samples) <= head+tail+1 {
		for _, s := range samples {
			print(s)
		}
		return
	}
	for _, s := range samples[:head] {
		print(s)
	}
	fmt.Fprintf(w, "  %10s   %14s\n", "…", "…")
	for _, s := range samples[len(samples)-tail:] {
		print(s)
	}
}

func defaultKubeconfig() string {
	if v := os.Getenv("KUBECONFIG"); v != "" {
		return v
	}
	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, ".kube", "config")
	}
	return ""
}

func mustEnsureNamespace(ctx context.Context, kc kclientset.Interface, name string) {
	_, err := kc.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: name},
	}, metav1.CreateOptions{})
	if err != nil && !kerror.IsAlreadyExists(err) {
		log.Fatalf("ensuring namespace %s: %v", name, err)
	}
}

func mustEnsureService(ctx context.Context, kc kclientset.Interface, namespace, name string) {
	_, err := kc.CoreV1().Services(namespace).Create(ctx, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Annotations: map[string]string{"traefik.io/service.nativeLB": "true"},
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeClusterIP,
			Ports: []corev1.ServicePort{
				{Name: "http", Port: 80, Protocol: corev1.ProtocolTCP},
			},
		},
	}, metav1.CreateOptions{})
	if err != nil && !kerror.IsAlreadyExists(err) {
		log.Fatalf("ensuring service %s/%s: %v", namespace, name, err)
	}
}

func mustEnsureGatewayClass(ctx context.Context, gc gateclientset.Interface, name string) {
	_, err := gc.GatewayV1().GatewayClasses().Create(ctx, &gatev1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: gatev1.GatewayClassSpec{
			ControllerName: traefikControllerName,
		},
	}, metav1.CreateOptions{})
	if err != nil && !kerror.IsAlreadyExists(err) {
		log.Fatalf("ensuring GatewayClass %s: %v", name, err)
	}
}

func mustEnsureGateway(ctx context.Context, gc gateclientset.Interface, namespace, name, gatewayClass string) {
	_, err := gc.GatewayV1().Gateways(namespace).Create(ctx, &gatev1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: gatev1.GatewaySpec{
			GatewayClassName: gatev1.ObjectName(gatewayClass),
			Listeners: []gatev1.Listener{
				{Name: "web", Port: 80, Protocol: gatev1.HTTPProtocolType},
			},
		},
	}, metav1.CreateOptions{})
	if err != nil && !kerror.IsAlreadyExists(err) {
		log.Fatalf("ensuring Gateway %s/%s: %v", namespace, name, err)
	}
}

func mustWaitForGatewayClassAccepted(ctx context.Context, gc gateclientset.Interface, name string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for {
		got, err := gc.GatewayV1().GatewayClasses().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			log.Fatalf("getting GatewayClass %s: %v", name, err)
		}
		for _, c := range got.Status.Conditions {
			if c.Type == string(gatev1.GatewayClassConditionStatusAccepted) && c.Status == metav1.ConditionTrue {
				return
			}
		}
		if time.Now().After(deadline) {
			log.Fatalf("GatewayClass %s did not reach Accepted=True within %s — is Traefik running?", name, timeout)
		}
		select {
		case <-ctx.Done():
			log.Fatalf("context canceled waiting for GatewayClass Accepted")
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// createRoutes drives the create phase. With workers == 1 the loop is
// sequential — exact match for pilot-load's AggregateSimulation.Run. With
// workers > 1 the work is striped across goroutines, which synthesizes the
// "events arrive faster than rebuilds drain" regime the bench observed on
// fast hardware.
func createRoutes(ctx context.Context, gc gateclientset.Interface, namespace, gateway, service string, total, workers int) {
	if workers <= 1 {
		for i := range total {
			mustCreateOneRoute(ctx, gc, namespace, gateway, service, i)
		}
		return
	}

	done := make(chan struct{}, workers)
	for w := range workers {
		go func(worker int) {
			for i := worker; i < total; i += workers {
				mustCreateOneRoute(ctx, gc, namespace, gateway, service, i)
			}
			done <- struct{}{}
		}(w)
	}
	for range workers {
		<-done
	}
}

func mustCreateOneRoute(ctx context.Context, gc gateclientset.Interface, namespace, gateway, service string, idx int) {
	name := fmt.Sprintf("route-%05d", idx)
	host := gatev1.Hostname(fmt.Sprintf("perf-%05d.test", idx))
	port := gatev1.PortNumber(80)
	_, err := gc.GatewayV1().HTTPRoutes(namespace).Create(ctx, &gatev1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: gatev1.HTTPRouteSpec{
			CommonRouteSpec: gatev1.CommonRouteSpec{
				ParentRefs: []gatev1.ParentReference{
					{Name: gatev1.ObjectName(gateway)},
				},
			},
			Hostnames: []gatev1.Hostname{host},
			Rules: []gatev1.HTTPRouteRule{
				{
					BackendRefs: []gatev1.HTTPBackendRef{
						{
							BackendRef: gatev1.BackendRef{
								BackendObjectReference: gatev1.BackendObjectReference{
									Name: gatev1.ObjectName(service),
									Port: &port,
								},
							},
						},
					},
				},
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		log.Fatalf("creating HTTPRoute %s/%s: %v", namespace, name, err)
	}
}

// newHTTPRouteStatusSignal wires an HTTPRoute informer in ns and returns a
// channel that ticks on every Add/Update event, plus a counter incremented
// at each tick.
func newHTTPRouteStatusSignal(ctx context.Context, gc gateclientset.Interface, ns string) (<-chan struct{}, *int64) {
	factory := gateinformers.NewSharedInformerFactoryWithOptions(gc, 0, gateinformers.WithNamespace(ns))
	informer := factory.Gateway().V1().HTTPRoutes().Informer()

	signal := make(chan struct{}, 4096)
	var count int64
	tick := func() {
		count++
		select {
		case signal <- struct{}{}:
		default:
		}
	}
	if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(any) { tick() },
		UpdateFunc: func(any, any) { tick() },
	}); err != nil {
		log.Fatalf("adding informer event handler: %v", err)
	}

	factory.Start(ctx.Done())
	for typ, ok := range factory.WaitForCacheSync(ctx.Done()) {
		if !ok {
			log.Fatalf("informer cache failed to sync: %s", typ)
		}
	}
	return signal, &count
}

// gatewayAttachedRoutesWatcher records every observed value of
// Gateway.Status.Listeners[0].AttachedRoutes after a configurable "start"
// timestamp. It matches the bench's Watcher (tests/attachedroutes/attachedroutes.go).
type gatewayAttachedRoutesWatcher struct {
	mu      sync.Mutex
	start   time.Time
	last    int
	samples []sample
	reached chan int
}

func newGatewayAttachedRoutesWatcher(ctx context.Context, gc gateclientset.Interface, namespace, name string) *gatewayAttachedRoutesWatcher {
	w := &gatewayAttachedRoutesWatcher{
		reached: make(chan int, 1),
	}

	factory := gateinformers.NewSharedInformerFactoryWithOptions(gc, 0, gateinformers.WithNamespace(namespace))
	informer := factory.Gateway().V1().Gateways().Informer()

	onEvent := func(obj any) {
		gw, ok := obj.(*gatev1.Gateway)
		if !ok || gw.Name != name {
			return
		}
		if len(gw.Status.Listeners) == 0 {
			return
		}
		w.observe(int(gw.Status.Listeners[0].AttachedRoutes))
	}
	if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    onEvent,
		UpdateFunc: func(_, obj any) { onEvent(obj) },
	}); err != nil {
		log.Fatalf("adding Gateway informer event handler: %v", err)
	}

	factory.Start(ctx.Done())
	for typ, ok := range factory.WaitForCacheSync(ctx.Done()) {
		if !ok {
			log.Fatalf("Gateway informer cache failed to sync: %s", typ)
		}
	}
	return w
}

// SetStart marks the t0 used for sample offsets and primes the watcher to
// start recording. Events that arrive before SetStart is called are
// silently discarded.
func (w *gatewayAttachedRoutesWatcher) SetStart(t time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.start = t
	w.last = -1
	w.samples = nil
}

// WaitForN blocks until AttachedRoutes is observed to be >= want or the
// timeout elapses. Returns the absolute time of the qualifying sample.
func (w *gatewayAttachedRoutesWatcher) WaitForN(ctx context.Context, want int, timeout time.Duration) (time.Time, error) {
	w.mu.Lock()
	if w.last >= want {
		off := w.samples[len(w.samples)-1].OffsetMs
		t := w.start.Add(time.Duration(off) * time.Millisecond)
		w.mu.Unlock()
		return t, nil
	}
	w.mu.Unlock()

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		select {
		case <-ctx.Done():
			return time.Time{}, ctx.Err()
		case <-deadline.C:
			return time.Time{}, fmt.Errorf("AttachedRoutes did not reach %d within %s", want, timeout)
		case v := <-w.reached:
			if v >= want {
				w.mu.Lock()
				off := w.samples[len(w.samples)-1].OffsetMs
				t := w.start.Add(time.Duration(off) * time.Millisecond)
				w.mu.Unlock()
				return t, nil
			}
		}
	}
}

// Samples returns a copy of the recorded series.
func (w *gatewayAttachedRoutesWatcher) Samples() []sample {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]sample, len(w.samples))
	copy(out, w.samples)
	return out
}

func (w *gatewayAttachedRoutesWatcher) observe(attached int) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.start.IsZero() {
		return
	}
	if attached == w.last {
		return
	}
	w.last = attached
	w.samples = append(w.samples, sample{
		OffsetMs:       time.Since(w.start).Milliseconds(),
		AttachedRoutes: attached,
	})

	select {
	case w.reached <- attached:
	default:
	}
}

// waitForQuiescence blocks until signal has been silent for window duration
// or ctx expires. Returns the time of the last observed event.
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
