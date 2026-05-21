package gateway

import (
	"context"

	"github.com/rs/zerolog/log"
	ktypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	gatev1 "sigs.k8s.io/gateway-api/apis/v1"
	gatev1alpha2 "sigs.k8s.io/gateway-api/apis/v1alpha2"
)

// statusReport collects the status writes produced by a single rebuild so they
// can be flushed to the apiserver after the dynamic configuration has been
// published. Route- and policy-level entries hold slices of per-(parent or
// ancestor) statuses that accumulate across rebuild iterations: each distinct
// ParentRef/AncestorRef gets one entry, and a repeated write for the same
// ParentRef/AncestorRef overwrites just that entry. GatewayClass and Gateway
// statuses are built in full per resource, so a simple per-name map is enough.
type statusReport struct {
	gatewayClasses     map[string]gatev1.GatewayClassStatus
	gateways           map[ktypes.NamespacedName]gatev1.GatewayStatus
	httpRoutes         map[ktypes.NamespacedName][]gatev1.RouteParentStatus
	grpcRoutes         map[ktypes.NamespacedName][]gatev1.RouteParentStatus
	tcpRoutes          map[ktypes.NamespacedName][]gatev1alpha2.RouteParentStatus
	tlsRoutes          map[ktypes.NamespacedName][]gatev1.RouteParentStatus
	backendTLSPolicies map[ktypes.NamespacedName][]gatev1.PolicyAncestorStatus
}

func newStatusReport() *statusReport {
	return &statusReport{
		gatewayClasses:     map[string]gatev1.GatewayClassStatus{},
		gateways:           map[ktypes.NamespacedName]gatev1.GatewayStatus{},
		httpRoutes:         map[ktypes.NamespacedName][]gatev1.RouteParentStatus{},
		grpcRoutes:         map[ktypes.NamespacedName][]gatev1.RouteParentStatus{},
		tcpRoutes:          map[ktypes.NamespacedName][]gatev1alpha2.RouteParentStatus{},
		tlsRoutes:          map[ktypes.NamespacedName][]gatev1.RouteParentStatus{},
		backendTLSPolicies: map[ktypes.NamespacedName][]gatev1.PolicyAncestorStatus{},
	}
}

func (r *statusReport) recordHTTPRouteParent(route ktypes.NamespacedName, parent gatev1.RouteParentStatus) {
	r.httpRoutes[route] = upsertRouteParent(r.httpRoutes[route], parent)
}

func (r *statusReport) recordGRPCRouteParent(route ktypes.NamespacedName, parent gatev1.RouteParentStatus) {
	r.grpcRoutes[route] = upsertRouteParent(r.grpcRoutes[route], parent)
}

func (r *statusReport) recordTCPRouteParent(route ktypes.NamespacedName, parent gatev1alpha2.RouteParentStatus) {
	r.tcpRoutes[route] = upsertRouteParent(r.tcpRoutes[route], parent)
}

func (r *statusReport) recordTLSRouteParent(route ktypes.NamespacedName, parent gatev1.RouteParentStatus) {
	r.tlsRoutes[route] = upsertRouteParent(r.tlsRoutes[route], parent)
}

// recordBackendTLSPolicyAncestor appends ancestor to the policy's entry, or
// replaces the existing ancestor with the same AncestorRef. Last-write-wins
// applies only within a single AncestorRef — distinct ancestors accumulate so
// that a policy referenced from routes attached to multiple Gateways ends up
// with one entry per Gateway in its persisted status.
func (r *statusReport) recordBackendTLSPolicyAncestor(policy ktypes.NamespacedName, ancestor gatev1.PolicyAncestorStatus) {
	ancestors := r.backendTLSPolicies[policy]
	for i := range ancestors {
		if parentRefEquals(ancestors[i].AncestorRef, ancestor.AncestorRef) {
			ancestors[i] = ancestor
			r.backendTLSPolicies[policy] = ancestors
			return
		}
	}
	r.backendTLSPolicies[policy] = append(ancestors, ancestor)
}

// upsertRouteParent appends parent to parents or replaces the entry whose
// ParentRef matches. Last-write-wins applies only within a single ParentRef.
func upsertRouteParent(parents []gatev1.RouteParentStatus, parent gatev1.RouteParentStatus) []gatev1.RouteParentStatus {
	for i := range parents {
		if parentRefEquals(parents[i].ParentRef, parent.ParentRef) {
			parents[i] = parent
			return parents
		}
	}
	return append(parents, parent)
}

// parentRefEquals compares two ParentReference values by their identifying
// fields. Used for both RouteParentStatus.ParentRef and
// PolicyAncestorStatus.AncestorRef — the underlying type is the same.
func parentRefEquals(a, b gatev1.ParentReference) bool {
	return ptr.Equal(a.Group, b.Group) &&
		ptr.Equal(a.Kind, b.Kind) &&
		ptr.Equal(a.Namespace, b.Namespace) &&
		a.Name == b.Name &&
		ptr.Equal(a.SectionName, b.SectionName) &&
		ptr.Equal(a.Port, b.Port)
}

// flushStatusReport sends every status write collected during the rebuild to
// the apiserver. Writes are ordered GatewayClass → Gateway → routes →
// BackendTLSPolicy so that bench observers polling Gateway.AttachedRoutes
// converge ahead of the long tail of per-route writes.
func (p *Provider) flushStatusReport(ctx context.Context, report *statusReport) {
	logger := log.Ctx(ctx)

	for name, status := range report.gatewayClasses {
		if err := p.client.UpdateGatewayClassStatus(ctx, name, status); err != nil {
			logger.Warn().Err(err).Str("gateway_class", name).Msg("Unable to update GatewayClass status")
		}
	}

	for name, status := range report.gateways {
		if err := p.client.UpdateGatewayStatus(ctx, name, status); err != nil {
			logger.Warn().Err(err).Str("gateway", name.Name).Str("namespace", name.Namespace).Msg("Unable to update Gateway status")
		}
	}

	for name, parents := range report.httpRoutes {
		status := gatev1.HTTPRouteStatus{RouteStatus: gatev1.RouteStatus{Parents: parents}}
		if err := p.client.UpdateHTTPRouteStatus(ctx, name, status); err != nil {
			logger.Warn().Err(err).Str("http_route", name.Name).Str("namespace", name.Namespace).Msg("Unable to update HTTPRoute status")
		}
	}

	for name, parents := range report.grpcRoutes {
		status := gatev1.GRPCRouteStatus{RouteStatus: gatev1.RouteStatus{Parents: parents}}
		if err := p.client.UpdateGRPCRouteStatus(ctx, name, status); err != nil {
			logger.Warn().Err(err).Str("grpc_route", name.Name).Str("namespace", name.Namespace).Msg("Unable to update GRPCRoute status")
		}
	}

	for name, parents := range report.tcpRoutes {
		status := gatev1alpha2.TCPRouteStatus{RouteStatus: gatev1alpha2.RouteStatus{Parents: parents}}
		if err := p.client.UpdateTCPRouteStatus(ctx, name, status); err != nil {
			logger.Warn().Err(err).Str("tcp_route", name.Name).Str("namespace", name.Namespace).Msg("Unable to update TCPRoute status")
		}
	}

	for name, parents := range report.tlsRoutes {
		status := gatev1.TLSRouteStatus{RouteStatus: gatev1.RouteStatus{Parents: parents}}
		if err := p.client.UpdateTLSRouteStatus(ctx, name, status); err != nil {
			logger.Warn().Err(err).Str("tls_route", name.Name).Str("namespace", name.Namespace).Msg("Unable to update TLSRoute status")
		}
	}

	for name, ancestors := range report.backendTLSPolicies {
		status := gatev1.PolicyStatus{Ancestors: ancestors}
		if err := p.client.UpdateBackendTLSPolicyStatus(ctx, name, status); err != nil {
			logger.Warn().Err(err).Str("backend_tls_policy", name.Name).Str("namespace", name.Namespace).Msg("Unable to update BackendTLSPolicy status")
		}
	}
}
