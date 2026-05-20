package gateway

import (
	"context"

	"github.com/rs/zerolog/log"
	ktypes "k8s.io/apimachinery/pkg/types"
	gatev1 "sigs.k8s.io/gateway-api/apis/v1"
	gatev1alpha2 "sigs.k8s.io/gateway-api/apis/v1alpha2"
)

// statusReport collects the status writes produced by a single rebuild so they
// can be flushed to the apiserver after the dynamic configuration has been
// published. Entries are last-write-wins per resource: this matches the prior
// inline-write behavior where the final call for a given resource determined
// the persisted status.
type statusReport struct {
	gatewayClasses     map[string]gatev1.GatewayClassStatus
	gateways           map[ktypes.NamespacedName]gatev1.GatewayStatus
	httpRoutes         map[ktypes.NamespacedName]gatev1.HTTPRouteStatus
	grpcRoutes         map[ktypes.NamespacedName]gatev1.GRPCRouteStatus
	tcpRoutes          map[ktypes.NamespacedName]gatev1alpha2.TCPRouteStatus
	tlsRoutes          map[ktypes.NamespacedName]gatev1.TLSRouteStatus
	backendTLSPolicies map[ktypes.NamespacedName]gatev1.PolicyStatus
}

func newStatusReport() *statusReport {
	return &statusReport{
		gatewayClasses:     map[string]gatev1.GatewayClassStatus{},
		gateways:           map[ktypes.NamespacedName]gatev1.GatewayStatus{},
		httpRoutes:         map[ktypes.NamespacedName]gatev1.HTTPRouteStatus{},
		grpcRoutes:         map[ktypes.NamespacedName]gatev1.GRPCRouteStatus{},
		tcpRoutes:          map[ktypes.NamespacedName]gatev1alpha2.TCPRouteStatus{},
		tlsRoutes:          map[ktypes.NamespacedName]gatev1.TLSRouteStatus{},
		backendTLSPolicies: map[ktypes.NamespacedName]gatev1.PolicyStatus{},
	}
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

	for name, status := range report.httpRoutes {
		if err := p.client.UpdateHTTPRouteStatus(ctx, name, status); err != nil {
			logger.Warn().Err(err).Str("http_route", name.Name).Str("namespace", name.Namespace).Msg("Unable to update HTTPRoute status")
		}
	}

	for name, status := range report.grpcRoutes {
		if err := p.client.UpdateGRPCRouteStatus(ctx, name, status); err != nil {
			logger.Warn().Err(err).Str("grpc_route", name.Name).Str("namespace", name.Namespace).Msg("Unable to update GRPCRoute status")
		}
	}

	for name, status := range report.tcpRoutes {
		if err := p.client.UpdateTCPRouteStatus(ctx, name, status); err != nil {
			logger.Warn().Err(err).Str("tcp_route", name.Name).Str("namespace", name.Namespace).Msg("Unable to update TCPRoute status")
		}
	}

	for name, status := range report.tlsRoutes {
		if err := p.client.UpdateTLSRouteStatus(ctx, name, status); err != nil {
			logger.Warn().Err(err).Str("tls_route", name.Name).Str("namespace", name.Namespace).Msg("Unable to update TLSRoute status")
		}
	}

	for name, status := range report.backendTLSPolicies {
		if err := p.client.UpdateBackendTLSPolicyStatus(ctx, name, status); err != nil {
			logger.Warn().Err(err).Str("backend_tls_policy", name.Name).Str("namespace", name.Namespace).Msg("Unable to update BackendTLSPolicy status")
		}
	}
}
