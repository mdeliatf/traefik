package gateway

import (
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ktypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	gatev1 "sigs.k8s.io/gateway-api/apis/v1"
)

func TestStatusReport_recordBackendTLSPolicyAncestor(t *testing.T) {
	policy := ktypes.NamespacedName{Namespace: "ns", Name: "policy"}

	ancestor := func(gateway, section string) gatev1.PolicyAncestorStatus {
		return gatev1.PolicyAncestorStatus{
			AncestorRef: gatev1.ParentReference{
				Group:       ptr.To(gatev1.Group(groupGateway)),
				Kind:        ptr.To(gatev1.Kind(kindGateway)),
				Namespace:   ptr.To(gatev1.Namespace("ns")),
				Name:        gatev1.ObjectName(gateway),
				SectionName: ptr.To(gatev1.SectionName(section)),
			},
			ControllerName: controllerName,
			Conditions: []metav1.Condition{{
				Type:    string(gatev1.PolicyConditionAccepted),
				Status:  metav1.ConditionTrue,
				Reason:  string(gatev1.PolicyReasonAccepted),
				Message: gateway + "/" + section,
			}},
		}
	}

	t.Run("distinct AncestorRefs accumulate", func(t *testing.T) {
		report := newStatusReport()
		report.recordBackendTLSPolicyAncestor(policy, ancestor("gw-a", "http"))
		report.recordBackendTLSPolicyAncestor(policy, ancestor("gw-b", "https"))

		assert.Equal(t, []gatev1.PolicyAncestorStatus{
			ancestor("gw-a", "http"),
			ancestor("gw-b", "https"),
		}, report.backendTLSPolicies[policy])
	})

	t.Run("same AncestorRef upserts in place", func(t *testing.T) {
		report := newStatusReport()
		first := ancestor("gw-a", "http")
		report.recordBackendTLSPolicyAncestor(policy, first)

		updated := first
		updated.Conditions = []metav1.Condition{{
			Type:    string(gatev1.PolicyConditionAccepted),
			Status:  metav1.ConditionFalse,
			Reason:  string(gatev1.PolicyReasonConflicted),
			Message: "conflicted",
		}}
		report.recordBackendTLSPolicyAncestor(policy, updated)

		assert.Equal(t, []gatev1.PolicyAncestorStatus{updated}, report.backendTLSPolicies[policy])
	})

	t.Run("policies are independent", func(t *testing.T) {
		report := newStatusReport()
		other := ktypes.NamespacedName{Namespace: "ns", Name: "other"}
		report.recordBackendTLSPolicyAncestor(policy, ancestor("gw-a", "http"))
		report.recordBackendTLSPolicyAncestor(other, ancestor("gw-b", "https"))

		assert.Equal(t, []gatev1.PolicyAncestorStatus{ancestor("gw-a", "http")}, report.backendTLSPolicies[policy])
		assert.Equal(t, []gatev1.PolicyAncestorStatus{ancestor("gw-b", "https")}, report.backendTLSPolicies[other])
	})
}

func TestStatusReport_recordRouteParent(t *testing.T) {
	route := ktypes.NamespacedName{Namespace: "ns", Name: "route"}

	parent := func(gateway string) gatev1.RouteParentStatus {
		return gatev1.RouteParentStatus{
			ParentRef: gatev1.ParentReference{
				Group:     ptr.To(gatev1.Group(groupGateway)),
				Kind:      ptr.To(gatev1.Kind(kindGateway)),
				Namespace: ptr.To(gatev1.Namespace("ns")),
				Name:      gatev1.ObjectName(gateway),
			},
			ControllerName: controllerName,
			Conditions: []metav1.Condition{{
				Type:    string(gatev1.RouteConditionAccepted),
				Status:  metav1.ConditionTrue,
				Reason:  string(gatev1.RouteReasonAccepted),
				Message: gateway,
			}},
		}
	}

	t.Run("distinct ParentRefs accumulate", func(t *testing.T) {
		report := newStatusReport()
		report.recordHTTPRouteParent(route, parent("gw-a"))
		report.recordHTTPRouteParent(route, parent("gw-b"))

		assert.Equal(t, []gatev1.RouteParentStatus{parent("gw-a"), parent("gw-b")}, report.httpRoutes[route])
	})

	t.Run("same ParentRef upserts in place", func(t *testing.T) {
		report := newStatusReport()
		first := parent("gw-a")
		report.recordHTTPRouteParent(route, first)

		updated := first
		updated.Conditions = []metav1.Condition{{
			Type:   string(gatev1.RouteConditionAccepted),
			Status: metav1.ConditionFalse,
			Reason: string(gatev1.RouteReasonNoMatchingParent),
		}}
		report.recordHTTPRouteParent(route, updated)

		assert.Equal(t, []gatev1.RouteParentStatus{updated}, report.httpRoutes[route])
	})

	t.Run("route maps are independent across kinds", func(t *testing.T) {
		report := newStatusReport()
		report.recordHTTPRouteParent(route, parent("gw-http"))
		report.recordGRPCRouteParent(route, parent("gw-grpc"))
		report.recordTLSRouteParent(route, parent("gw-tls"))
		report.recordTCPRouteParent(route, parent("gw-tcp"))

		assert.Equal(t, []gatev1.RouteParentStatus{parent("gw-http")}, report.httpRoutes[route])
		assert.Equal(t, []gatev1.RouteParentStatus{parent("gw-grpc")}, report.grpcRoutes[route])
		assert.Equal(t, []gatev1.RouteParentStatus{parent("gw-tls")}, report.tlsRoutes[route])
		assert.Equal(t, []gatev1.RouteParentStatus{parent("gw-tcp")}, report.tcpRoutes[route])
	})
}

func TestParentRefEquals(t *testing.T) {
	base := gatev1.ParentReference{
		Group:       ptr.To(gatev1.Group(groupGateway)),
		Kind:        ptr.To(gatev1.Kind(kindGateway)),
		Namespace:   ptr.To(gatev1.Namespace("ns")),
		Name:        "gw",
		SectionName: ptr.To(gatev1.SectionName("http")),
	}

	t.Run("equal copies", func(t *testing.T) {
		other := base
		assert.True(t, parentRefEquals(base, other))
	})

	t.Run("name differs", func(t *testing.T) {
		other := base
		other.Name = "other"
		assert.False(t, parentRefEquals(base, other))
	})

	t.Run("section name differs", func(t *testing.T) {
		other := base
		other.SectionName = ptr.To(gatev1.SectionName("https"))
		assert.False(t, parentRefEquals(base, other))
	})

	t.Run("nil vs set pointer", func(t *testing.T) {
		other := base
		other.SectionName = nil
		assert.False(t, parentRefEquals(base, other))
	})

	t.Run("port differs", func(t *testing.T) {
		withPort := base
		withPort.Port = ptr.To(gatev1.PortNumber(80))
		other := withPort
		other.Port = ptr.To(gatev1.PortNumber(443))
		assert.False(t, parentRefEquals(withPort, other))
	})
}
