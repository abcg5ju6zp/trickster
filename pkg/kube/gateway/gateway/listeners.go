/*
 * Copyright 2018 The Trickster Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gateway

import (
	"errors"
	"fmt"
	"slices"
	"strconv"

	"github.com/trickstercache/trickster/v2/pkg/kube/gateway/internal/translate"
	"github.com/trickstercache/trickster/v2/pkg/kube/gateway/ir"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// listenerState is one accepted Gateway listener
type listenerState struct {
	// name is the listener's IR identity
	name string
	// owner is the object that declared the listener: the Gateway for an inline listener or the
	// ListenerSet that attached one
	owner ir.Source
	// lset is the ListenerSet an attached listener belongs to, nil for an inline listener
	lset *listenerSetState
	// section is the declaring object's own name for it, which a parentRef selects
	section  string
	port     int
	protocol string
	hostname string
	allowed  allowedRoutes
	// attached counts the routes accepted on this listener
	attached int
	// statusIdx locates the listener's entry in its Gateway's or ListenerSet's status
	statusIdx int
}

// listenerDecl is the common shape of a Gateway Listener and a ListenerSet ListenerEntry, which
// carry the same fields, so both are lowered through one path
type listenerDecl struct {
	section  string
	port     gwapiv1.PortNumber
	protocol gwapiv1.ProtocolType
	hostname *gwapiv1.Hostname
	tls      *gwapiv1.ListenerTLSConfig
	allowed  *gwapiv1.AllowedRoutes
}

func gatewayListenerDecl(l gwapiv1.Listener) listenerDecl {
	return listenerDecl{
		section: string(l.Name), port: l.Port, protocol: l.Protocol,
		hostname: l.Hostname, tls: l.TLS, allowed: l.AllowedRoutes,
	}
}

func listenerEntryDecl(l gwapiv1.ListenerEntry) listenerDecl {
	return listenerDecl{
		section: string(l.Name), port: l.Port, protocol: l.Protocol,
		hostname: l.Hostname, tls: l.TLS, allowed: l.AllowedRoutes,
	}
}

// allowedRoutes is a listener's admission policy for routes
type allowedRoutes struct {
	from     gwapiv1.FromNamespaces
	selector labels.Selector
	// kinds are the route kinds admitted: those the listener's protocol serves, narrowed by
	// allowedRoutes.kinds
	kinds map[string]bool
}

// routeKinds maps each listener protocol to the route kinds this controller serves on it
var routeKinds = map[string][]string{
	ir.ProtocolHTTP:  {kindHTTPRoute, kindGRPCRoute},
	ir.ProtocolHTTPS: {kindHTTPRoute, kindGRPCRoute},
	ir.ProtocolTCP:   {kindTCPRoute},
	ir.ProtocolTLS:   {kindTLSRoute},
	ir.ProtocolUDP:   {kindUDPRoute},
}

func (a allowedRoutes) admits(kind, gatewayNS, routeNS string, ns *corev1.Namespace) bool {
	if !a.kinds[kind] {
		return false
	}
	switch a.from {
	case gwapiv1.NamespacesFromAll:
		return true
	case gwapiv1.NamespacesFromSame:
		return gatewayNS == routeNS
	case gwapiv1.NamespacesFromSelector:
		return ns != nil && a.selector != nil && a.selector.Matches(labels.Set(ns.Labels))
	}
	return false
}

func condition[T ~string, R ~string](typ T, status bool, reason R, msg string) ir.Condition {
	return ir.Condition{Type: string(typ), Status: status, Reason: string(reason), Message: msg}
}

// verdict accumulates a listener's conditions while it is translated; a listener starts accepted
// and resolved, and each problem found lowers exactly the condition that describes it
type verdict struct {
	accepted   ir.Condition
	resolved   ir.Condition
	conflicted ir.Condition
	kinds      []string
	// served is false once any problem keeps the listener out of the data plane
	served bool
}

func newVerdict() *verdict {
	return &verdict{
		accepted: condition(gwapiv1.ListenerConditionAccepted, true,
			gwapiv1.ListenerReasonAccepted, "the listener is served"),
		resolved: condition(gwapiv1.ListenerConditionResolvedRefs, true,
			gwapiv1.ListenerReasonResolvedRefs, "every reference resolved"),
		conflicted: condition(gwapiv1.ListenerConditionConflicted, false,
			gwapiv1.ListenerReasonNoConflicts, "no conflict with another listener"),
		served: true,
	}
}

func (v *verdict) refuse(c *ir.Condition, reason gwapiv1.ListenerConditionReason, msg string) {
	c.Status = false
	c.Reason = string(reason)
	c.Message = msg
	v.served = false
}

func (v *verdict) conflict(reason gwapiv1.ListenerConditionReason, msg string) {
	v.conflicted = condition(gwapiv1.ListenerConditionConflicted, true, reason, msg)
	v.refuse(&v.accepted, reason, msg)
}

func (v *verdict) unserved() {
	v.served = false
}

func (v *verdict) unresolved(reason gwapiv1.ListenerConditionReason, msg string) {
	if !v.resolved.Status {
		return
	}
	v.resolved = condition(gwapiv1.ListenerConditionResolvedRefs, false, reason, msg)
}

func (v *verdict) status(section string) ir.ListenerStatus {
	programmed := condition(gwapiv1.ListenerConditionProgrammed, true,
		gwapiv1.ListenerReasonProgrammed, "the listener is programmed")
	if !v.served {
		programmed = condition(gwapiv1.ListenerConditionProgrammed, false,
			gwapiv1.ListenerReasonInvalid, "the listener is not served")
	}
	return ir.ListenerStatus{
		Name: section, SupportedKinds: v.kinds,
		Conditions: []ir.Condition{v.accepted, programmed, v.resolved, v.conflicted},
	}
}

func gatewayConditions(total, valid int) []ir.Condition {
	accepted := condition(gwapiv1.GatewayConditionAccepted, true,
		gwapiv1.GatewayReasonAccepted, "the gateway is served")
	programmed := condition(gwapiv1.GatewayConditionProgrammed, true,
		gwapiv1.GatewayReasonProgrammed, "the gateway is programmed")
	switch {
	case total == 0 || valid == total:
	case valid > 0:
		accepted.Reason = string(gwapiv1.GatewayReasonListenersNotValid)
		accepted.Message = fmt.Sprintf("%d of %d listeners are not valid", total-valid, total)
	default:
		accepted = condition(gwapiv1.GatewayConditionAccepted, false,
			gwapiv1.GatewayReasonListenersNotValid, "no listener is valid")
		programmed = condition(gwapiv1.GatewayConditionProgrammed, false,
			gwapiv1.GatewayReasonInvalid, "the gateway is not served")
	}
	return []ir.Condition{accepted, programmed}
}

func (t *translator) buildGateways() *portClaims {
	claims := newPortClaims()
	for _, gw := range translate.ByAge(t.cfg.Cache.Gateways()) {
		class := string(gw.Spec.GatewayClassName)
		src := translate.Source(ir.KindGateway, gw)
		st, claimed := t.classes[class]
		if !claimed {
			continue
		}
		state := &gatewayState{
			src: src, served: st.served,
			status: &ir.GatewayStatus{Source: src},
		}
		// a cache policy on the Gateway is written over its class's parameters
		state.policy, _ = t.policies.Merge(st.policy,
			t.policies.Attach(translate.TargetGateway, gw.Namespace, gw.Name, ""))
		if gw.Spec.AllowedListeners != nil {
			// its status reports the attached count once the Gateway asks for ListenerSets
			state.reportsListenerSets = true
			state.allowedFrom, state.allowedSelector = t.listenerSetSelector(src,
				gw.Spec.AllowedListeners)
		}
		t.gatewayOrder = append(t.gatewayOrder, state)
		if !st.served {
			// the Gateway stays claimed: its listeners are described and the routes naming it are
			// told why they are not served, rather than left with whatever status they had
			state.refusal = fmt.Sprintf("gatewayClass %q is not accepted because its parameters "+
				"cannot be honored; the gateway is not served", class)
			t.reject(src, "%s", state.refusal)
			state.status.Conditions = []ir.Condition{
				condition(gwapiv1.GatewayConditionAccepted, false,
					gwapiv1.GatewayReasonInvalidParameters, state.refusal),
				condition(gwapiv1.GatewayConditionProgrammed, false,
					gwapiv1.GatewayReasonInvalid, "the gateway is not served"),
			}
		}
		if p := gw.Spec.Infrastructure; state.served && p != nil && p.ParametersRef != nil {
			// nothing reads Gateway parameters, so a Gateway that depends on them is refused
			// rather than served without them
			state.served = false
			state.refusal = fmt.Sprintf("spec.infrastructure.parametersRef %s/%s %q is not "+
				"supported; the gateway is not served", p.ParametersRef.Group, p.ParametersRef.Kind,
				p.ParametersRef.Name)
			t.reject(src, "%s", state.refusal)
			state.status.Conditions = []ir.Condition{
				condition(gwapiv1.GatewayConditionAccepted, false,
					gwapiv1.GatewayReasonInvalidParameters, state.refusal),
				condition(gwapiv1.GatewayConditionProgrammed, false,
					gwapiv1.GatewayReasonInvalid, "the gateway is not served"),
			}
		}
		state.inline = gw.Spec.Listeners
		if gw.Spec.TLS != nil {
			t.reject(src, "spec.tls is not supported and is ignored")
		}
		if len(gw.Spec.Addresses) > 0 {
			t.reject(src, "spec.addresses is not supported; addresses come from "+
				"kubernetes.published_service")
		}
		t.gateways[translate.NamespacedName(gw.Namespace, gw.Name)] = state
	}
	return claims
}

// lowerListeners lowers, in the Gateway API's merge precedence, every inline listener and then
// its attached ListenerSets' entries, Gateway by Gateway oldest first; one portClaims spans the
// pass, so an attached entry meets the same port, protocol, hostname and certificate rules as an
// inline listener or a listener of another Gateway
func (t *translator) lowerListeners(claims *portClaims) {
	for _, gw := range t.gatewayOrder {
		for _, l := range gw.inline {
			if ls := t.gatewayListener(gw, l, claims); ls != nil {
				gw.listeners = append(gw.listeners, ls)
			}
		}
		for _, lset := range gw.listenerSets {
			t.lowerListenerSet(lset, claims)
		}
	}
}

func (t *translator) gatewayListener(gw *gatewayState, l gwapiv1.Listener, claims *portClaims,
) *listenerState {
	v := newVerdict()
	ls := t.lowerListener(gw.src, gatewayListenerDecl(l), gw.served, claims, v)
	gw.status.Listeners = append(gw.status.Listeners, v.status(string(l.Name)))
	if ls != nil {
		ls.statusIdx = len(gw.status.Listeners) - 1
	}
	return ls
}

// lowerListenerSet lowers one attached set's entries into its status and the model; a set the
// Gateway did not admit is still described, but its entries claim nothing and reach no route
func (t *translator) lowerListenerSet(lset *listenerSetState, claims *portClaims) {
	for _, e := range lset.entries {
		v := newVerdict()
		ls := t.lowerListener(lset.src, listenerEntryDecl(e), lset.served, claims, v)
		lset.status.Listeners = append(lset.status.Listeners, v.status(string(e.Name)))
		if ls != nil {
			ls.lset = lset
			ls.statusIdx = len(lset.status.Listeners) - 1
			lset.listeners = append(lset.listeners, ls)
		}
	}
}

func (t *translator) refuseListener(v *verdict, c *ir.Condition, src ir.Source,
	reason gwapiv1.ListenerConditionReason, format string, args ...any,
) {
	msg := fmt.Sprintf(format, args...)
	v.refuse(c, reason, msg)
	t.reject(src, "%s", msg)
}

// listenerProtocols maps the Gateway API listener protocols this controller serves to the IR's
var listenerProtocols = map[gwapiv1.ProtocolType]string{
	gwapiv1.HTTPProtocolType:  ir.ProtocolHTTP,
	gwapiv1.HTTPSProtocolType: ir.ProtocolHTTPS,
	gwapiv1.TCPProtocolType:   ir.ProtocolTCP,
	gwapiv1.TLSProtocolType:   ir.ProtocolTLS,
	gwapiv1.UDPProtocolType:   ir.ProtocolUDP,
}

// transport returns the port space a protocol binds in, since a TCP and a UDP listener may share
// a port number without conflict
func transport(protocol string) string {
	if protocol == ir.ProtocolUDP {
		return ir.ProtocolUDP
	}
	return ir.ProtocolTCP
}

// lowerListener lowers one inline listener or ListenerSet entry. owner is the object that
// declared it and active whether it may claim a port this pass: an unserved Gateway's listener
// and an unadmitted ListenerSet's entry are described in status but claim nothing
func (t *translator) lowerListener(owner ir.Source, l listenerDecl,
	active bool, claims *portClaims, v *verdict,
) *listenerState {
	src := owner
	section := l.section
	protocol, ok := listenerProtocols[l.protocol]
	if !ok {
		t.refuseListener(v, &v.accepted, src, gwapiv1.ListenerReasonUnsupportedProtocol,
			"listener %q: protocol %q is not supported", section, l.protocol)
		return nil
	}
	v.kinds = routeKinds[protocol]
	var hostname string
	if l.hostname != nil && *l.hostname != "" {
		if protocol == ir.ProtocolTCP || protocol == ir.ProtocolUDP {
			// nothing in a TCP or UDP stream names a host, so a hostname cannot be honored
			t.refuseListener(v, &v.accepted, src, gwapiv1.ListenerReasonUnsupportedValue,
				"listener %q: a %s listener cannot have a hostname", section, l.protocol)
			return nil
		}
		h, err := translate.Hostname(string(*l.hostname), translate.HostnameRequired)
		if err != nil {
			t.refuseListener(v, &v.accepted, src, gwapiv1.ListenerReasonUnsupportedValue,
				"listener %q: hostname %q is not routable: %s", section, *l.hostname, err)
			return nil
		}
		hostname = h
	}
	port := int(l.port)
	portKey := transport(protocol) + ":" + strconv.Itoa(port)
	if !active {
		// described but not served: it claims no port, collects no certificate and
		// reaches no route, so it cannot conflict with a listener that does serve
		v.unserved()
		return &listenerState{
			owner: owner, name: src.Key() + "/" + section, section: section,
			port: port, protocol: protocol, hostname: hostname,
			allowed: t.allowedRoutes(src, section, l.allowed, v),
		}
	}
	if existing, ok := claims.protocols[portKey]; ok && existing != protocol {
		msg := fmt.Sprintf("listener %q: port %d already serves %s and cannot also serve %s",
			section, port, existing, protocol)
		v.conflict(gwapiv1.ListenerReasonProtocolConflict, msg)
		t.reject(src, "%s", msg)
		return nil
	}
	// listeners of different objects merge on a port, each admitting its own routes, since a port
	// is one socket whichever declares it; a hostname repeated within one object conflicts
	hostKey := portKey + "|" + hostname
	selfKey := src.Key() + "|" + hostKey
	if _, repeated := claims.owners[selfKey]; repeated {
		t.hostnameConflict(v, src, section, hostname, port, src)
		return nil
	}
	var certs []string
	switch protocol {
	case ir.ProtocolHTTPS:
		var ok bool
		if certs, ok = t.listenerCerts(owner, l, v); !ok {
			return nil
		}
	case ir.ProtocolTLS:
		if !t.passthrough(owner, l, v) {
			return nil
		}
	default:
		if l.tls != nil {
			t.reject(src, "listener %q: tls is ignored on a %s listener", section, l.protocol)
		}
	}
	claimOwner, taken := claims.owners[hostKey]
	if taken && claimOwner.conflicts(protocol, hostname, certs) {
		t.hostnameConflict(v, src, section, hostname, port, claimOwner.src)
		return nil
	}
	// what a certificate answers for is asked of the store this listener shares, since unusable
	// material leaves a store serving what it held
	here := ir.Listener{Port: port, Protocol: protocol}
	identity := func(cert string) ir.CertIdentity { return t.certs.Identity(here, cert) }
	if msg := certConflict(claims, portKey, port, section, certs, identity); msg != "" {
		v.conflict(gwapiv1.ListenerReasonHostnameConflict, msg)
		t.reject(src, "%s", msg)
		return nil
	}
	claims.protocols[portKey] = protocol
	claims.owners[selfKey] = listenerOwner{src: src}
	if !taken {
		claims.owners[hostKey] = listenerOwner{src: src, certs: certs}
	}
	claims.claimNames(portKey, src, section, certs, identity)
	ls := &listenerState{
		owner: owner, name: src.Key() + "/" + section, section: section,
		port: port, protocol: protocol, hostname: hostname,
		allowed: t.allowedRoutes(src, section, l.allowed, v),
	}
	t.model.Listeners = append(t.model.Listeners, ir.Listener{
		Name: ls.name, Section: section, Port: port, Protocol: protocol,
		Hostname: hostname, CertRefs: certs, Source: src,
	})
	return ls
}

// portClaims is what the served listeners have claimed so far: the protocol on each port, the
// owner of each hostname on it, and the certificate answering for each TLS name on it
type portClaims struct {
	protocols map[string]string
	owners    map[string]listenerOwner
	names     map[string]nameClaim
}

func newPortClaims() *portClaims {
	return &portClaims{
		protocols: make(map[string]string),
		owners:    make(map[string]listenerOwner),
		names:     make(map[string]nameClaim),
	}
}

// listenerOwner is the first listener to claim a hostname on a port, and what a later
// claimant is measured against
type listenerOwner struct {
	src   ir.Source
	certs []string
}

// nameClaim is the certificate first installed for a TLS name on a port, and the listener that
// brought it; the store serving the port answers for the name with that certificate
type nameClaim struct {
	src     ir.Source
	section string
	cert    string
	digest  string
}

// certConflict explains the first TLS name the listener's certificates would answer for on the
// port that another listener's different certificate already answers for, or nothing
func certConflict(claims *portClaims, portKey string, port int, section string, certs []string,
	identity func(string) ir.CertIdentity,
) string {
	for _, cert := range certs {
		id := identity(cert)
		for _, name := range id.Names {
			claim, taken := claims.names[portKey+"|"+name]
			if !taken || claim.digest == id.Digest {
				continue
			}
			return fmt.Sprintf("listener %q: certificate %s answers for %q on port %d, which "+
				"listener %q of %s already answers for from certificate %s", section, cert, name,
				port, claim.section, claim.src.Key(), claim.cert)
		}
	}
	return ""
}

// claimNames records the TLS names the listener's certificates answer for on the port, the first
// certificate carrying a name keeping it, as the store serving the port would
func (c *portClaims) claimNames(portKey string, src ir.Source, section string, certs []string,
	identity func(string) ir.CertIdentity,
) {
	for _, cert := range certs {
		id := identity(cert)
		for _, name := range id.Names {
			key := portKey + "|" + name
			if _, taken := c.names[key]; !taken {
				c.names[key] = nameClaim{src: src, section: section, cert: cert, digest: id.Digest}
			}
		}
	}
}

func (t *translator) hostnameConflict(v *verdict, src ir.Source, section, hostname string,
	port int, owner ir.Source,
) {
	msg := fmt.Sprintf("listener %q: hostname %q on port %d is already served by %s",
		section, hostname, port, owner.Key())
	v.conflict(gwapiv1.ListenerReasonHostnameConflict, msg)
	t.reject(src, "%s", msg)
}

func (o listenerOwner) conflicts(protocol, hostname string, certs []string) bool {
	if protocol != ir.ProtocolHTTPS || hostname == "" {
		return false
	}
	return !slices.Equal(slices.Sorted(slices.Values(o.certs)), slices.Sorted(slices.Values(certs)))
}

func (t *translator) listenerCerts(owner ir.Source, l listenerDecl, v *verdict,
) ([]string, bool) {
	// a listener that cannot terminate TLS is refused, an unresolved Secret is
	// only reported
	src := owner
	section := l.section
	if l.tls == nil {
		t.refuseListener(v, &v.accepted, src, gwapiv1.ListenerReasonUnsupportedValue,
			"listener %q: an HTTPS listener requires a tls block", section)
		return nil, false
	}
	if l.tls.Mode != nil && *l.tls.Mode != gwapiv1.TLSModeTerminate {
		t.refuseListener(v, &v.accepted, src, gwapiv1.ListenerReasonUnsupportedValue,
			"listener %q: tls mode %q is not supported", section, *l.tls.Mode)
		return nil, false
	}
	if len(l.tls.CertificateRefs) == 0 {
		t.refuseListener(v, &v.resolved, src, gwapiv1.ListenerReasonInvalidCertificateRef,
			"listener %q: tls names no certificateRefs", section)
		return nil, false
	}
	var out []string
	for _, ref := range l.tls.CertificateRefs {
		group, kind := translate.GroupOf(ref.Group), translate.KindOf(ref.Kind, kindSecret)
		if group != "" || kind != kindSecret {
			msg := fmt.Sprintf("listener %q: certificateRef kind %s/%s is not supported",
				section, group, kind)
			v.unresolved(gwapiv1.ListenerReasonInvalidCertificateRef, msg)
			t.reject(src, "%s", msg)
			continue
		}
		ns, name := translate.NamespaceOf(ref.Namespace, src.Namespace), string(ref.Name)
		if ns != src.Namespace && !t.grants.permits(
			// a ListenerSet's grants are its own: the parent Gateway's never carry over
			reference{group: gwapiv1.GroupName, kind: src.Kind, namespace: src.Namespace},
			reference{group: "", kind: kindSecret, namespace: ns, name: name}) {
			msg := fmt.Sprintf("listener %q: certificateRef %s/%s is not permitted by any "+
				"ReferenceGrant", section, ns, name)
			v.unresolved(gwapiv1.ListenerReasonRefNotPermitted, msg)
			t.reject(src, "%s", msg)
			continue
		}
		key, err := t.certs.Collect(t.cfg.Cache, &t.model, src, ns, name)
		if err != nil {
			msg := fmt.Sprintf("listener %q: tls secret %q: %s", section, key, err)
			v.unresolved(gwapiv1.ListenerReasonInvalidCertificateRef, msg)
			t.problems.RejectAs(ir.ReasonInvalidCertificate, src, "%s", msg)
			if errors.Is(err, translate.ErrSecretNotFound) {
				continue
			}
			// a Secret that exists but cannot be served stays referenced, so the certificate
			// the listener already serves under it is not withdrawn over a bad rotation
		}
		out = append(out, key)
	}
	return out, true
}

func (t *translator) passthrough(owner ir.Source, l listenerDecl, v *verdict) bool {
	// a TLS listener relays the handshake to the backend, so it holds no certificate of its own
	// and cannot terminate; the mode defaults to Terminate, which is refused rather than assumed
	src, section := owner, l.section
	if l.tls == nil || l.tls.Mode == nil || *l.tls.Mode != gwapiv1.TLSModePassthrough {
		t.refuseListener(v, &v.accepted, src, gwapiv1.ListenerReasonUnsupportedValue,
			"listener %q: a TLS listener is served in tls mode Passthrough only", section)
		return false
	}
	if len(l.tls.CertificateRefs) > 0 {
		t.reject(src, "listener %q: certificateRefs are ignored in tls mode Passthrough", section)
	}
	return true
}

func (t *translator) allowedRoutes(src ir.Source, section string,
	ar *gwapiv1.AllowedRoutes, v *verdict,
) allowedRoutes {
	out := allowedRoutes{from: gwapiv1.NamespacesFromSame, kinds: make(map[string]bool, len(v.kinds))}
	for _, k := range v.kinds {
		out.kinds[k] = true
	}
	if ar == nil {
		return out
	}
	if ar.Namespaces != nil {
		if ar.Namespaces.From != nil {
			out.from = *ar.Namespaces.From
		}
		switch out.from {
		case gwapiv1.NamespacesFromAll, gwapiv1.NamespacesFromSame, gwapiv1.NamespacesFromNone:
		case gwapiv1.NamespacesFromSelector:
			if ar.Namespaces.Selector == nil {
				t.reject(src, "listener %q: allowedRoutes.namespaces.from is Selector "+
					"but no selector is set; no route is admitted", section)
				out.from = gwapiv1.NamespacesFromNone
				break
			}
			sel, err := metav1.LabelSelectorAsSelector(ar.Namespaces.Selector)
			if err != nil {
				t.reject(src, "listener %q: allowedRoutes.namespaces.selector is invalid: %s",
					section, err)
				out.from = gwapiv1.NamespacesFromNone
				break
			}
			out.selector = sel
		default:
			t.reject(src, "listener %q: allowedRoutes.namespaces.from %q is not "+
				"recognized; no route is admitted", section, out.from)
			out.from = gwapiv1.NamespacesFromNone
		}
	}
	if len(ar.Kinds) > 0 {
		// a kind the protocol does not carry is refused, as an HTTPRoute on a TCP listener is
		supported := v.kinds
		clear(out.kinds)
		v.kinds = nil
		for _, k := range ar.Kinds {
			group := gwapiv1.GroupName
			if k.Group != nil && *k.Group != "" {
				group = string(*k.Group)
			}
			if group == gwapiv1.GroupName && slices.Contains(supported, string(k.Kind)) {
				if !out.kinds[string(k.Kind)] {
					v.kinds = append(v.kinds, string(k.Kind))
				}
				out.kinds[string(k.Kind)] = true
				continue
			}
			msg := fmt.Sprintf("listener %q: route kind %s/%s is not supported on the listener",
				section, group, k.Kind)
			v.unresolved(gwapiv1.ListenerReasonInvalidRouteKinds, msg)
			t.reject(src, "%s", msg)
		}
		if len(out.kinds) == 0 {
			t.reject(src, "listener %q: allowedRoutes.kinds admits no kind this "+
				"controller serves on the listener", section)
		}
	}
	return out
}
