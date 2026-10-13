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

// Package gateway translates claimed Gateway API objects (GatewayClass, Gateway, HTTPRoute, GRPCRoute,
// TCPRoute, TLSRoute, UDPRoute, ReferenceGrant, BackendTLSPolicy, XBackendTrafficPolicy) into the IR
package gateway

import (
	kubecfg "github.com/trickstercache/trickster/v2/pkg/config/kubernetes"
	"github.com/trickstercache/trickster/v2/pkg/kube/gateway/cachepolicy"
	"github.com/trickstercache/trickster/v2/pkg/kube/gateway/class"
	"github.com/trickstercache/trickster/v2/pkg/kube/gateway/internal/translate"
	"github.com/trickstercache/trickster/v2/pkg/kube/gateway/ir"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"
	gwapiv1a2 "sigs.k8s.io/gateway-api/apis/v1alpha2"
	gwapix "sigs.k8s.io/gateway-api/apisx/v1alpha1"
)

// Cache is the read side of the watch layer this translator reads; every lookup is served from
// an informer cache, so a whole translation pass makes no API calls
type Cache interface {
	GatewayClasses() []*gwapiv1.GatewayClass
	Gateways() []*gwapiv1.Gateway
	ListenerSets() []*gwapiv1.ListenerSet
	// ListenerSet returns one ListenerSet, or nil when absent or out of scope; the nil answer
	// distinguishes a deleted direct parent from one parented to an unclaimed Gateway
	ListenerSet(namespace, name string) *gwapiv1.ListenerSet
	HTTPRoutes() []*gwapiv1.HTTPRoute
	GRPCRoutes() []*gwapiv1.GRPCRoute
	TCPRoutes() []*gwapiv1a2.TCPRoute
	TLSRoutes() []*gwapiv1a2.TLSRoute
	UDPRoutes() []*gwapiv1a2.UDPRoute
	ReferenceGrants() []*gwapiv1.ReferenceGrant
	BackendTLSPolicies() []*gwapiv1.BackendTLSPolicy
	BackendTrafficPolicies() []*gwapix.XBackendTrafficPolicy
	translate.CoreCache
	ConfigMap(namespace, name string) *corev1.ConfigMap
	// KeySecret returns a Secret labeled as holding a session token key, or nil
	KeySecret(namespace, name string) *corev1.Secret
	Namespace(name string) *corev1.Namespace
}

// Config carries the translator's inputs
type Config struct {
	// Cache reads the watched objects
	Cache Cache
	// Claimer decides which GatewayClasses, and so which Gateways, belong
	// to this controller
	Claimer *class.Claimer
	// Options is the validated kubernetes configuration section
	Options *kubecfg.Options
	// KnownNames reports what the running configuration defines that a
	// GatewayClass's parameters may name. Nil accepts any name.
	KnownNames func() ir.ConfiguredNames
	// CertJudge judges a TLS Secret's material and reads what its certificate answers for; nil
	// parses it on every pass
	CertJudge func(*corev1.Secret) (ir.CertIdentity, error)
	// CertServing answers what serves under a Secret on a listener when its material is unusable,
	// since a store keeps what it holds; nil means nothing does
	CertServing func(ir.Listener, string) (ir.CertIdentity, bool)
	// Policies are the cache policies read this pass, judged and indexed by target; nil
	// governs nothing
	Policies *cachepolicy.Index
}

// Problem is one thing the translator could not do
type Problem = ir.Problem

// Translate builds the IR for every claimed Gateway and the HTTPRoutes attached to them, and the
// status report describing what became of every claimed object
func Translate(cfg Config) (*ir.IR, *ir.Report, []Problem) {
	if cfg.Cache == nil || cfg.Claimer == nil || cfg.Options == nil {
		return &ir.IR{}, &ir.Report{}, nil
	}
	t := &translator{
		cfg:          cfg,
		certs:        translate.CertRefs{Judge: cfg.CertJudge, Serving: cfg.CertServing},
		problems:     translate.NewProblems(true),
		gateways:     make(map[string]*gatewayState),
		listenerSets: make(map[string]*listenerSetState),
		claims:       make(map[string]ir.Source),
	}
	var source translate.PolicySource
	if cfg.Policies != nil {
		source = cfg.Policies
	}
	t.policies = translate.NewPolicies(&t.model, source, t.problems)
	return t.run()
}

type translator struct {
	cfg   Config
	model ir.IR
	// problems deduplicates: one rule resolved once per listener and
	// hostname would otherwise report one unresolvable reference many times
	problems *translate.Problems
	// known is what the running configuration defines that parameters may
	// name
	known ir.ConfiguredNames
	// policies holds every policy the model names, from class parameters and cache policies,
	// and mints the combinations a rule governed by several needs
	policies *translate.Policies
	// grants answers whether a cross-namespace reference is permitted
	grants *grantIndex
	// tlsPolicies answers which BackendTLSPolicy governs a Service port
	tlsPolicies *backendTLSIndex
	// traffic holds the session each Service's XBackendTrafficPolicy asks for, by namespace/name
	traffic map[string]trafficSession
	// classes holds every claimed GatewayClass: the policy its parameters
	// produced, and whether its Gateways are served at all
	classes map[string]classState
	// gateways holds every claimed Gateway by namespace/name, with the
	// listeners that were accepted
	gateways map[string]*gatewayState
	// listenerSets holds every ListenerSet attaching to a claimed Gateway, by namespace/name,
	// whether or not its parent's allowedListeners admitted it
	listenerSets map[string]*listenerSetState
	// listenerSetOrder holds every ListenerSet a claimed Gateway parents, in namespace/name
	// order, so the report lists them deterministically
	listenerSetOrder []*listenerSetState
	// certs records each referenced TLS Secret once, however many listeners name it
	certs translate.CertRefs
	// claims maps a router entry the translator has awarded to the route
	// that owns it, so a duplicate can be refused and reported
	claims map[string]ir.Source
	// report is what is written back to the claimed objects
	report ir.Report
	// gatewayOrder holds every claimed Gateway, served or not, oldest first, so the report
	// lists them in one order however the cache yielded them
	gatewayOrder []*gatewayState
	// routeReports holds one status entry per HTTPRoute naming a claimed Gateway
	routeReports []*routeReport
	// cookies holds the named session cookies claimed so far, by name, so a younger route cannot
	// set one an older route's ALB sets for the same host on any listener
	cookies map[string][]cookieClaim
}

// gatewayState is one claimed Gateway and what survived of its listeners
type gatewayState struct {
	src ir.Source
	// policy names the IR policy the Gateway's class parameters produced,
	// or nothing
	policy string
	// served is false for a Gateway of a class whose parameters cannot be honored, and refusal
	// says so for the routes naming it
	served  bool
	refusal string
	// listeners are the accepted inline listeners, in declaration order
	listeners []*listenerState
	// inline are the Gateway's declared listeners, lowered after the ListenerSets that select
	// the Gateway have been indexed, so inline and attached listeners share one precedence pass
	inline []gwapiv1.Listener
	// allowedFrom and allowedSelector are the parsed spec.allowedListeners.namespaces admission
	// for selecting ListenerSets; absent allowedListeners admits none
	allowedFrom     gwapiv1.FromNamespaces
	allowedSelector labels.Selector
	// listenerSets are the ListenerSets that selected this Gateway, in attachment precedence
	// (oldest first, then namespace/name), whether or not the Gateway admitted them
	listenerSets []*listenerSetState
	// reportsListenerSets is true once the Gateway configured allowedListeners or a ListenerSet
	// selects it, so its status carries the attached count even when it is zero
	reportsListenerSets bool
	// status is the Gateway's status entry, completed once every route has attached
	status *ir.GatewayStatus
}

func (t *translator) reject(s ir.Source, format string, args ...any) {
	t.problems.Reject(s, format, args...)
}

func (t *translator) run() (*ir.IR, *ir.Report, []Problem) {
	if t.cfg.KnownNames != nil {
		t.known = t.cfg.KnownNames()
	}
	t.grants = indexGrants(t.cfg.Cache.ReferenceGrants())
	t.tlsPolicies = t.indexBackendTLS()
	t.traffic = t.indexBackendTraffic()
	t.classes = t.claimClasses()
	if len(t.classes) == 0 {
		return &ir.IR{}, &t.report, t.problems.List()
	}
	claims := t.buildGateways()
	// ListenerSets are indexed, admitted and ordered before any listener is lowered, so inline
	// and attached listeners meet one port-claim pass in the API's merge precedence
	t.indexListenerSets()
	t.lowerListeners(claims)
	t.buildRoutes()
	t.buildStreamRoutes()
	t.finish()
	return &t.model, &t.report, t.problems.List()
}

func (t *translator) finish() {
	for _, gw := range t.gatewayOrder {
		for _, l := range gw.listeners {
			gw.status.Listeners[l.statusIdx].AttachedRoutes = l.attached
		}
		attachedSets := t.finishListenerSets(gw)
		if gw.served {
			gw.status.Conditions = gatewayConditions(len(gw.status.Listeners), len(gw.listeners))
		}
		if gw.reportsListenerSets {
			gw.status.AttachedListenerSets = attachedSets
		}
		t.report.Gateways = append(t.report.Gateways, *gw.status)
	}
	for _, lset := range t.listenerSetOrder {
		t.report.ListenerSets = append(t.report.ListenerSets, *lset.status)
	}
	for _, r := range t.routeReports {
		t.report.Routes = append(t.report.Routes, r.status())
	}
}
