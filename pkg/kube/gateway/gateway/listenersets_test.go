/*
 * Copyright 2026 The Trickster Authors
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
	"path/filepath"
	"slices"
	"testing"

	"github.com/trickstercache/trickster/v2/pkg/kube/gateway/class"
	"github.com/trickstercache/trickster/v2/pkg/kube/gateway/ir"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"
	gwapiv1a2 "sigs.k8s.io/gateway-api/apis/v1alpha2"
)

func listenerSet(ns, name, parentNS, parent string, entries ...gwapiv1.ListenerEntry) *gwapiv1.ListenerSet {
	if parentNS == "" {
		parentNS = ns
	}
	pns := gwapiv1.Namespace(parentNS)
	return &gwapiv1.ListenerSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: gwapiv1.ListenerSetSpec{
			ParentRef: gwapiv1.ParentGatewayReference{
				Name:      gwapiv1.ObjectName(parent),
				Namespace: &pns,
			},
			Listeners: entries,
		},
	}
}

func httpEntry(name string, port int, from gwapiv1.FromNamespaces) gwapiv1.ListenerEntry {
	e := gwapiv1.ListenerEntry{
		Name: gwapiv1.SectionName(name), Port: gwapiv1.PortNumber(port),
		Protocol: gwapiv1.HTTPProtocolType,
	}
	if from != "" {
		e.AllowedRoutes = &gwapiv1.AllowedRoutes{Namespaces: &gwapiv1.RouteNamespaces{From: &from}}
	}
	return e
}

func httpsEntry(name string, port int, host, secret string, from gwapiv1.FromNamespaces) gwapiv1.ListenerEntry {
	h := gwapiv1.Hostname(host)
	mode := gwapiv1.TLSModeTerminate
	e := gwapiv1.ListenerEntry{
		Name: gwapiv1.SectionName(name), Port: gwapiv1.PortNumber(port),
		Protocol: gwapiv1.HTTPSProtocolType, Hostname: &h,
		TLS: &gwapiv1.ListenerTLSConfig{
			Mode:            &mode,
			CertificateRefs: []gwapiv1.SecretObjectReference{{Name: gwapiv1.ObjectName(secret)}},
		},
	}
	if from != "" {
		e.AllowedRoutes = &gwapiv1.AllowedRoutes{Namespaces: &gwapiv1.RouteNamespaces{From: &from}}
	}
	return e
}

func listenerSetStatus(report *ir.Report, ns, name string) ir.ListenerSetStatus {
	for _, s := range report.ListenerSets {
		if s.Source.Namespace == ns && s.Source.Name == name {
			return s
		}
	}
	return ir.ListenerSetStatus{}
}

func gatewayStatusNamed(report *ir.Report, name string) ir.GatewayStatus {
	for _, g := range report.Gateways {
		if g.Source.Name == name {
			return g
		}
	}
	return ir.GatewayStatus{}
}

// parentAccepted reports the Accepted status the report carries for one route parentRef
func parentAccepted(r *ir.RouteStatus, kind, ns, name string) bool {
	for _, p := range r.Parents {
		if p.Ref.Kind == kind && p.Ref.Namespace == ns && p.Ref.Name == name {
			c, ok := ir.Find(p.Conditions, "Accepted")
			return ok && c.Status
		}
	}
	return false
}

func TestTranslateListenerSets(t *testing.T) {
	// attached listeners merge with the inline ones on one port-claim pass, and routes attach to
	// their direct parent only
	require.Empty(t, golden(t, "listenersets"))
	model, report := translateReport(t, "listenersets")

	var names []string
	for _, l := range model.Listeners {
		names = append(names, l.Name)
	}
	require.ElementsMatch(t, []string{
		"Gateway/infra/gw/http", "Gateway/infra/gw/https",
		"ListenerSet/infra/extra/extra-http", "ListenerSet/infra/extra/extra-https",
	}, names)
	require.Len(t, model.Certs, 2)

	gw := gatewayStatusNamed(report, "gw")
	require.True(t, condOf(t, gw.Conditions, "Accepted").Status)
	require.Equal(t, 1, gw.AttachedListenerSets)

	lset := listenerSetStatus(report, "infra", "extra")
	require.True(t, condOf(t, lset.Conditions, "Accepted").Status)
	require.EqualValues(t, gwapiv1.ListenerSetReasonAccepted,
		condOf(t, lset.Conditions, "Accepted").Reason)
	require.True(t, condOf(t, lset.Conditions, "Programmed").Status)
	require.Len(t, lset.Listeners, 2)
	entries := map[string]ir.ListenerStatus{}
	for _, l := range lset.Listeners {
		entries[l.Name] = l
		require.True(t, condOf(t, l.Conditions, "Accepted").Status, l.Name)
	}
	require.Equal(t, 1, entries["extra-http"].AttachedRoutes, "only web attaches to :8080")
	require.Equal(t, 2, entries["extra-https"].AttachedRoutes, "web and api attach to :443")

	// web has two accepted parents, the Gateway and the ListenerSet; api names the set alone with
	// a sectionName, so the inline HTTPS listener never admits it
	var web, api ir.RouteStatus
	for _, r := range report.Routes {
		switch r.Source.Name {
		case "web":
			web = r
		case "api":
			api = r
		}
	}
	require.True(t, parentAccepted(&web, "", "infra", "gw"))
	require.True(t, parentAccepted(&web, ir.KindListenerSet, "infra", "extra"))
	require.True(t, parentAccepted(&api, ir.KindListenerSet, "infra", "extra"))

	// web reaches the inline :80 and the set's :8080 and :443; api reaches only extra-https
	webListeners := map[string]bool{}
	for _, r := range model.Routes {
		if r.Source.Name != "web" {
			continue
		}
		for _, l := range r.Listeners {
			webListeners[l] = true
		}
	}
	for _, want := range []string{
		"Gateway/infra/gw/http", "ListenerSet/infra/extra/extra-http",
		"ListenerSet/infra/extra/extra-https",
	} {
		require.True(t, webListeners[want], want)
	}
	require.False(t, webListeners["Gateway/infra/gw/https"],
		"the inline HTTPS hostname does not intersect web's hostnames")
	var apiRoute *ir.Route
	for i := range model.Routes {
		if model.Routes[i].Source.Name == "api" {
			apiRoute = &model.Routes[i]
		}
	}
	require.NotNil(t, apiRoute)
	require.Equal(t, []string{"ListenerSet/infra/extra/extra-https"}, apiRoute.Listeners)
}

func TestTranslateListenerSetNotAllowedByDefault(t *testing.T) {
	// without allowedListeners a Gateway admits no ListenerSet: its entries do not serve and a
	// route naming the set is refused, while the inline listeners are untouched
	c := load(t, filepath.Join("testdata", "basic.yaml"))
	c.listenerSets = append(c.listenerSets,
		listenerSet("infra", "extra", "", "gw", httpEntry("extra", 8080, gwapiv1.NamespacesFromAll)))
	parentNS := gwapiv1.Namespace("infra")
	kind := gwapiv1.Kind(ir.KindListenerSet)
	c.routes = append(c.routes, &gwapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "extra-web"},
		Spec: gwapiv1.HTTPRouteSpec{CommonRouteSpec: gwapiv1.CommonRouteSpec{
			ParentRefs: []gwapiv1.ParentReference{{
				Namespace: &parentNS, Name: "extra", Kind: &kind,
			}},
		}},
	})
	model, report, problems := Translate(Config{
		Cache: c, Claimer: class.New(controllerName, ""), Options: options(t),
	})
	lset := listenerSetStatus(report, "infra", "extra")
	accepted := condOf(t, lset.Conditions, "Accepted")
	require.False(t, accepted.Status)
	require.EqualValues(t, gwapiv1.ListenerSetReasonNotAllowed, accepted.Reason)
	require.False(t, condOf(t, lset.Conditions, "Programmed").Status)
	require.Len(t, lset.Listeners, 1)
	require.False(t, condOf(t, lset.Listeners[0].Conditions, "Programmed").Status)

	require.NotContains(t, modelListenerNames(model), "ListenerSet/infra/extra/extra")
	gw := gatewayStatusNamed(report, "gw")
	require.Equal(t, 0, gw.AttachedListenerSets, "a rejected set is not counted")

	containing(t, problems, ir.KindListenerSet+"/infra/extra", "does not allow ListenerSets")
	var extra ir.RouteStatus
	for _, r := range report.Routes {
		if r.Source.Name == "extra-web" {
			extra = r
		}
	}
	require.False(t, parentAccepted(&extra, ir.KindListenerSet, "infra", "extra"))
}

func TestTranslateListenerSetAdmissionModes(t *testing.T) {
	// Same admits only the Gateway's namespace; All admits a selected namespace as well
	fromSame := gwapiv1.NamespacesFromSame

	c := load(t, filepath.Join("testdata", "basic.yaml"))
	c.gateways[0].Spec.AllowedListeners = &gwapiv1.AllowedListeners{
		Namespaces: &gwapiv1.ListenerNamespaces{From: &fromSame},
	}
	c.listenerSets = append(c.listenerSets,
		listenerSet("team-a", "remote", "infra", "gw",
			httpEntry("remote", 8080, gwapiv1.NamespacesFromAll)))
	_, report, _ := Translate(Config{
		Cache: c, Claimer: class.New(controllerName, ""), Options: options(t),
	})
	require.False(t, condOf(t,
		listenerSetStatus(report, "team-a", "remote").Conditions, "Accepted").Status)

	all := gwapiv1.NamespacesFromAll
	c.gateways[0].Spec.AllowedListeners = &gwapiv1.AllowedListeners{
		Namespaces: &gwapiv1.ListenerNamespaces{From: &all},
	}
	_, report, _ = Translate(Config{
		Cache: c, Claimer: class.New(controllerName, ""), Options: options(t),
	})
	require.True(t, condOf(t,
		listenerSetStatus(report, "team-a", "remote").Conditions, "Accepted").Status)
	require.Equal(t, 1, gatewayStatusNamed(report, "gw").AttachedListenerSets)

	// Selector with the namespace unlabeled admits nothing; labeled it does
	sel := gwapiv1.NamespacesFromSelector
	c.gateways[0].Spec.AllowedListeners = &gwapiv1.AllowedListeners{
		Namespaces: &gwapiv1.ListenerNamespaces{From: &sel, Selector: &metav1.LabelSelector{
			MatchLabels: map[string]string{"team": "a"},
		}},
	}
	_, report, _ = Translate(Config{
		Cache: c, Claimer: class.New(controllerName, ""), Options: options(t),
	})
	require.False(t, condOf(t,
		listenerSetStatus(report, "team-a", "remote").Conditions, "Accepted").Status)
	c.namespaces["team-a"] = &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: "team-a", Labels: map[string]string{"team": "a"},
	}}
	_, report, _ = Translate(Config{
		Cache: c, Claimer: class.New(controllerName, ""), Options: options(t),
	})
	require.True(t, condOf(t,
		listenerSetStatus(report, "team-a", "remote").Conditions, "Accepted").Status)
}

func TestTranslateListenerSetBadParentRef(t *testing.T) {
	// a parentRef that cannot name a Gateway is reported once the name matches a claimed Gateway
	c := load(t, filepath.Join("testdata", "basic.yaml"))
	lset := listenerSet("infra", "wrong", "", "gw", httpEntry("wrong", 8080, ""))
	kind := gwapiv1.Kind("ConfigMap")
	lset.Spec.ParentRef.Kind = &kind
	c.listenerSets = append(c.listenerSets, lset)
	_, report, problems := Translate(Config{
		Cache: c, Claimer: class.New(controllerName, ""), Options: options(t),
	})
	st := listenerSetStatus(report, "infra", "wrong")
	accepted := condOf(t, st.Conditions, "Accepted")
	require.False(t, accepted.Status)
	require.EqualValues(t, gwapiv1.ListenerSetReasonInvalid, accepted.Reason)
	containing(t, problems, ir.KindListenerSet+"/infra/wrong",
		"kind gateway.networking.k8s.io/ConfigMap is not supported")
}

func TestTranslateListenerSetConflictAndDelete(t *testing.T) {
	// an entry repeating the inline HTTPS hostname with a different certificate conflicts; the
	// set stays attached through its good entry but says ListenersNotValid, and deleting the set
	// withdraws its listeners as of the next whole-world pass
	c := load(t, filepath.Join("testdata", "listenersets.yaml"))
	require.NotEmpty(t, c.listenerSets)
	c.listenerSets[0].Spec.Listeners = append(c.listenerSets[0].Spec.Listeners,
		httpsEntry("clash", 443, "inline.example.com", "extra-tls", gwapiv1.NamespacesFromAll))
	model, report, problems := Translate(Config{
		Cache: c, Claimer: class.New(controllerName, ""), Options: options(t),
	})
	containing(t, problems, ir.KindListenerSet+"/infra/extra", `listener "clash"`,
		"already served by Gateway/infra/gw")
	st := listenerSetStatus(report, "infra", "extra")
	require.True(t, condOf(t, st.Conditions, "Accepted").Status, "the good entries still attach")
	require.EqualValues(t, gwapiv1.ListenerSetReasonListenersNotValid,
		condOf(t, st.Conditions, "Accepted").Reason)
	require.False(t, condOf(t, st.Conditions, "Programmed").Status)
	var clash ir.ListenerStatus
	for _, l := range st.Listeners {
		if l.Name == "clash" {
			clash = l
		}
	}
	require.NotEmpty(t, clash.Name)
	require.True(t, condOf(t, clash.Conditions, "Conflicted").Status)
	require.False(t, condOf(t, clash.Conditions, "Accepted").Status)
	// the conflicting entry never enters the model, its sibling entries do
	require.NotContains(t, modelListenerNames(model), "ListenerSet/infra/extra/clash")
	require.Contains(t, modelListenerNames(model), "ListenerSet/infra/extra/extra-http")
	require.Equal(t, 1, gatewayStatusNamed(report, "gw").AttachedListenerSets)

	// a set whose every entry conflicts is not accepted and not counted
	c.listenerSets[0].Spec.Listeners = []gwapiv1.ListenerEntry{
		httpsEntry("clash", 443, "inline.example.com", "extra-tls", gwapiv1.NamespacesFromAll),
	}
	_, report, _ = Translate(Config{
		Cache: c, Claimer: class.New(controllerName, ""), Options: options(t),
	})
	st = listenerSetStatus(report, "infra", "extra")
	require.False(t, condOf(t, st.Conditions, "Accepted").Status)
	require.EqualValues(t, gwapiv1.ListenerSetReasonListenersNotValid,
		condOf(t, st.Conditions, "Accepted").Reason)
	require.Equal(t, 0, gatewayStatusNamed(report, "gw").AttachedListenerSets)

	// deleting the set withdraws every attached listener and its status on the next pass
	c.listenerSets = nil
	model, report, _ = Translate(Config{
		Cache: c, Claimer: class.New(controllerName, ""), Options: options(t),
	})
	require.Empty(t, report.ListenerSets)
	for _, n := range modelListenerNames(model) {
		require.NotContains(t, n, "ListenerSet/")
	}
	var certNames []string
	for _, c := range model.Certs {
		certNames = append(certNames, c.Name)
	}
	require.Contains(t, certNames, "infra/web-tls", "the inline listener keeps its certificate")
	require.NotContains(t, certNames, "infra/extra-tls",
		"the deleted set's certificate is withdrawn from the model")
	var web ir.RouteStatus
	for _, r := range report.Routes {
		if r.Source.Name == "web" {
			web = r
		}
	}
	require.True(t, parentAccepted(&web, "", "infra", "gw"),
		"the Gateway parentRef is unaffected by the set's deletion")
	require.False(t, parentAccepted(&web, ir.KindListenerSet, "infra", "extra"))
}

func TestTranslateListenerSetRouteNamespaces(t *testing.T) {
	// the entry's allowedRoutes decides which route namespaces attach, with the set's own
	// namespace standing in for the Gateway's
	fromSame := gwapiv1.NamespacesFromSame
	c := load(t, filepath.Join("testdata", "listenersets.yaml"))
	// replace the entries with one Same-namespace HTTP listener
	c.listenerSets[0].Spec.Listeners = []gwapiv1.ListenerEntry{
		httpEntry("local", 8080, fromSame),
	}
	_, report, _ := Translate(Config{
		Cache: c, Claimer: class.New(controllerName, ""), Options: options(t),
	})
	var web ir.RouteStatus
	for _, r := range report.Routes {
		if r.Source.Name == "web" {
			web = r
		}
	}
	// web lives in shop, the set in infra, so the default Same admission refuses it
	require.False(t, parentAccepted(&web, ir.KindListenerSet, "infra", "extra"))
	for _, p := range web.Parents {
		if p.Ref.Kind == ir.KindListenerSet {
			c, ok := ir.Find(p.Conditions, "Accepted")
			require.True(t, ok)
			require.False(t, c.Status)
			require.EqualValues(t, gwapiv1.RouteReasonNotAllowedByListeners, c.Reason)
		}
	}
}

func modelListenerNames(m *ir.IR) []string {
	var out []string
	for _, l := range m.Listeners {
		out = append(out, l.Name)
	}
	slices.Sort(out)
	return out
}

func tlsPassthroughEntry(name string, port int, host string, from gwapiv1.FromNamespaces) gwapiv1.ListenerEntry {
	h := gwapiv1.Hostname(host)
	mode := gwapiv1.TLSModePassthrough
	e := gwapiv1.ListenerEntry{
		Name: gwapiv1.SectionName(name), Port: gwapiv1.PortNumber(port),
		Protocol: gwapiv1.TLSProtocolType, Hostname: &h,
		TLS: &gwapiv1.ListenerTLSConfig{Mode: &mode},
	}
	if from != "" {
		e.AllowedRoutes = &gwapiv1.AllowedRoutes{Namespaces: &gwapiv1.RouteNamespaces{From: &from}}
	}
	return e
}

func TestTranslateListenerSetStreamRoute(t *testing.T) {
	// a passthrough TLS entry in another namespace, admitted with from: All, carries a stream
	// route that names the ListenerSet directly
	all := gwapiv1.NamespacesFromAll
	c := load(t, filepath.Join("testdata", "basic.yaml"))
	c.gateways[0].Spec.AllowedListeners = &gwapiv1.AllowedListeners{
		Namespaces: &gwapiv1.ListenerNamespaces{From: &all},
	}
	c.listenerSets = append(c.listenerSets,
		listenerSet("team-a", "streams", "infra", "gw",
			tlsPassthroughEntry("db", 8443, "db.example.com", all)))
	c.services["shop/db"] = &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "db"}}
	c.services["shop/db"].Spec.Ports = []corev1.ServicePort{{Name: "tls", Port: 8443}}
	setNS := gwapiv1.Namespace("team-a")
	setKind := gwapiv1.Kind(ir.KindListenerSet)
	c.tlsRoutes = append(c.tlsRoutes, &gwapiv1a2.TLSRoute{
		ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "db"},
		Spec: gwapiv1a2.TLSRouteSpec{CommonRouteSpec: gwapiv1.CommonRouteSpec{
			ParentRefs: []gwapiv1.ParentReference{{
				Namespace: &setNS, Name: "streams", Kind: &setKind,
			}},
		}, Hostnames: []gwapiv1.Hostname{"db.example.com"},
			Rules: []gwapiv1a2.TLSRouteRule{{BackendRefs: []gwapiv1a2.BackendRef{{
				Name: "db", Port: ptrTo(gwapiv1a2.PortNumber(8443)),
			}}}},
		},
	})
	model, report, problems := Translate(Config{
		Cache: c, Claimer: class.New(controllerName, ""), Options: options(t),
	})
	require.Empty(t, problems)
	require.Contains(t, modelListenerNames(model), "ListenerSet/team-a/streams/db")
	var stream *ir.Route
	for i := range model.Routes {
		if model.Routes[i].Source.Name == "db" {
			stream = &model.Routes[i]
		}
	}
	require.NotNil(t, stream)
	require.Equal(t, []string{"ListenerSet/team-a/streams/db"}, stream.Listeners)
	lset := listenerSetStatus(report, "team-a", "streams")
	require.True(t, condOf(t, lset.Conditions, "Accepted").Status)
	require.Equal(t, 1, lset.Listeners[0].AttachedRoutes)
}

func TestTranslateListenerSetOwnReferenceGrant(t *testing.T) {
	// the parent Gateway's ReferenceGrant does not cover a cross-namespace Secret a ListenerSet
	// entry names; a grant naming the ListenerSet itself resolves it
	all := gwapiv1.NamespacesFromAll
	c := load(t, filepath.Join("testdata", "grants.yaml"))
	var gwObj *gwapiv1.Gateway
	for _, g := range c.gateways {
		if g.Namespace == "infra" && g.Name == "gw" {
			gwObj = g
		}
	}
	require.NotNil(t, gwObj)
	gwObj.Spec.AllowedListeners = &gwapiv1.AllowedListeners{
		Namespaces: &gwapiv1.ListenerNamespaces{From: &all},
	}
	lockedNS := gwapiv1.Namespace("locked")
	entry := httpsEntry("locked", 8443, "locked.example.com", "forbidden-tls", all)
	entry.TLS.CertificateRefs[0].Namespace = &lockedNS
	c.listenerSets = append(c.listenerSets,
		listenerSet("infra", "locked-set", "infra", "gw", entry))
	_, report, problems := Translate(Config{
		Cache: c, Claimer: class.New(controllerName, ""), Options: options(t),
	})
	lset := listenerSetStatus(report, "infra", "locked-set")
	require.True(t, condOf(t, lset.Conditions, "Accepted").Status)
	entryStatus := lset.Listeners[0]
	require.False(t, condOf(t, entryStatus.Conditions, "ResolvedRefs").Status,
		"the Gateway's grant does not permit the ListenerSet's Secret")
	require.EqualValues(t, gwapiv1.ListenerReasonRefNotPermitted,
		condOf(t, entryStatus.Conditions, "ResolvedRefs").Reason)
	containing(t, problems, ir.KindListenerSet+"/infra/locked-set", "not permitted by any ReferenceGrant")

	// once the locked namespace grants the ListenerSet itself, the reference resolves
	c.grants = append(c.grants, &gwapiv1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Namespace: "locked", Name: "allow-lset"},
		Spec: gwapiv1.ReferenceGrantSpec{From: []gwapiv1.ReferenceGrantFrom{{
			Group: gwapiv1.GroupName, Kind: ir.KindListenerSet, Namespace: "infra",
		}}, To: []gwapiv1.ReferenceGrantTo{{Group: "", Kind: "Secret"}}},
	})
	_, report, problems = Translate(Config{
		Cache: c, Claimer: class.New(controllerName, ""), Options: options(t),
	})
	lset = listenerSetStatus(report, "infra", "locked-set")
	entryStatus = lset.Listeners[0]
	require.True(t, condOf(t, entryStatus.Conditions, "ResolvedRefs").Status)
	for _, p := range problems {
		require.NotContains(t, p.Detail, "locked-set")
	}
}

func ptrTo[T any](v T) *T { return &v }
