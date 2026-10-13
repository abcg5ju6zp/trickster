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
	"fmt"

	"github.com/trickstercache/trickster/v2/pkg/kube/gateway/internal/translate"
	"github.com/trickstercache/trickster/v2/pkg/kube/gateway/ir"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// listenerSetReasonParentNotProgrammed is the Programmed reason the API defines for a set whose
// parent Gateway is not programmed; this release's types spell it in the spec comments without a
// constant, so the vocabulary is carried literally
const listenerSetReasonParentNotProgrammed = gwapiv1.ListenerSetConditionReason("ParentNotProgrammed")

// listenerSetState is one ListenerSet that named a claimed Gateway, whether or not the Gateway's
// allowedListeners admitted it
type listenerSetState struct {
	src    ir.Source
	parent *gatewayState
	// served is true once its parentRef is valid, its parent Gateway is served, and the
	// Gateway's allowedListeners select its namespace; only then do its entries claim ports
	served bool
	// accepted is whether the set's Accepted condition is true: served and at least one entry
	// accepted
	accepted bool
	// acceptReason and acceptMsg explain a set-level refusal; empty for an admitted set
	acceptReason string
	acceptMsg    string
	// entries are the declared listeners, in declaration order
	entries []gwapiv1.ListenerEntry
	// listeners are the accepted entries, in declaration order
	listeners []*listenerState
	// status is completed once every route has attached
	status *ir.ListenerSetStatus
}

// listenerSetSelector parses a Gateway's allowedListeners into the admission it applies; an
// absent or unparsable selector admits nothing, as allowedRoutes' selector handling does
func (t *translator) listenerSetSelector(src ir.Source, al *gwapiv1.AllowedListeners,
) (gwapiv1.FromNamespaces, labels.Selector) {
	if al == nil || al.Namespaces == nil || al.Namespaces.From == nil {
		// the default is None: a Gateway admits no ListenerSet unless it says so
		return gwapiv1.NamespacesFromNone, nil
	}
	from := *al.Namespaces.From
	switch from {
	case gwapiv1.NamespacesFromAll, gwapiv1.NamespacesFromSame, gwapiv1.NamespacesFromNone:
		return from, nil
	case gwapiv1.NamespacesFromSelector:
		if al.Namespaces.Selector == nil {
			t.reject(src, "allowedListeners.namespaces.from is Selector but no selector is set; "+
				"no ListenerSet is admitted")
			return gwapiv1.NamespacesFromNone, nil
		}
		sel, err := metav1.LabelSelectorAsSelector(al.Namespaces.Selector)
		if err != nil {
			t.reject(src, "allowedListeners.namespaces.selector is invalid: %s; no "+
				"ListenerSet is admitted", err)
			return gwapiv1.NamespacesFromNone, nil
		}
		return from, sel
	default:
		t.reject(src, "allowedListeners.namespaces.from %q is not recognized; no "+
			"ListenerSet is admitted", from)
		return gwapiv1.NamespacesFromNone, nil
	}
}

// allowsListenerSet reports whether a Gateway admits a ListenerSet from the named namespace
// under its parsed allowedListeners
func (gw *gatewayState) allowsListenerSet(t *translator, namespace string) bool {
	switch gw.allowedFrom {
	case gwapiv1.NamespacesFromAll:
		return true
	case gwapiv1.NamespacesFromSame:
		return gw.src.Namespace == namespace
	case gwapiv1.NamespacesFromSelector:
		ns := t.cfg.Cache.Namespace(namespace)
		return ns != nil && gw.allowedSelector != nil &&
			gw.allowedSelector.Matches(labels.Set(ns.Labels))
	default:
		return false
	}
}

// indexListenerSets discovers every ListenerSet naming a claimed Gateway, decides whether the
// Gateway admits it, and orders the admitted sets per parent; sets are read oldest first and the
// per-parent subsequence therefore already holds the API's precedence (creation time, then
// namespace/name)
func (t *translator) indexListenerSets() {
	for _, obj := range translate.ByAge(t.cfg.Cache.ListenerSets()) {
		t.indexListenerSet(obj)
	}
}

func (t *translator) indexListenerSet(obj *gwapiv1.ListenerSet) {
	src := translate.Source(ir.KindListenerSet, obj)
	ref := obj.Spec.ParentRef
	group := translate.GroupOf(ref.Group)
	if group == "" {
		// ParentGatewayReference defaults an absent group to the Gateway API group, unlike a core
		// reference such as a Secret's
		group = gwapiv1.GroupName
	}
	kind := translate.KindOf(ref.Kind, kindGateway)
	ns := translate.NamespaceOf(ref.Namespace, obj.Namespace)
	state := &listenerSetState{
		src:     src,
		entries: obj.Spec.Listeners,
		status:  &ir.ListenerSetStatus{Source: src},
	}
	gw := t.gateways[translate.NamespacedName(ns, string(ref.Name))]
	if gw == nil {
		// the parent Gateway is absent, out of scope, or not claimed by this controller; nothing
		// of ours speaks for this ListenerSet
		return
	}
	state.parent = gw
	gw.listenerSets = append(gw.listenerSets, state)
	gw.reportsListenerSets = true
	t.listenerSets[translate.NamespacedName(src.Namespace, src.Name)] = state
	t.listenerSetOrder = append(t.listenerSetOrder, state)
	switch {
	case group != gwapiv1.GroupName || kind != kindGateway:
		state.acceptReason = string(gwapiv1.ListenerSetReasonInvalid)
		state.acceptMsg = fmt.Sprintf("parentRef %s/%s: kind %s/%s is not supported",
			ns, ref.Name, group, kind)
		t.reject(src, "%s", state.acceptMsg)
	case !gw.served:
		state.acceptReason = string(gwapiv1.ListenerSetReasonParentNotAccepted)
		state.acceptMsg = fmt.Sprintf("parent Gateway %s is not accepted: %s",
			gw.src.Key(), gw.refusal)
		t.reject(src, "%s", state.acceptMsg)
	case !gw.allowsListenerSet(t, src.Namespace):
		state.acceptReason = string(gwapiv1.ListenerSetReasonNotAllowed)
		state.acceptMsg = fmt.Sprintf("parent Gateway %s does not allow ListenerSets from "+
			"namespace %q in its allowedListeners", gw.src.Key(), src.Namespace)
		t.reject(src, "%s", state.acceptMsg)
	default:
		state.served = true
	}
}

// finishListenerSets counts attached routes and completes each set's conditions; it returns the
// number of sets accepted on the Gateway, which its status reports
func (t *translator) finishListenerSets(gw *gatewayState) int {
	var attached int
	for _, lset := range gw.listenerSets {
		for _, l := range lset.listeners {
			lset.status.Listeners[l.statusIdx].AttachedRoutes = l.attached
		}
		total := len(lset.status.Listeners)
		valid := len(lset.listeners)
		switch {
		case !lset.served:
			lset.status.Conditions = refusedListenerSetConditions(lset)
		case total == 0:
			// the API requires at least one listener; an object without one programs nothing
			lset.status.Conditions = []ir.Condition{
				condition(gwapiv1.ListenerSetConditionAccepted, false,
					gwapiv1.ListenerSetReasonListenersNotValid, "the ListenerSet names no listeners"),
				condition(gwapiv1.ListenerSetConditionProgrammed, false,
					gwapiv1.ListenerSetReasonListenersNotValid, "the ListenerSet names no listeners"),
			}
		case valid == total:
			lset.accepted = true
			lset.status.Conditions = []ir.Condition{
				condition(gwapiv1.ListenerSetConditionAccepted, true,
					gwapiv1.ListenerSetReasonAccepted, "the ListenerSet is attached"),
				condition(gwapiv1.ListenerSetConditionProgrammed, true,
					gwapiv1.ListenerSetReasonProgrammed, "every listener is programmed"),
			}
		default:
			// one or more entries failed conflict, TLS or protocol checks; with none left the set
			// itself is not accepted either
			msg := fmt.Sprintf("%d of %d listeners are not valid", total-valid, total)
			accepted := valid > 0
			lset.accepted = accepted
			lset.status.Conditions = []ir.Condition{
				condition(gwapiv1.ListenerSetConditionAccepted, accepted,
					gwapiv1.ListenerSetReasonListenersNotValid, msg),
				condition(gwapiv1.ListenerSetConditionProgrammed, false,
					gwapiv1.ListenerSetReasonListenersNotValid, msg),
			}
		}
		if lset.accepted {
			attached++
		}
	}
	return attached
}

// refusedListenerSetConditions builds the two conditions of a set that did not attach, mapping
// each set-level refusal onto the reason vocabulary the ListenerSet API defines for it
func refusedListenerSetConditions(lset *listenerSetState) []ir.Condition {
	accepted := condition(gwapiv1.ListenerSetConditionAccepted, false,
		gwapiv1.ListenerSetConditionReason(lset.acceptReason), lset.acceptMsg)
	reason := gwapiv1.ListenerSetReasonInvalid
	switch gwapiv1.ListenerSetConditionReason(lset.acceptReason) {
	case gwapiv1.ListenerSetReasonParentNotAccepted:
		reason = listenerSetReasonParentNotProgrammed
	case gwapiv1.ListenerSetReasonListenersNotValid:
		reason = gwapiv1.ListenerSetReasonListenersNotValid
	}
	msg := lset.acceptMsg
	if msg == "" {
		msg = "no listener is valid"
	}
	programmed := condition(gwapiv1.ListenerSetConditionProgrammed, false, reason, msg)
	return []ir.Condition{accepted, programmed}
}
