/*
Copyright 2026 Apoxy, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	"fmt"
	"strings"
)

// EgressListenerShape encodes how the egextension server should rewrite
// each Envoy listener generated for an EgressListener entry. The shape
// travels from the controller to the extension via the Gateway listener
// name (the only stable channel — the extension does not read the
// EgressGateway CR), as a "<egListenerName>--<shape>" suffix.
type EgressListenerShape string

const (
	EgressShapeHTTP           EgressListenerShape = "http"
	EgressShapeHTTPS          EgressListenerShape = "https"
	EgressShapeTLSTerminate   EgressListenerShape = "tls-terminate"
	EgressShapeTLSPassthrough EgressListenerShape = "tls-passthrough"
	EgressShapeTCP            EgressListenerShape = "tcp"
)

// listenerNameSep separates the EgressListener name from its shape suffix
// in the Gateway-API listener section name. Permitted in RFC1123 labels
// and unlikely to clash with operator-chosen names.
const listenerNameSep = "--"

// ShapeForListener resolves a CRD EgressListener to its on-the-wire shape.
// Returns ("", error) for unsupported combinations — callers surface the
// error on Status.Conditions[Ready] and skip Gateway materialization.
func ShapeForListener(l EgressListener) (EgressListenerShape, error) {
	switch l.Protocol {
	case EgressProtocolHTTP:
		return EgressShapeHTTP, nil
	case EgressProtocolHTTPS:
		return EgressShapeHTTPS, nil
	case EgressProtocolTLS:
		mode := EgressTLSPassthrough
		if l.TLS != nil && l.TLS.Mode != "" {
			mode = l.TLS.Mode
		}
		switch mode {
		case EgressTLSTerminate:
			return EgressShapeTLSTerminate, nil
		case EgressTLSPassthrough:
			return EgressShapeTLSPassthrough, nil
		default:
			return "", fmt.Errorf("listener %q: invalid TLS mode %q", l.Name, mode)
		}
	case EgressProtocolTCP:
		return EgressShapeTCP, nil
	case EgressProtocolUDP:
		return "", fmt.Errorf("listener %q: protocol UDP is not yet supported", l.Name)
	default:
		return "", fmt.Errorf("listener %q: unknown protocol %q", l.Name, l.Protocol)
	}
}

// EncodeListenerSectionName builds the Gateway-API listener section name
// the controller stamps and the extension parses back: "<name>--<shape>".
func EncodeListenerSectionName(name string, shape EgressListenerShape) string {
	return name + listenerNameSep + string(shape)
}

// ParseListenerSectionName splits a section name produced by
// EncodeListenerSectionName back into (egListenerName, shape). Returns
// ok=false when the suffix is missing or unrecognized.
func ParseListenerSectionName(section string) (string, EgressListenerShape, bool) {
	idx := strings.LastIndex(section, listenerNameSep)
	if idx < 0 {
		return "", "", false
	}
	name := section[:idx]
	shape := EgressListenerShape(section[idx+len(listenerNameSep):])
	switch shape {
	case EgressShapeHTTP, EgressShapeHTTPS, EgressShapeTLSTerminate,
		EgressShapeTLSPassthrough, EgressShapeTCP:
		return name, shape, true
	default:
		return "", "", false
	}
}

// ShapePriority orders listener shapes for tie-breaking at dial time when
// multiple listeners catch the same destination port. Higher value wins.
// TLS-terminate / HTTPS go first because they sniff and can carry any byte
// stream; plain TCP is last-resort because it hard-commits the connection
// to passthrough.
func ShapePriority(s EgressListenerShape) int {
	switch s {
	case EgressShapeTLSTerminate:
		return 5
	case EgressShapeHTTPS:
		return 4
	case EgressShapeHTTP:
		return 3
	case EgressShapeTLSPassthrough:
		return 2
	case EgressShapeTCP:
		return 1
	default:
		return 0
	}
}
