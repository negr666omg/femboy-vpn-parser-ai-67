//go:build linux

package core

import "testing"

func TestDefaultRouteUsesExpectedInterface(t *testing.T) {
	if !defaultRouteUsesInterface("default via 192.0.2.1 dev swm123 metric 100\n", "swm123") {
		t.Fatal("expected route through the isolated macvlan to pass")
	}
	if defaultRouteUsesInterface("default via 192.0.2.1 dev tun0 metric 100\n", "swm123") {
		t.Fatal("route through a different interface must fail")
	}
	if defaultRouteUsesInterface("10.0.0.0/24 dev swm123 proto kernel\n", "swm123") {
		t.Fatal("a connected route must not be accepted as the default route")
	}
}
