//go:build linux

package core

import (
	"net"
	"strings"
	"testing"
)

func TestParseDefaultRoute(t *testing.T) {
	gateway, source, err := parseDefaultRoute("default via 192.168.1.75 proto dhcp src 192.168.1.87 metric 100\n")
	if err != nil {
		t.Fatal(err)
	}
	if gateway != "192.168.1.75" {
		t.Fatalf("gateway = %q, want 192.168.1.75", gateway)
	}
	if source != "192.168.1.87" {
		t.Fatalf("source = %q, want 192.168.1.87", source)
	}
}

func TestParseDirectDefaultRoute(t *testing.T) {
	gateway, source, err := parseDefaultRoute("default dev ppp0 scope link\n")
	if err != nil {
		t.Fatal(err)
	}
	if gateway != "" || source != "" {
		t.Fatalf("direct route returned gateway=%q source=%q", gateway, source)
	}
}

func TestParseDefaultRouteRejectsMissingOrInvalidRoute(t *testing.T) {
	if _, _, err := parseDefaultRoute("192.168.1.0/24 dev enp1s0\n"); err == nil {
		t.Fatal("expected missing default route to fail")
	}
	if _, _, err := parseDefaultRoute("default via not-an-ip dev enp1s0\n"); err == nil {
		t.Fatal("expected invalid gateway to fail")
	}
}

func TestParseRouteNetworksAndOverlap(t *testing.T) {
	routes := parseRouteNetworks(strings.Join([]string{
		"10.254.12.0/30 dev swh123 proto kernel scope link",
		"local 10.254.13.1 dev swh456 table local proto kernel scope host",
		"192.168.1.0/24 dev enp1s0 proto kernel scope link",
	}, "\n"))
	if len(routes) != 3 {
		t.Fatalf("parsed %d routes, want 3", len(routes))
	}
	candidate := mustCIDR(t, "10.254.12.0/30")
	if !overlapsAnyRoute(candidate, routes) {
		t.Fatal("candidate overlapping an existing route was not rejected")
	}
	free := mustCIDR(t, "10.254.14.0/30")
	if overlapsAnyRoute(free, routes) {
		t.Fatal("unused candidate unexpectedly overlaps an existing route")
	}
}

func TestNftRulesUseOnlyTheSelectedInterfaceAndNamespaceIP(t *testing.T) {
	ns := &NetworkNamespace{
		hostVeth:      "swh123",
		ProxyIP:       "10.254.12.2",
		firewallTable: "subwatch123",
	}
	rules := ns.nftRules("enp1s0")
	for _, want := range []string{
		"table ip subwatch123",
		"iifname \"swh123\" oifname \"enp1s0\" ip saddr 10.254.12.2",
		"iifname \"enp1s0\" oifname \"swh123\" ip daddr 10.254.12.2 ct state established,related",
		"oifname \"enp1s0\" ip saddr 10.254.12.2 counter masquerade",
	} {
		if !strings.Contains(rules, want) {
			t.Errorf("nft rules do not contain %q:\n%s", want, rules)
		}
	}
	if strings.Contains(rules, "macvlan") || strings.Contains(rules, "dhclient") {
		t.Fatalf("legacy second-MAC/DHCP isolation leaked into firewall rules: %s", rules)
	}
}

func mustCIDR(t *testing.T, value string) *net.IPNet {
	t.Helper()
	_, network, err := net.ParseCIDR(value)
	if err != nil {
		t.Fatal(err)
	}
	return network
}
