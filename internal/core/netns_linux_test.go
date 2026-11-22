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

func TestParseDefaultRouteForInterface(t *testing.T) {
	routes := "default via 10.0.0.1 dev tun0 table 51820\n" +
		"default via 192.168.1.75 dev enp1s0 table 100\n"
	gateway, source, err := parseDefaultRouteForInterface(routes, "enp1s0")
	if err != nil {
		t.Fatal(err)
	}
	if gateway != "192.168.1.75" {
		t.Fatalf("gateway = %q, want 192.168.1.75", gateway)
	}
	if source != "" {
		t.Fatalf("source = %q, want empty", source)
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

func TestParseRouteNetworksUsesOnlyDestinationField(t *testing.T) {
	routes := strings.Join([]string{
		"default via 192.168.1.75 dev enp1s0 src 192.168.1.87",
		"0.0.0.0/1 dev tun0",
		"128.0.0.0/1 dev tun0",
		"192.168.1.0/24 dev enp1s0 src 192.168.1.87",
		"local 192.168.1.87 dev enp1s0 table local",
		"broadcast 192.168.1.255 dev enp1s0 table local",
	}, "\n")
	parsed := parseRouteNetworks(routes)
	if len(parsed) != 4 {
		t.Fatalf("parsed %d destination routes, want 4: %#v", len(parsed), parsed)
	}
	for _, route := range parsed {
		if route.Contains(net.ParseIP("192.168.1.75")) && route.String() == "192.168.1.75/32" {
			t.Fatal("default route gateway was incorrectly treated as a reserved destination")
		}
		if route.Contains(net.ParseIP("192.168.1.87")) && route.String() == "192.168.1.87/32" {
			// This address is valid because it is explicitly the destination
			// of the "local" route, not merely the source address of default.
			continue
		}
	}
}

func TestNetworkFullyCoveredByRoute(t *testing.T) {
	_, whole, err := net.ParseCIDR("10.0.0.0/16")
	if err != nil {
		t.Fatal(err)
	}
	_, broad, _ := net.ParseCIDR("10.0.0.0/8")
	if !networkFullyCoveredByRoute(whole, []*net.IPNet{broad}) {
		t.Fatal("a /8 route should cover the complete /16 candidate pool")
	}
	_, partial, _ := net.ParseCIDR("10.0.0.0/24")
	if networkFullyCoveredByRoute(whole, []*net.IPNet{partial}) {
		t.Fatal("a /24 route must not mark the entire /16 pool as covered")
	}
	_, disjoint, _ := net.ParseCIDR("192.168.0.0/16")
	if networkFullyCoveredByRoute(whole, []*net.IPNet{disjoint}) {
		t.Fatal("a disjoint route must not mark the pool as covered")
	}
}

func TestSplitDefaultRoutesDoNotReservePrivateSubnetPools(t *testing.T) {
	_, pool, _ := net.ParseCIDR("172.16.0.0/16")
	_, zeroHalf, _ := net.ParseCIDR("0.0.0.0/1")
	_, oneHalf, _ := net.ParseCIDR("128.0.0.0/1")
	_, defaultRoute, _ := net.ParseCIDR("0.0.0.0/0")
	routes := []*net.IPNet{zeroHalf, oneHalf, defaultRoute}

	if networkFullyCoveredByRoute(pool, routes) {
		t.Fatal("split-default routes must not make an otherwise free /16 pool appear occupied")
	}
	_, candidate, _ := net.ParseCIDR("172.16.12.0/30")
	if overlapsAnyRoute(candidate, routes) {
		t.Fatal("split-default routes must not reject a /30 veth subnet")
	}
}

func TestOverlapsAnyRouteRejectsOnlyCollidingSubnet(t *testing.T) {
	_, lan, _ := net.ParseCIDR("192.168.1.0/24")
	_, candidateInside, _ := net.ParseCIDR("192.168.1.4/30")
	_, candidateOutside, _ := net.ParseCIDR("192.168.2.4/30")
	if !overlapsAnyRoute(candidateInside, []*net.IPNet{lan}) {
		t.Fatal("candidate inside an existing LAN route was not rejected")
	}
	if overlapsAnyRoute(candidateOutside, []*net.IPNet{lan}) {
		t.Fatal("candidate outside existing LAN route was incorrectly rejected")
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
