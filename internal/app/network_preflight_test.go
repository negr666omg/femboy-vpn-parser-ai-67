package app

import (
	"testing"

	"github.com/negr666omg/subwatch/internal/sub"
)

func TestNetworkPreflightTargetsSamplesAcrossNodeSet(t *testing.T) {
	nodes := make([]sub.Node, 100)
	for i := range nodes {
		nodes[i] = sub.Node{Outbound: map[string]any{
			"server":      "proxy.example",
			"server_port": 1000 + i,
		}}
	}
	got := networkPreflightTargets(nodes, 5)
	want := []string{
		"proxy.example:1000",
		"proxy.example:1024",
		"proxy.example:1049",
		"proxy.example:1074",
		"proxy.example:1099",
	}
	if len(got) != len(want) {
		t.Fatalf("networkPreflightTargets() returned %d targets, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("networkPreflightTargets()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestNetworkPreflightTargetsSkipsInvalidAndDeduplicates(t *testing.T) {
	nodes := []sub.Node{
		{Outbound: map[string]any{"server": "proxy.example", "server_port": 443}},
		{Outbound: map[string]any{"server": "proxy.example", "server_port": 443}},
		{Outbound: map[string]any{"server": "missing-port"}},
		{Outbound: map[string]any{"server": "proxy.example", "server_port": "not-a-port"}},
	}
	got := networkPreflightTargets(nodes, 10)
	if len(got) != 1 || got[0] != "proxy.example:443" {
		t.Fatalf("networkPreflightTargets() = %v, want [proxy.example:443]", got)
	}
}
