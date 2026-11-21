package sub

import ("testing"; "strings")

func TestParseURIProtocols(t *testing.T) {
	cases := []struct{ name, uri, typ string }{
		{"vless", "vless://11111111-1111-4111-8111-111111111111@example.com:443?type=ws&security=tls&sni=example.com&path=%2Fws#one", "vless"},
		{"trojan", "trojan://secret@example.com:443?sni=example.com#two", "trojan"},
		{"socks", "socks5://user:pass@example.com:1080#three", "socks"},
		{"http", "http://user:pass@example.com:8080#four", "http"},
		{"hysteria2", "hysteria2://secret@example.com:443?sni=example.com#five", "hysteria2"},
		{"tuic", "tuic://11111111-1111-4111-8111-111111111111:secret@example.com:443?sni=example.com#six", "tuic"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n, err := ParseURI(tc.uri)
			if err != nil {
				t.Fatal(err)
			}
			if got := n.Outbound["type"]; got != tc.typ {
				t.Fatalf("type=%v want %s", got, tc.typ)
			}
			if n.Name == "" {
				t.Fatal("missing display name")
			}
		})
	}
}
func TestParseVMess(t *testing.T) {
	n, err := ParseURI("vmess://eyJhZGQiOiJleGFtcGxlLmNvbSIsInBvcnQiOiI0NDMiLCJpZCI6IjExMTExMTExLTExMTEtNDExMS04MTExLTExMTExMTExMTExMSIsIm5ldCI6IndzIiwidGxzIjoidGxzIiwicHMiOiJ2bWVzcyJ9")
	if err != nil {
		t.Fatal(err)
	}
	if n.Outbound["type"] != "vmess" {
		t.Fatalf("unexpected type: %v", n.Outbound["type"])
	}
}
func TestDedupKeyIgnoresDisplayName(t *testing.T) {
	a, e := ParseURI("vless://11111111-1111-4111-8111-111111111111@example.com:443?security=tls#A")
	if e != nil {
		t.Fatal(e)
	}
	b, e := ParseURI("vless://11111111-1111-4111-8111-111111111111@example.com:443?security=tls#B")
	if e != nil {
		t.Fatal(e)
	}
	if a.Key != b.Key {
		t.Fatal("name changed dedup key")
	}
}

func TestParseShadowsocksURI(t *testing.T) {
	for _, uri := range []string{
		"ss://YWVzLTI1Ni1nY206cGFzcw@example.com:8388#ss-one",
		"ss://YWVzLTI1Ni1nY206cGFzc0BleGFtcGxlLmNvbTo4Mzg4#ss-two",
	} {
		n, err := ParseURI(uri)
		if err != nil {
			t.Fatalf("ParseURI(%q): %v", uri, err)
		}
		if n.Outbound["type"] != "shadowsocks" {
			t.Fatalf("unexpected outbound: %#v", n.Outbound)
		}
	}
}

func TestParseDocumentReportsLineProgress(t *testing.T) {
	var body strings.Builder
	for i := 0; i < 300; i++ {
		body.WriteString("vless://11111111-1111-4111-8111-111111111111@example.test:443\n")
	}
	var updates int
	lastProcessed, lastTotal, lastNodes, lastInvalid := 0, 0, 0, 0
	nodes, invalid, err := ParseDocumentWithProgress([]byte(body.String()), func(processed, total, parsed, bad int) {
		updates++
		lastProcessed, lastTotal, lastNodes, lastInvalid = processed, total, parsed, bad
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 300 || invalid != 0 {
		t.Fatalf("nodes=%d invalid=%d, want 300 and 0", len(nodes), invalid)
	}
	if updates < 3 || lastProcessed != 300 || lastTotal != 300 || lastNodes != 300 || lastInvalid != 0 {
		t.Fatalf("unexpected final progress: updates=%d processed=%d total=%d nodes=%d invalid=%d", updates, lastProcessed, lastTotal, lastNodes, lastInvalid)
	}
}

func TestParseBase64Subscription(t *testing.T) {
	encoded := "dmxlc3M6Ly8xMTExMTExMS0xMTExLTQxMTEtODExMS0xMTExMTExMTExMTFAZXhhbXBsZS5jb206NDQzP3NlY3VyaXR5PXRscyNhbHBoYQp0cm9qYW46Ly9zZWNyZXRAZXhhbXBsZS5jb206NDQzI2JldGE="
	nodes, invalid, err := ParseDocument([]byte(encoded))
	if err != nil {
		t.Fatal(err)
	}
	if invalid != 0 || len(nodes) != 2 {
		t.Fatalf("nodes=%d invalid=%d", len(nodes), invalid)
	}
}

func TestRealityDefaultsUTLSAndIgnoresNonePacketEncoding(t *testing.T) {
	node, err := ParseURI("vless://00000000-0000-0000-0000-000000000001@example.test:443?security=reality&pbk=abc&packetEncoding=none")
	if err != nil {
		t.Fatal(err)
	}
	tls, ok := node.Outbound["tls"].(map[string]any)
	if !ok {
		t.Fatalf("expected TLS options, got %#v", node.Outbound["tls"])
	}
	utls, ok := tls["utls"].(map[string]any)
	if !ok || utls["enabled"] != true || utls["fingerprint"] != "chrome" {
		t.Fatalf("expected default Chrome uTLS fingerprint, got %#v", tls["utls"])
	}
	if _, ok := node.Outbound["packet_encoding"]; ok {
		t.Fatalf("packetEncoding=none should be omitted, got %#v", node.Outbound["packet_encoding"])
	}
}

func TestRealityUnsafeFingerprintFallsBackToChrome(t *testing.T) {
	node, err := ParseURI("vless://00000000-0000-0000-0000-000000000001@example.test:443?security=reality&fp=unsafe&pbk=abc")
	if err != nil {
		t.Fatal(err)
	}
	tls := node.Outbound["tls"].(map[string]any)
	utls := tls["utls"].(map[string]any)
	if utls["fingerprint"] != "chrome" {
		t.Fatalf("expected fallback Chrome fingerprint, got %#v", utls)
	}
}

func TestRenameNodeUpdatesSubscriptionRemark(t *testing.T) {
	node, err := ParseURI("vless://11111111-1111-4111-8111-111111111111@example.com:443#old-name")
	if err != nil { t.Fatal(err) }
	RenameNode(&node, "🇩🇪 70↓34↑")
	if node.Name != "🇩🇪 70↓34↑" || !strings.Contains(node.URI, "#") {
		t.Fatalf("rename was not written to URI: name=%q uri=%q", node.Name, node.URI)
	}
	parsed, err := ParseURI(node.URI)
	if err != nil { t.Fatal(err) }
	if parsed.Name != node.Name { t.Fatalf("parsed name=%q want %q", parsed.Name, node.Name) }
}



func TestParseDocumentReturnsInvalidConfigLines(t *testing.T) {
	valid := "vless://11111111-1111-4111-8111-111111111111@example.test:443#valid"
	invalidOne := "vless://missing-port"
	invalidTwo := "unsupported://example.test:443"
	body := strings.Join([]string{valid, "# comment", "", invalidOne, invalidTwo, "name,server,port"}, "\n")

	nodes, invalid, err := ParseDocumentWithDiagnostics([]byte(body), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 {
		t.Fatalf("parsed nodes=%d, want 1", len(nodes))
	}
	want := []string{invalidOne, invalidTwo}
	if len(invalid) != len(want) {
		t.Fatalf("invalid lines=%q, want %q", invalid, want)
	}
	for i := range want {
		if invalid[i] != want[i] {
			t.Fatalf("invalid line %d=%q, want %q", i, invalid[i], want[i])
		}
	}
}
