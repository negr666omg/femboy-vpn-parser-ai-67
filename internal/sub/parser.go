package sub

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

type Node struct {
	Name         string          `json:"name"`
	Protocol     string          `json:"protocol"`
	URI          string          `json:"uri"`
	Outbound     map[string]any  `json:"outbound"`
	Key          string          `json:"key"`
	URLPassed    bool            `json:"url_passed"`
	URLLatencyMS int64           `json:"url_latency_ms,omitempty"`
	URLError     string          `json:"url_error,omitempty"`
	SpeedPassed  bool            `json:"speed_passed"`
	DownloadMbps float64         `json:"download_mbps"`
	UploadMbps   float64         `json:"upload_mbps"`
	SpeedError   string          `json:"speed_error,omitempty"`
	GeminiStatus string          `json:"gemini_status,omitempty"`
	GeminiNote   string          `json:"gemini_note,omitempty"`
	GeminiRegion string          `json:"gemini_region,omitempty"`
	ServiceError string          `json:"service_error,omitempty"`
	ExitIP       string          `json:"exit_ip,omitempty"`
	Country      string          `json:"country,omitempty"`
	ASN          string          `json:"asn,omitempty"`
	Org          string          `json:"org,omitempty"`
	IPType       string          `json:"ip_type,omitempty"`
	IPFlags      map[string]bool `json:"ip_flags,omitempty"`
}

func ParseDocument(body []byte) ([]Node, int, error) {
	return ParseDocumentWithProgress(body, nil)
}

// ParseDocumentWithProgress parses a subscription and periodically reports
// line-level progress. The callback runs synchronously in the parser goroutine.
func ParseDocumentWithProgress(body []byte, progress func(processedLines, totalLines, parsedNodes, invalidLines int)) ([]Node, int, error) {
	if len(body) > 8<<20 {
		return nil, 0, errors.New("source exceeds 8 MiB")
	}
	text := strings.TrimSpace(string(body))
	if text == "" {
		return nil, 0, errors.New("empty subscription")
	}
	if !strings.Contains(text, "://") {
		decoded, err := decodeBase64(text)
		if err == nil && strings.Contains(string(decoded), "://") {
			text = string(decoded)
		}
	}
	var nodes []Node
	invalid := 0
	totalLines := strings.Count(text, "\n") + 1
	if progress != nil {
		progress(0, totalLines, 0, 0)
	}
	scanner := bufio.NewScanner(strings.NewReader(text))
	scanner.Buffer(make([]byte, 64*1024), 2<<20)
	processedLines := 0
	for scanner.Scan() {
		processedLines++
		line := strings.TrimSpace(strings.TrimPrefix(scanner.Text(), "\ufeff"))
		if line != "" && !strings.HasPrefix(line, "#") && !(strings.Contains(line, ",") && !strings.Contains(line, "://")) {
			node, err := ParseURI(line)
			if err != nil {
				invalid++
			} else {
				nodes = append(nodes, node)
			}
		}
		if progress != nil && processedLines%128 == 0 {
			progress(processedLines, totalLines, len(nodes), invalid)
		}
	}
	if progress != nil && processedLines%128 != 0 {
		progress(processedLines, totalLines, len(nodes), invalid)
	}
	if err := scanner.Err(); err != nil {
		return nil, invalid, err
	}
	return nodes, invalid, nil
}

func decodeBase64(s string) ([]byte, error) {
	s = strings.Join(strings.Fields(strings.TrimSpace(s)), "")
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		b, err := enc.DecodeString(s)
		if err == nil {
			return b, nil
		}
	}
	return nil, errors.New("not base64")
}

func ParseURI(raw string) (Node, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return Node{}, err
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme == "" {
		return Node{}, errors.New("missing scheme")
	}
	name := u.Fragment
	if decoded, e := url.QueryUnescape(name); e == nil {
		name = decoded
	}
	if name == "" {
		name = u.Hostname()
	}
	var out map[string]any
	switch scheme {
	case "vmess":
		out, err = parseVMess(u)
	case "ss", "shadowsocks":
		out, err = parseShadowsocks(u, raw)
	case "vless":
		out, err = parseVLESS(u)
	case "trojan":
		out, err = parseTrojan(u)
	case "hysteria2", "hy2":
		out, err = parseHysteria2(u)
	case "tuic":
		out, err = parseTUIC(u)
	case "socks", "socks5":
		out, err = parseSOCKS(u)
	case "http", "https":
		out, err = parseHTTP(u)
	default:
		err = fmt.Errorf("unsupported scheme %q", scheme)
	}
	if err != nil {
		return Node{}, err
	}
	if vmessName, ok := out["_name"].(string); ok && vmessName != "" && (u.Fragment == "" || name == u.Hostname()) {
		name = vmessName
	}
	out["tag"] = "proxy"
	if strings.TrimSpace(name) != "" {
		out["_name"] = name
	}
	clean := make(map[string]any, len(out))
	for k, v := range out {
		if k != "_name" && k != "tag" {
			clean[k] = v
		}
	}
	b, _ := json.Marshal(clean)
	protocol, _ := out["type"].(string)
	protocol = normalizeProtocol(protocol)
	return Node{Name: name, Protocol: protocol, URI: raw, Outbound: out, Key: string(b)}, nil
}

func normalizeProtocol(protocol string) string {
	switch strings.ToLower(protocol) {
	case "shadowsocks", "ss":
		return "ss"
	case "hy2", "hysteria2":
		return "hysteria2"
	case "socks5":
		return "socks"
	default:
		return strings.ToLower(protocol)
	}
}

func hostPort(u *url.URL) (string, uint16, error) {
	host := u.Hostname()
	if host == "" {
		return "", 0, errors.New("missing host")
	}
	p := u.Port()
	if p == "" {
		return "", 0, errors.New("missing port")
	}
	n, err := strconv.Atoi(p)
	if err != nil || n < 1 || n > 65535 {
		return "", 0, errors.New("invalid port")
	}
	return host, uint16(n), nil
}
func tlsOptions(q url.Values, defaultServer string) map[string]any {
	security := strings.ToLower(q.Get("security"))
	enabled := security == "tls" || security == "reality" || q.Get("tls") == "1" || q.Get("tls") == "true"
	if !enabled {
		return nil
	}
	t := map[string]any{"enabled": true}
	server := q.Get("sni")
	if server == "" {
		server = q.Get("servername")
	}
	if server == "" {
		server = q.Get("serverName")
	}
	if server == "" {
		server = defaultServer
	}
	if server != "" {
		t["server_name"] = server
	}
	if q.Get("allowInsecure") == "1" || q.Get("insecure") == "1" || q.Get("insecure") == "true" {
		t["insecure"] = true
	}
	if q.Get("alpn") != "" {
		t["alpn"] = strings.Split(q.Get("alpn"), ",")
	}
	if security == "reality" {
		reality := map[string]any{"enabled": true}
		if q.Get("pbk") != "" {
			reality["public_key"] = q.Get("pbk")
		}
		if q.Get("sid") != "" {
			reality["short_id"] = q.Get("sid")
		}
		t["reality"] = reality
	}
	fingerprint := strings.ToLower(strings.TrimSpace(q.Get("fp")))
	if security == "reality" && fingerprint == "" {
		fingerprint = "chrome"
	}
	if fingerprint == "unsafe" {
		fingerprint = "chrome"
	}
	switch fingerprint {
	case "chrome", "firefox", "safari", "edge", "360", "qq", "ios", "android", "random", "randomized":
		t["utls"] = map[string]any{"enabled": true, "fingerprint": fingerprint}
	case "":
		// uTLS is optional for ordinary TLS nodes.
	default:
		if security == "reality" {
			t["utls"] = map[string]any{"enabled": true, "fingerprint": "chrome"}
		}
	}
	return t
}
func transportOptions(q url.Values) map[string]any {
	typ := strings.ToLower(q.Get("type"))
	if typ == "" {
		typ = strings.ToLower(q.Get("network"))
	}
	switch typ {
	case "ws":
		path := q.Get("path")
		if path == "" {
			path = "/"
		}
		ws := map[string]any{"type": "ws", "path": path}
		if host := q.Get("host"); host != "" {
			ws["headers"] = map[string]string{"Host": host}
		}
		return ws
	case "grpc":
		service := q.Get("serviceName")
		if service == "" {
			service = q.Get("service_name")
		}
		out := map[string]any{"type": "grpc"}
		if service != "" {
			out["service_name"] = service
		}
		return out
	case "httpupgrade", "http-upgrade":
		path := q.Get("path")
		if path == "" {
			path = "/"
		}
		out := map[string]any{"type": "httpupgrade", "path": path}
		if host := q.Get("host"); host != "" {
			out["host"] = host
		}
		return out
	case "http", "h2":
		out := map[string]any{"type": "http"}
		if host := q.Get("host"); host != "" {
			out["host"] = strings.Split(host, ",")
		}
		if path := q.Get("path"); path != "" {
			out["path"] = path
		}
		return out
	default:
		return nil
	}
}
func parseVLESS(u *url.URL) (map[string]any, error) {
	h, p, e := hostPort(u)
	if e != nil {
		return nil, e
	}
	id := u.User.Username()
	if id == "" {
		return nil, errors.New("missing UUID")
	}
	o := map[string]any{"type": "vless", "server": h, "server_port": p, "uuid": id}
	if f := u.Query().Get("flow"); f != "" {
		o["flow"] = f
	}
	if t := tlsOptions(u.Query(), h); t != nil {
		o["tls"] = t
	}
	if t := transportOptions(u.Query()); t != nil {
		o["transport"] = t
	}
	if packetEncoding := strings.ToLower(strings.TrimSpace(u.Query().Get("packetEncoding"))); packetEncoding != "" && packetEncoding != "none" {
		o["packet_encoding"] = packetEncoding
	}
	return o, nil
}
func parseTrojan(u *url.URL) (map[string]any, error) {
	h, p, e := hostPort(u)
	if e != nil {
		return nil, e
	}
	password := u.User.Username()
	if password == "" {
		return nil, errors.New("missing password")
	}
	o := map[string]any{"type": "trojan", "server": h, "server_port": p, "password": password}
	if t := tlsOptions(u.Query(), h); t != nil {
		o["tls"] = t
	} else {
		o["tls"] = map[string]any{"enabled": true, "server_name": h}
	}
	if t := transportOptions(u.Query()); t != nil {
		o["transport"] = t
	}
	return o, nil
}
func parseVMess(u *url.URL) (map[string]any, error) {
	raw := strings.TrimPrefix(u.Opaque, "//")
	if raw == "" {
		raw = strings.TrimPrefix(u.Host, "//")
	}
	if raw == "" {
		raw = u.Host
	}
	b, e := decodeBase64(raw)
	if e != nil {
		return nil, fmt.Errorf("vmess payload: %w", e)
	}
	var v struct {
		Add  string          `json:"add"`
		Port json.RawMessage `json:"port"`
		ID   string          `json:"id"`
		Aid  json.RawMessage `json:"aid"`
		Scy  string          `json:"scy"`
		Net  string          `json:"net"`
		Host string          `json:"host"`
		Path string          `json:"path"`
		TLS  string          `json:"tls"`
		SNI  string          `json:"sni"`
		Alpn string          `json:"alpn"`
		Name string          `json:"ps"`
		Type string          `json:"type"`
	}
	if e = json.Unmarshal(b, &v); e != nil {
		return nil, e
	}
	var port int
	if len(v.Port) > 0 {
		_ = json.Unmarshal(v.Port, &port)
		if port == 0 {
			var s string
			_ = json.Unmarshal(v.Port, &s)
			port, _ = strconv.Atoi(s)
		}
	}
	if v.Add == "" || port < 1 || port > 65535 || v.ID == "" {
		return nil, errors.New("vmess requires add, port and id")
	}
	security := v.Scy
	if security == "" {
		security = "auto"
	}
	o := map[string]any{"type": "vmess", "server": v.Add, "server_port": port, "uuid": v.ID, "security": security}
	if v.Name != "" {
		o["_name"] = v.Name
	}
	var aid int
	if len(v.Aid) > 0 {
		_ = json.Unmarshal(v.Aid, &aid)
		if aid == 0 {
			var s string
			_ = json.Unmarshal(v.Aid, &s)
			aid, _ = strconv.Atoi(s)
		}
	}
	o["alter_id"] = aid
	q := url.Values{}
	if strings.EqualFold(v.TLS, "tls") {
		q.Set("security", "tls")
	}
	if v.SNI != "" {
		q.Set("sni", v.SNI)
	}
	if v.Alpn != "" {
		q.Set("alpn", v.Alpn)
	}
	if t := tlsOptions(q, v.Add); t != nil {
		o["tls"] = t
	}
	q = url.Values{}
	q.Set("type", v.Net)
	q.Set("host", v.Host)
	q.Set("path", v.Path)
	if t := transportOptions(q); t != nil {
		o["transport"] = t
	}
	return o, nil
}
func parseShadowsocks(u *url.URL, raw string) (map[string]any, error) {
	server, port, e := hostPort(u)
	if e != nil {
		// SIP002 whole-payload Base64 form.
		payload := strings.TrimPrefix(raw, "ss://")
		payload = strings.SplitN(payload, "#", 2)[0]
		payload = strings.SplitN(payload, "?", 2)[0]
		b, err := decodeBase64(payload)
		if err != nil {
			return nil, e
		}
		decoded := string(b)
		at := strings.LastIndex(decoded, "@")
		if at < 0 {
			return nil, errors.New("invalid shadowsocks URI")
		}
		cred := strings.SplitN(decoded[:at], ":", 2)
		hp := strings.Split(decoded[at+1:], ":")
		if len(cred) != 2 || len(hp) != 2 {
			return nil, errors.New("invalid shadowsocks payload")
		}
		portn, _ := strconv.Atoi(hp[1])
		if portn < 1 || portn > 65535 {
			return nil, errors.New("invalid shadowsocks port")
		}
		return map[string]any{"type": "shadowsocks", "server": hp[0], "server_port": portn, "method": cred[0], "password": cred[1]}, nil
	}
	userinfo := u.User
	if userinfo == nil {
		return nil, errors.New("missing credentials")
	}
	cred := userinfo.Username()
	if pass, ok := userinfo.Password(); ok {
		cred += ":" + pass
	} else {
		decoded, err := decodeBase64(cred)
		if err != nil {
			return nil, err
		}
		cred = string(decoded)
	}
	parts := strings.SplitN(cred, ":", 2)
	if len(parts) != 2 {
		return nil, errors.New("expected method:password")
	}
	return map[string]any{"type": "shadowsocks", "server": server, "server_port": port, "method": parts[0], "password": parts[1]}, nil
}
func parseHysteria2(u *url.URL) (map[string]any, error) {
	h, p, e := hostPort(u)
	if e != nil {
		return nil, e
	}
	pass := u.User.Username()
	if pass == "" {
		return nil, errors.New("missing password")
	}
	q := u.Query()
	tls := tlsOptions(q, h)
	if tls == nil {
		tls = map[string]any{"enabled": true, "server_name": h}
	}
	o := map[string]any{"type": "hysteria2", "server": h, "server_port": p, "password": pass, "tls": tls}
	if q.Get("obfs") != "" {
		o["obfs"] = map[string]any{"type": q.Get("obfs"), "password": q.Get("obfs-password")}
	}
	return o, nil
}
func parseTUIC(u *url.URL) (map[string]any, error) {
	h, p, e := hostPort(u)
	if e != nil {
		return nil, e
	}
	user := u.User.Username()
	pass, _ := u.User.Password()
	if user == "" || pass == "" {
		return nil, errors.New("TUIC needs UUID and password")
	}
	q := u.Query()
	tls := tlsOptions(q, h)
	if tls == nil {
		tls = map[string]any{"enabled": true, "server_name": h, "alpn": []string{"h3"}}
	}
	o := map[string]any{"type": "tuic", "server": h, "server_port": p, "uuid": user, "password": pass, "tls": tls}
	if q.Get("congestion_control") != "" {
		o["congestion_control"] = q.Get("congestion_control")
	}
	if q.Get("udp_relay_mode") != "" {
		o["udp_relay_mode"] = q.Get("udp_relay_mode")
	}
	return o, nil
}
func parseSOCKS(u *url.URL) (map[string]any, error) {
	h, p, e := hostPort(u)
	if e != nil {
		return nil, e
	}
	o := map[string]any{"type": "socks", "server": h, "server_port": p, "version": "5"}
	if u.User != nil {
		o["username"] = u.User.Username()
		if pass, ok := u.User.Password(); ok {
			o["password"] = pass
		}
	}
	return o, nil
}
func parseHTTP(u *url.URL) (map[string]any, error) {
	h, p, e := hostPort(u)
	if e != nil {
		return nil, e
	}
	o := map[string]any{"type": "http", "server": h, "server_port": p}
	if u.User != nil {
		o["username"] = u.User.Username()
		if pass, ok := u.User.Password(); ok {
			o["password"] = pass
		}
	}
	if u.Scheme == "https" {
		o["tls"] = map[string]any{"enabled": true, "server_name": h}
	}
	return o, nil
}

// RenameNode updates both the exported node name and the URI remark consumed by subscription clients.
func RenameNode(n *Node, name string) {
	if n == nil || strings.TrimSpace(name) == "" { return }
	u, err := url.Parse(n.URI)
	if err != nil { return }
	u.Fragment = name
	n.URI = u.String()
	n.Name = name
	n.Outbound["_name"] = name
}
