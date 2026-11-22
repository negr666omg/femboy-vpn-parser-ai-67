package core

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// Preflight runs a direct HTTP request from inside the isolated namespace.
// It deliberately does not use sing-box or proxy environment variables: a
// failure here means the namespace's own DNS/routing/NAT path is unhealthy.
func (n *NetworkNamespace) Preflight(ctx context.Context, target string, timeout time.Duration, output io.Writer) error {
	if n == nil || strings.TrimSpace(n.Name) == "" {
		return fmt.Errorf("network namespace is not initialized")
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	executable, err := os.Executable()
	if err != nil {
		return fmt.Errorf("locate SubWatch executable for network preflight: %w", err)
	}
	uid, gid := strconv.Itoa(os.Getuid()), strconv.Itoa(os.Getgid())
	args := []string{
		"-n", "ip", "netns", "exec", n.Name,
		"setpriv", "--reuid", uid, "--regid", gid, "--init-groups",
		executable, "__network-probe",
		"--url", target,
		"--timeout", timeout.String(),
	}
	probeCtx, cancel := context.WithTimeout(ctx, timeout+3*time.Second)
	defer cancel()
	cmd := exec.CommandContext(probeCtx, "sudo", args...)
	data, runErr := cmd.CombinedOutput()
	if output != nil && len(data) > 0 {
		_, _ = output.Write(data)
	}
	if runErr != nil {
		detail := strings.TrimSpace(string(data))
		if detail != "" {
			return fmt.Errorf("isolated network preflight failed: %w: %s", runErr, detail)
		}
		return fmt.Errorf("isolated network preflight failed: %w", runErr)
	}
	return nil
}

// RunNetworkProbe is the private helper command executed after entering the
// namespace. The request is intentionally direct (Transport.Proxy is nil).
func RunNetworkProbe(target string, timeout time.Duration, output io.Writer) error {
	if output == nil {
		output = io.Discard
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	u, err := url.ParseRequestURI(strings.TrimSpace(target))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" {
		return fmt.Errorf("preflight URL must be an absolute http:// or https:// URL: %q", target)
	}
	host := u.Hostname()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ips, err := net.DefaultResolver.LookupIP(ctx, "ip4", host)
	if err != nil {
		return fmt.Errorf("DNS preflight failed for %s: %w", host, err)
	}
	if len(ips) == 0 {
		return fmt.Errorf("DNS preflight failed for %s: no IPv4 addresses returned", host)
	}
	parts := make([]string, 0, len(ips))
	for _, ip := range ips {
		parts = append(parts, ip.String())
	}
	fmt.Fprintf(output, "[preflight] DNS OK: %s -> %s\n", host, strings.Join(parts, ", "))

	port := u.Port()
	if port == "" {
		if u.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	dialer := &net.Dialer{Timeout: minDuration(timeout/3, 3*time.Second)}
	var selectedIP net.IP
	var tcpErr error
	var tcpConn net.Conn
	tcpStarted := time.Now()
	for _, ip := range ips {
		tcpConn, tcpErr = dialer.DialContext(ctx, "tcp", net.JoinHostPort(ip.String(), port))
		if tcpErr == nil {
			selectedIP = ip
			break
		}
	}
	if tcpErr != nil {
		return fmt.Errorf("TCP preflight failed for %s:%s (resolved %s): %w", host, port, strings.Join(parts, ", "), tcpErr)
	}
	tcpRemote := tcpConn.RemoteAddr().String()
	_ = tcpConn.Close()
	fmt.Fprintf(output, "[preflight] TCP OK: %s via %s in %s\n", tcpRemote, selectedIP, time.Since(tcpStarted).Round(time.Millisecond))

	var httpRemote string
	transport := &http.Transport{
		Proxy:               nil,
		TLSHandshakeTimeout: minDuration(timeout/2, 5*time.Second),
		ResponseHeaderTimeout: minDuration(timeout/2, 5*time.Second),
		DialContext: func(dialCtx context.Context, network, address string) (net.Conn, error) {
			requestHost, requestPort, splitErr := net.SplitHostPort(address)
			if splitErr == nil && strings.EqualFold(requestHost, host) {
				var lastErr error
				for _, ip := range ips {
					conn, dialErr := dialer.DialContext(dialCtx, network, net.JoinHostPort(ip.String(), requestPort))
					if dialErr == nil {
						httpRemote = conn.RemoteAddr().String()
						return conn, nil
					}
					lastErr = dialErr
				}
				return nil, lastErr
			}
			conn, dialErr := dialer.DialContext(dialCtx, network, address)
			if dialErr == nil {
				httpRemote = conn.RemoteAddr().String()
			}
			return conn, dialErr
		},
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: timeout}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return fmt.Errorf("create preflight request: %w", err)
	}
	req.Header.Set("User-Agent", "SubWatch/0.1 network preflight")
	httpStarted := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("direct HTTP preflight failed for %s via %s after TCP succeeded: %w", u.Host, selectedIP, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		return fmt.Errorf("direct HTTP preflight returned %s from %s via %s", resp.Status, u.Host, selectedIP)
	}
	fmt.Fprintf(output, "[preflight] HTTP OK: status=%s remote=%s elapsed=%s\n", resp.Status, httpRemote, time.Since(httpStarted).Round(time.Millisecond))
	return nil
}

func minDuration(a, b time.Duration) time.Duration {
	if a <= 0 || a > b {
		return b
	}
	return a
}
