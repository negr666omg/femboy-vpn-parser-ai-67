package core

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Preflight checks TCP reachability to actual proxy endpoints from inside
// the isolated namespace. Testing the URL-test target directly is incorrect:
// the target is reached through each proxy, while the namespace only needs
// direct connectivity to proxy servers.
func (n *NetworkNamespace) Preflight(ctx context.Context, targets []string, timeout time.Duration, output io.Writer) error {
	if n == nil || strings.TrimSpace(n.Name) == "" {
		return fmt.Errorf("network namespace is not initialized")
	}
	if timeout <= 0 {
		timeout = 12 * time.Second
	}
	if len(targets) == 0 {
		return fmt.Errorf("no proxy server endpoints available for network preflight")
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
		"--tcp-targets", strings.Join(targets, ","),
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
		if output != nil {
			n.appendNetworkDiagnostics(ctx, output)
		}
		if detail != "" {
			return fmt.Errorf("isolated network preflight failed: %w: %s", runErr, detail)
		}
		return fmt.Errorf("isolated network preflight failed: %w", runErr)
	}
	return nil
}

func (n *NetworkNamespace) appendNetworkDiagnostics(ctx context.Context, output io.Writer) {
	if output == nil {
		return
	}
	type diagnostic struct {
		label string
		args  []string
	}
	items := []diagnostic{
		{"namespace routes", []string{"ip", "-n", n.Name, "-4", "route", "show", "table", "all"}},
		{"host policy rules", []string{"ip", "-4", "rule", "show"}},
		{"selected-interface route table", []string{"ip", "-4", "route", "show", "table", strconv.Itoa(n.routeTable)}},
		{"host route lookup from namespace", []string{"ip", "-4", "route", "get", "1.1.1.1", "from", n.ProxyIP, "iif", n.hostVeth}},
	}
	if n.firewallBackend == "nft" {
		items = append(items, diagnostic{"temporary nftables counters", []string{"nft", "-a", "list", "table", "ip", n.firewallTable}})
	} else if n.firewallBackend == "iptables" {
		items = append(items,
			diagnostic{"iptables forward counters", []string{"iptables", "-vnL", "FORWARD"}},
			diagnostic{"iptables NAT counters", []string{"iptables", "-t", "nat", "-vnL", "POSTROUTING"}},
		)
	}
	fmt.Fprintln(output, "[preflight] ---- network diagnostics (captured before cleanup) ----")
	for _, item := range items {
		diagCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		cmd := exec.CommandContext(diagCtx, "sudo", append([]string{"-n"}, item.args...)...)
		data, err := cmd.CombinedOutput()
		cancel()
		fmt.Fprintf(output, "[preflight] %s:\n", item.label)
		if len(data) > 0 {
			_, _ = output.Write(data)
			if data[len(data)-1] != '\n' {
				fmt.Fprintln(output)
			}
		}
		if err != nil {
			fmt.Fprintf(output, "[preflight] diagnostic command failed: %v\n", err)
		}
	}
	fmt.Fprintln(output, "[preflight] ---- end network diagnostics ----")
}

// RunNetworkTCPProbe runs direct TCP probes against a sample of proxy
// server:port endpoints. At least one reachable endpoint is enough to establish
// that the namespace can reach proxy servers; individual dead nodes are normal.
func RunNetworkTCPProbe(rawTargets string, timeout time.Duration, output io.Writer) error {
	if output == nil {
		output = io.Discard
	}
	if timeout <= 0 {
		timeout = 12 * time.Second
	}
	targets := make([]string, 0)
	seen := make(map[string]bool)
	for _, raw := range strings.Split(rawTargets, ",") {
		target := strings.TrimSpace(raw)
		if target == "" || seen[target] {
			continue
		}
		host, port, err := net.SplitHostPort(target)
		if err != nil || host == "" || port == "" {
			return fmt.Errorf("invalid proxy endpoint %q (expected host:port)", target)
		}
		if _, err := strconv.Atoi(port); err != nil {
			return fmt.Errorf("invalid port in proxy endpoint %q", target)
		}
		seen[target] = true
		targets = append(targets, target)
	}
	if len(targets) == 0 {
		return fmt.Errorf("no valid proxy endpoints supplied")
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	type result struct {
		target  string
		ip      string
		elapsed time.Duration
		err     error
	}
	results := make([]result, len(targets))
	jobs := make(chan int)
	var workers sync.WaitGroup
	workerCount := 8
	if len(targets) < workerCount {
		workerCount = len(targets)
	}
	for worker := 0; worker < workerCount; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := range jobs {
				results[i].target = targets[i]
				host, port, _ := net.SplitHostPort(targets[i])
				started := time.Now()
				ips, lookupErr := net.DefaultResolver.LookupIP(ctx, "ip4", host)
				if lookupErr != nil {
					results[i].err = fmt.Errorf("DNS: %w", lookupErr)
					results[i].elapsed = time.Since(started)
					continue
				}
				if len(ips) == 0 {
					results[i].err = fmt.Errorf("DNS returned no IPv4 addresses")
					results[i].elapsed = time.Since(started)
					continue
				}
				dialer := &net.Dialer{Timeout: minDuration(timeout/2, 3*time.Second)}
				var lastErr error
				for _, ip := range ips {
					conn, dialErr := dialer.DialContext(ctx, "tcp", net.JoinHostPort(ip.String(), port))
					if dialErr != nil {
						lastErr = dialErr
						if ctx.Err() != nil {
							break
						}
						continue
					}
					results[i].ip = ip.String()
					_ = conn.Close()
					lastErr = nil
					break
				}
				results[i].elapsed = time.Since(started)
				results[i].err = lastErr
				if results[i].err == nil && results[i].ip == "" {
					results[i].err = errors.New("TCP connection failed without a reported cause")
				}
			}
		}()
	}
schedule:
	for i := range targets {
		select {
		case jobs <- i:
		case <-ctx.Done():
			break schedule
		}
	}
	close(jobs)
	workers.Wait()

	successes := 0
	failures := make([]string, 0, len(results))
	for _, result := range results {
		if result.target == "" {
			continue
		}
		if result.err == nil && result.ip != "" {
			successes++
			fmt.Fprintf(output, "[preflight] TCP OK: %s via %s in %s\n", result.target, result.ip, result.elapsed.Round(time.Millisecond))
		} else {
			message := "unknown error"
			if result.err != nil {
				message = result.err.Error()
			}
			fmt.Fprintf(output, "[preflight] TCP FAIL: %s: %s\n", result.target, message)
			failures = append(failures, result.target+": "+message)
		}
	}
	fmt.Fprintf(output, "[preflight] reachable proxy endpoints: %d/%d\n", successes, len(targets))
	if successes == 0 {
		if ctx.Err() != nil {
			return fmt.Errorf("no sampled proxy endpoint was reachable before timeout: %w; failures: %s", ctx.Err(), strings.Join(failures, "; "))
		}
		return fmt.Errorf("none of %d sampled proxy endpoints accepted TCP connections: %s", len(targets), strings.Join(failures, "; "))
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
