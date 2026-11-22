//go:build linux

package core

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// NetworkNamespace provides an isolated network stack. A private veth pair
// connects it to the host; policy routing and temporary NAT rules make packets
// arriving from that veth leave through the selected host interface. No second
// MAC address, DHCP lease, host default-route replacement, or VPN route change
// is required.
type NetworkNamespace struct {
	Name             string
	ProxyIP          string
	HostIP           string
	hostVeth         string
	subnet           string
	routeTable       int
	firewallTable    string
	firewallBackend  string
	physicalInterface string
	resolvDir        string

	namespaceCreated  bool
	hostVethCreated   bool
	routeTableCreated bool
	returnRuleCreated bool
	egressRuleCreated bool
	firewallCreated   bool
	resolvDirCreated  bool
	ipForwardOriginal string
	ipForwardChanged  bool
	rpFilterPath      string
	sysctlOriginal    map[string]string
}

// HostInterface returns the host-side veth used to reach the proxy inside this
// namespace. Host-side proxy connections must use this link rather than the
// host's default route, which may belong to an unrelated VPN.
func (n *NetworkNamespace) HostInterface() string {
	if n == nil {
		return ""
	}
	return n.hostVeth
}

// namespaceProxyDialer pins host-to-namespace proxy traffic to the veth and
// binds the source address to the host side of that link.
func namespaceProxyDialer(hostInterface, hostIP string) *net.Dialer {
	if hostInterface == "" || hostIP == "" {
		return nil
	}
	return &net.Dialer{
		Timeout:   200 * time.Millisecond,
		LocalAddr: &net.TCPAddr{IP: net.ParseIP(hostIP)},
		Control: func(_, _ string, raw syscall.RawConn) error {
			var setErr error
			if err := raw.Control(func(fd uintptr) {
				setErr = syscall.SetsockoptString(int(fd), syscall.SOL_SOCKET, syscall.SO_BINDTODEVICE, hostInterface)
			}); err != nil {
				return err
			}
			// Kernels before 5.7 may require CAP_NET_RAW for SO_BINDTODEVICE.
			// The source address still selects the connected veth route there.
			if setErr == syscall.EPERM || setErr == syscall.EACCES || setErr == syscall.ENOPROTOOPT {
				return nil
			}
			return setErr
		},
	}
}

func NewNetworkNamespace(ctx context.Context, iface string, output io.Writer) (*NetworkNamespace, error) {
	if iface == "" || iface == "auto" {
		return nil, nil
	}
	it, err := net.InterfaceByName(iface)
	if err != nil {
		return nil, fmt.Errorf("network interface %q not found: %w", iface, err)
	}
	if it.Flags&net.FlagUp == 0 {
		return nil, fmt.Errorf("network interface %q is DOWN", iface)
	}
	for _, tool := range []string{"ip", "sudo", "setpriv"} {
		if _, err := exec.LookPath(tool); err != nil {
			return nil, fmt.Errorf("Linux network isolation requires %s (install the corresponding system package)", tool)
		}
	}
	if _, nftErr := exec.LookPath("nft"); nftErr != nil {
		if _, iptablesErr := exec.LookPath("iptables"); iptablesErr != nil {
			return nil, fmt.Errorf("Linux network isolation requires nftables (nft) or iptables for temporary NAT rules")
		}
	}

	interfaceIP, interfacePrefix, gateway, err := selectedInterfaceRoute(ctx, it)
	if err != nil {
		return nil, err
	}
	subnet, hostIP, proxyIP, err := chooseProxySubnet(ctx, os.Getpid())
	if err != nil {
		return nil, err
	}
	tableID, err := chooseRouteTable(ctx, os.Getpid())
	if err != nil {
		return nil, err
	}

	id := strconv.Itoa(os.Getpid()%100000) + strconv.FormatInt(time.Now().UnixNano()%1679616, 36)
	ns := &NetworkNamespace{
		Name:              "sw-" + id,
		ProxyIP:           proxyIP,
		HostIP:            hostIP,
		hostVeth:          "swh" + id,
		subnet:            subnet.String(),
		routeTable:        tableID,
		firewallTable:     "subwatch" + id,
		physicalInterface: it.Name,
		resolvDir:         filepath.Join("/etc/netns", "sw-"+id),
	}
	peer := "swn" + id
	ns.rpFilterPath = filepath.Join("/proc/sys/net/ipv4/conf", it.Name, "rp_filter")

	fmt.Fprintln(output, "[network] requesting administrator authorization")
	auth := exec.CommandContext(ctx, "sudo", "-v")
	auth.Stdin, auth.Stdout, auth.Stderr = os.Stdin, os.Stdout, os.Stderr
	if err := auth.Run(); err != nil {
		return nil, fmt.Errorf("administrator authorization failed: %w", err)
	}
	fail := func(err error) (*NetworkNamespace, error) {
		ns.Close()
		return nil, err
	}
	run := func(args ...string) error { return ns.root(ctx, args...) }

	fmt.Fprintf(output, "[network] creating isolated namespace and veth for %s\n", it.Name)
	if err := run("ip", "netns", "add", ns.Name); err != nil {
		return fail(fmt.Errorf("create network namespace: %w", err))
	}
	ns.namespaceCreated = true
	if err := run("install", "-d", "-m", "755", "/etc/netns"); err != nil {
		return fail(fmt.Errorf("prepare isolated DNS directory: %w", err))
	}
	if err := run("mkdir", "-m", "755", ns.resolvDir); err != nil {
		return fail(fmt.Errorf("prepare isolated DNS: %w", err))
	}
	ns.resolvDirCreated = true
	resolv := filepath.Join(ns.resolvDir, "resolv.conf")
	if err := run("sh", "-c", "printf 'nameserver 1.1.1.1\\nnameserver 9.9.9.9\\n' > "+shellQuote(resolv)); err != nil {
		return fail(fmt.Errorf("prepare isolated DNS: %w", err))
	}

	if err := run("ip", "link", "add", ns.hostVeth, "type", "veth", "peer", "name", peer); err != nil {
		return fail(fmt.Errorf("create isolated veth pair: %w", err))
	}
	ns.hostVethCreated = true
	if err := run("ip", "link", "set", peer, "netns", ns.Name); err != nil {
		return fail(fmt.Errorf("move veth into network namespace: %w", err))
	}
	if err := run("ip", "addr", "add", ns.HostIP+"/30", "dev", ns.hostVeth); err != nil {
		return fail(fmt.Errorf("configure host veth address: %w", err))
	}
	if err := run("ip", "link", "set", ns.hostVeth, "up"); err != nil {
		return fail(fmt.Errorf("activate host veth: %w", err))
	}
	if err := run("ip", "-n", ns.Name, "link", "set", "lo", "up"); err != nil {
		return fail(fmt.Errorf("activate namespace loopback: %w", err))
	}
	if err := run("ip", "-n", ns.Name, "addr", "add", ns.ProxyIP+"/30", "dev", peer); err != nil {
		return fail(fmt.Errorf("configure namespace veth address: %w", err))
	}
	if err := run("ip", "-n", ns.Name, "link", "set", peer, "up"); err != nil {
		return fail(fmt.Errorf("activate namespace veth: %w", err))
	}
	if err := run("ip", "-n", ns.Name, "route", "add", "default", "via", ns.HostIP, "dev", peer); err != nil {
		return fail(fmt.Errorf("configure namespace default route: %w", err))
	}

	// Keep reply traffic to the namespace on the connected main-table route,
	// even when a host VPN installs higher-priority policy rules. Packets
	// arriving from this veth use a dedicated table whose default route points
	// directly at the requested host interface, before VPN rules are evaluated.
	fmt.Fprintf(output, "[network] installing policy route via %s (gateway %s)\n", it.Name, gateway)
	if err := run("ip", "-4", "route", "add", interfacePrefix, "dev", it.Name, "scope", "link", "src", interfaceIP.String(), "table", strconv.Itoa(ns.routeTable)); err != nil {
		return fail(fmt.Errorf("add connected route for %s to isolated routing table: %w", it.Name, err))
	}
	ns.routeTableCreated = true
	_, selectedPrefix, _ := net.ParseCIDR(interfacePrefix)
	if gateway != "" && (selectedPrefix == nil || !selectedPrefix.Contains(net.ParseIP(gateway))) {
		if err := run("ip", "-4", "route", "add", gateway+"/32", "dev", it.Name, "scope", "link", "table", strconv.Itoa(ns.routeTable)); err != nil {
			return fail(fmt.Errorf("add off-link gateway route %s through %s: %w", gateway, it.Name, err))
		}
	}
	defaultRoute := []string{"ip", "-4", "route", "add", "default"}
	if gateway != "" {
		defaultRoute = append(defaultRoute, "via", gateway)
	}
	defaultRoute = append(defaultRoute, "dev", it.Name, "table", strconv.Itoa(ns.routeTable))
	if err := run(defaultRoute...); err != nil {
		return fail(fmt.Errorf("add isolated default route through %s: %w", it.Name, err))
	}
	if err := run("ip", "-4", "rule", "add", "pref", "1", "to", ns.subnet, "lookup", "main"); err != nil {
		return fail(fmt.Errorf("add return route policy rule: %w", err))
	}
	ns.returnRuleCreated = true
	if err := run("ip", "-4", "rule", "add", "pref", "2", "iif", ns.hostVeth, "lookup", strconv.Itoa(ns.routeTable)); err != nil {
		return fail(fmt.Errorf("add selected-interface policy rule: %w", err))
	}
	ns.egressRuleCreated = true

	if err := ns.enableForwarding(ctx); err != nil {
		return fail(fmt.Errorf("enable temporary IPv4 forwarding: %w", err))
	}
	if err := ns.installFirewall(ctx, it.Name); err != nil {
		return fail(fmt.Errorf("install temporary forwarding/NAT rules: %w", err))
	}

	fmt.Fprintf(output, "[network] isolated namespace %s is online via %s (existing host address %s; no extra DHCP/MAC)\n", ns.Name, it.Name, interfaceIP)
	return ns, nil
}

func selectedInterfaceRoute(ctx context.Context, iface *net.Interface) (net.IP, string, string, error) {
	addrList, err := iface.Addrs()
	if err != nil {
		return nil, "", "", fmt.Errorf("inspect addresses on %s: %w", iface.Name, err)
	}
	type address struct {
		ip     net.IP
		prefix string
	}
	var addresses []address
	for _, value := range addrList {
		ip, network, parseErr := net.ParseCIDR(value.String())
		if parseErr != nil || ip.To4() == nil || !ip.IsGlobalUnicast() {
			continue
		}
		ip = ip.To4()
		prefix := (&net.IPNet{IP: ip.Mask(network.Mask), Mask: network.Mask}).String()
		addresses = append(addresses, address{ip: ip, prefix: prefix})
	}
	if len(addresses) == 0 {
		return nil, "", "", fmt.Errorf("network interface %q has no usable global IPv4 address", iface.Name)
	}

	cmd := exec.CommandContext(ctx, "ip", "-4", "route", "show", "default", "dev", iface.Name)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, "", "", fmt.Errorf("inspect IPv4 default route through %s: %w: %s", iface.Name, err, strings.TrimSpace(string(out)))
	}
	gateway, source, err := parseDefaultRoute(string(out))
	if err != nil {
		// Some VPN/network managers keep the physical interface's gateway in
		// a non-main table. Search all tables before declaring it unavailable.
		allRoutes, routeErr := exec.CommandContext(ctx, "ip", "-4", "route", "show", "table", "all").CombinedOutput()
		if routeErr == nil {
			gateway, source, err = parseDefaultRouteForInterface(string(allRoutes), iface.Name)
		}
	}
	if err != nil {
		return nil, "", "", fmt.Errorf("network interface %q has no IPv4 default route; connect it to a network with a gateway: %w", iface.Name, err)
	}
	for _, addr := range addresses {
		if source == "" || addr.ip.Equal(net.ParseIP(source)) {
			return addr.ip, addr.prefix, gateway, nil
		}
	}
	return nil, "", "", fmt.Errorf("default route through %s selects source %s, but that address is not assigned to the interface", iface.Name, source)
}

func parseDefaultRouteForInterface(routes, iface string) (gateway, source string, err error) {
	for _, line := range strings.Split(routes, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] != "default" {
			continue
		}
		for i := 0; i+1 < len(fields); i++ {
			if fields[i] == "dev" && fields[i+1] == iface {
				return parseDefaultRoute(line)
			}
		}
	}
	return "", "", fmt.Errorf("no default route found for interface %s", iface)
}

func parseDefaultRoute(routes string) (gateway, source string, err error) {
	for _, line := range strings.Split(routes, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] != "default" {
			continue
		}
		for i := 0; i+1 < len(fields); i++ {
			switch fields[i] {
			case "via":
				gateway = fields[i+1]
			case "src":
				source = fields[i+1]
			}
		}
		if gateway != "" {
			ip := net.ParseIP(gateway)
			if ip == nil || ip.To4() == nil {
				return "", "", fmt.Errorf("invalid IPv4 gateway %q", gateway)
			}
		}
		if source != "" {
			ip := net.ParseIP(source)
			if ip == nil || ip.To4() == nil {
				return "", "", fmt.Errorf("invalid IPv4 source address %q", source)
			}
		}
		return gateway, source, nil
	}
	return "", "", fmt.Errorf("no default route found")
}

func chooseProxySubnet(ctx context.Context, pid int) (*net.IPNet, string, string, error) {
	cmd := exec.CommandContext(ctx, "ip", "-4", "route", "show", "table", "all")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, "", "", fmt.Errorf("inspect existing IPv4 routes before allocating the namespace link: %w: %s", err, strings.TrimSpace(string(out)))
	}
	used := parseRouteNetworks(string(out))
	// Try multiple non-public address pools. VPN clients commonly install a
	// broad 10.0.0.0/8 route, so relying on one hard-coded 10.254.0.0/16 block
	// makes an otherwise healthy machine appear to have no free subnet.
	for _, poolCIDR := range []string{
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
		"100.64.0.0/10",
		"169.254.0.0/16",
		"198.18.0.0/15",
		"192.0.2.0/24",
		"198.51.100.0/24",
		"203.0.113.0/24",
	} {
		_, pool, parseErr := net.ParseCIDR(poolCIDR)
		if parseErr != nil {
			continue
		}
		ones, bits := pool.Mask.Size()
		if bits != 32 || ones > 16 {
			continue
		}
		poolBase := binary.BigEndian.Uint32(pool.IP.To4())
		subnetCount := 1 << uint(16-ones)
		// Search independently inside /16 chunks. This lets us skip a whole
		// chunk covered by a VPN route instead of iterating millions of /30s.
		for chunkIndex := 0; chunkIndex < subnetCount; chunkIndex++ {
			chunkBase := poolBase + uint32(chunkIndex<<16)
			chunkIP := make(net.IP, net.IPv4len)
			binary.BigEndian.PutUint32(chunkIP, chunkBase)
			chunk := &net.IPNet{IP: chunkIP, Mask: net.CIDRMask(16, 32)}
			if networkFullyCoveredByRoute(chunk, used) {
				continue
			}
			const candidatesPer16 = 1 << 14
			start := pid % candidatesPer16
			for offset := 0; offset < candidatesPer16; offset++ {
				index := (start + offset) % candidatesPer16
				base := make(net.IP, net.IPv4len)
				binary.BigEndian.PutUint32(base, chunkBase+uint32(index*4))
				candidate := &net.IPNet{IP: base, Mask: net.CIDRMask(30, 32)}
				if overlapsAnyRoute(candidate, used) {
					continue
				}
				host := append(net.IP(nil), base...)
				host[3]++
				proxy := append(net.IP(nil), base...)
				proxy[3] += 2
				return candidate, host.String(), proxy.String(), nil
			}
		}
	}
	return nil, "", "", fmt.Errorf("no unused /30 subnet found in private/link-local/benchmark address pools (10/8, 172.16/12, 192.168/16, 100.64/10, 169.254/16, 198.18/15, 192.0.2/24, 198.51.100/24, 203.0.113/24); existing routes or addresses conflict with every candidate")
}

// networkFullyCoveredByRoute avoids scanning all addresses in a /16 when an
// existing broader route (for example 10.0.0.0/8) covers that entire chunk.
func networkFullyCoveredByRoute(pool *net.IPNet, routes []*net.IPNet) bool {
	if pool == nil {
		return true
	}
	first := pool.IP.To4()
	if len(first) != net.IPv4len {
		return true
	}
	last := append(net.IP(nil), first...)
	for i := len(last) - 1; i >= 0; i-- {
		last[i] |= ^pool.Mask[i]
	}
	for _, route := range routes {
		ones, bits := route.Mask.Size()
		// Ignore catch-all and split-default routes (0/0, 0/1, 128/1).
		// They describe the host's normal egress path, not an occupied
		// connected subnet: our /30 veth route is more specific and is
		// explicitly selected by the destination policy rule.
		if bits != 32 || ones <= 1 {
			continue
		}
		if route.Contains(first) && route.Contains(last) {
			return true
		}
	}
	return false
}

func parseRouteNetworks(routes string) []*net.IPNet {
	var result []*net.IPNet
	routeTypes := map[string]bool{
		"local": true, "broadcast": true, "unicast": true,
		"multicast": true, "blackhole": true, "unreachable": true,
		"prohibit": true, "throw": true, "nat": true, "anycast": true,
	}
	for _, line := range strings.Split(routes, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		destination := fields[0]
		// iproute2 may prefix special route types before the destination:
		// "local 192.0.2.1 dev lo table local".
		if routeTypes[destination] {
			if len(fields) < 2 {
				continue
			}
			destination = fields[1]
		}
		// Default and split-default routes describe egress policy, not an
		// occupied connected address range. Never parse their gateway/src as
		// if those addresses were the route destination.
		if destination == "default" {
			continue
		}
		if _, network, err := net.ParseCIDR(destination); err == nil && network.IP.To4() != nil {
			network.IP = network.IP.To4()
			result = append(result, network)
			continue
		}
		if ip := net.ParseIP(destination); ip != nil && ip.To4() != nil {
			result = append(result, &net.IPNet{IP: ip.To4(), Mask: net.CIDRMask(32, 32)})
		}
	}
	return result
}

func overlapsAnyRoute(candidate *net.IPNet, routes []*net.IPNet) bool {
	for _, route := range routes {
		ones, bits := route.Mask.Size()
		// Broad default/split-default routes are not connected address
		// allocations. A more-specific connected veth route wins over them.
		if bits != 32 || ones <= 1 {
			continue
		}
		if route.Contains(candidate.IP) || candidate.Contains(route.IP) {
			return true
		}
	}
	return false
}

func chooseRouteTable(ctx context.Context, pid int) (int, error) {
	start := 10000 + pid%1000000
	for offset := 0; offset < 10000; offset++ {
		table := start + offset
		cmd := exec.CommandContext(ctx, "ip", "-4", "route", "show", "table", strconv.Itoa(table))
		out, err := cmd.CombinedOutput()
		if err == nil && strings.TrimSpace(string(out)) == "" {
			return table, nil
		}
		if err != nil && strings.Contains(strings.ToLower(string(out)), "fib table does not exist") {
			return table, nil
		}
	}
	return 0, fmt.Errorf("could not allocate an unused IPv4 routing table")
}

func (n *NetworkNamespace) enableForwarding(ctx context.Context) error {
	forwardPath := "/proc/sys/net/ipv4/ip_forward"
	forward, err := os.ReadFile(forwardPath)
	if err != nil {
		return fmt.Errorf("read %s: %w", forwardPath, err)
	}
	if _, err := os.ReadFile(n.rpFilterPath); err != nil {
		return fmt.Errorf("read %s: %w", n.rpFilterPath, err)
	}
	n.ipForwardOriginal = strings.TrimSpace(string(forward))
	n.sysctlOriginal = snapshotIPv4ForwardingSysctls()

	// ip_forward switches Linux between host/router defaults and can reset
	// IPv4 interface settings. Snapshot those settings before enabling it.
	if n.ipForwardOriginal != "1" {
		if err := n.writeRootFile(ctx, forwardPath, "1"); err != nil {
			return err
		}
		n.ipForwardChanged = true
	}
	if err := n.writeRootFile(ctx, n.rpFilterPath, "2"); err != nil {
		return fmt.Errorf("set loose reverse-path filtering on the selected interface: %w", err)
	}
	return nil
}

func snapshotIPv4ForwardingSysctls() map[string]string {
	const root = "/proc/sys/net/ipv4/conf"
	values := make(map[string]string)
	dirs, err := os.ReadDir(root)
	if err != nil {
		return values
	}
	for _, dir := range dirs {
		if !dir.IsDir() {
			continue
		}
		confDir := filepath.Join(root, dir.Name())
		entries, readErr := os.ReadDir(confDir)
		if readErr != nil {
			continue
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			path := filepath.Join(confDir, entry.Name())
			data, readErr := os.ReadFile(path)
			if readErr == nil {
				values[path] = strings.TrimSpace(string(data))
			}
		}
	}
	return values
}

func (n *NetworkNamespace) installFirewall(ctx context.Context, iface string) error {
	if _, err := exec.LookPath("nft"); err == nil {
		if err := n.rootInput(ctx, n.nftRules(iface), "nft", "-f", "-"); err == nil {
			n.firewallBackend = "nft"
			n.firewallCreated = true
			return nil
		} else if _, iptablesErr := exec.LookPath("iptables"); iptablesErr == nil {
			if fallbackErr := n.installIPTables(ctx, iface); fallbackErr == nil {
				return nil
			} else {
				return fmt.Errorf("nft failed (%v); iptables fallback failed (%w)", err, fallbackErr)
			}
		} else {
			return fmt.Errorf("nft: %w", err)
		}
	}
	if _, err := exec.LookPath("iptables"); err == nil {
		return n.installIPTables(ctx, iface)
	}
	return fmt.Errorf("neither nft nor iptables is installed")
}

func (n *NetworkNamespace) nftRules(iface string) string {
	hostVeth := nftQuote(n.hostVeth)
	physical := nftQuote(iface)
	return fmt.Sprintf("table ip %s {\n"+
		"  chain forward {\n"+
		"    type filter hook forward priority -10; policy accept;\n"+
		"    iifname %s oifname %s ip saddr %s counter accept\n"+
		"    iifname %s oifname %s ip daddr %s ct state established,related counter accept\n"+
		"  }\n"+
		"  chain postrouting {\n"+
		"    type nat hook postrouting priority 99; policy accept;\n"+
		"    oifname %s ip saddr %s counter masquerade\n"+
		"  }\n"+
		"}\n",
		n.firewallTable, hostVeth, physical, n.ProxyIP, physical, hostVeth, n.ProxyIP, physical, n.ProxyIP)
}

func nftQuote(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "\"", "\\\"")
	return "\"" + value + "\""
}

func (n *NetworkNamespace) installIPTables(ctx context.Context, iface string) error {
	outbound := []string{"-w", "10", "-I", "FORWARD", "1", "-i", n.hostVeth, "-o", iface, "-s", n.ProxyIP + "/32", "-j", "ACCEPT"}
	inbound := []string{"-w", "10", "-I", "FORWARD", "1", "-i", iface, "-o", n.hostVeth, "-d", n.ProxyIP + "/32", "-m", "conntrack", "--ctstate", "RELATED,ESTABLISHED", "-j", "ACCEPT"}
	masquerade := []string{"-w", "10", "-t", "nat", "-I", "POSTROUTING", "1", "-s", n.ProxyIP + "/32", "-o", iface, "-j", "MASQUERADE"}

	if err := n.root(ctx, append([]string{"iptables"}, outbound...)...); err != nil {
		return fmt.Errorf("allow namespace egress: %w", err)
	}
	if err := n.root(ctx, append([]string{"iptables"}, inbound...)...); err != nil {
		_ = n.root(ctx, "iptables", "-w", "10", "-D", "FORWARD", "-i", n.hostVeth, "-o", iface, "-s", n.ProxyIP+"/32", "-j", "ACCEPT")
		return fmt.Errorf("allow namespace return traffic: %w", err)
	}
	if err := n.root(ctx, append([]string{"iptables"}, masquerade...)...); err != nil {
		_ = n.root(ctx, "iptables", "-w", "10", "-D", "FORWARD", "-i", iface, "-o", n.hostVeth, "-d", n.ProxyIP+"/32", "-m", "conntrack", "--ctstate", "RELATED,ESTABLISHED", "-j", "ACCEPT")
		_ = n.root(ctx, "iptables", "-w", "10", "-D", "FORWARD", "-i", n.hostVeth, "-o", iface, "-s", n.ProxyIP+"/32", "-j", "ACCEPT")
		return fmt.Errorf("enable namespace source NAT: %w", err)
	}
	n.firewallBackend = "iptables"
	n.firewallCreated = true
	return nil
}

func (n *NetworkNamespace) root(ctx context.Context, args ...string) error {
	cmd := exec.CommandContext(ctx, "sudo", append([]string{"-n"}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}

func (n *NetworkNamespace) rootInput(ctx context.Context, input string, args ...string) error {
	cmd := exec.CommandContext(ctx, "sudo", append([]string{"-n"}, args...)...)
	cmd.Stdin = strings.NewReader(input)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}

func (n *NetworkNamespace) writeRootFile(ctx context.Context, path, value string) error {
	script := "printf '%s\\n' " + shellQuote(value) + " > " + shellQuote(path)
	return n.root(ctx, "sh", "-c", script)
}

// Command runs a process inside the namespace as the invoking user.
func (n *NetworkNamespace) Command(ctx context.Context, binary string, args ...string) *exec.Cmd {
	uid, gid := strconv.Itoa(os.Getuid()), strconv.Itoa(os.Getgid())
	command := []string{"-n", "ip", "netns", "exec", n.Name, "setpriv", "--reuid", uid, "--regid", gid, "--init-groups", binary}
	command = append(command, args...)
	return exec.CommandContext(ctx, "sudo", command...)
}

func (n *NetworkNamespace) Close() {
	if n == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	if n.firewallCreated {
		switch n.firewallBackend {
		case "nft":
			_ = n.root(ctx, "nft", "delete", "table", "ip", n.firewallTable)
		case "iptables":
			_ = n.root(ctx, "iptables", "-w", "10", "-t", "nat", "-D", "POSTROUTING", "-s", n.ProxyIP+"/32", "-o", n.physicalInterface, "-j", "MASQUERADE")
			_ = n.root(ctx, "iptables", "-w", "10", "-D", "FORWARD", "-i", n.physicalInterface, "-o", n.hostVeth, "-d", n.ProxyIP+"/32", "-m", "conntrack", "--ctstate", "RELATED,ESTABLISHED", "-j", "ACCEPT")
			_ = n.root(ctx, "iptables", "-w", "10", "-D", "FORWARD", "-i", n.hostVeth, "-o", n.physicalInterface, "-s", n.ProxyIP+"/32", "-j", "ACCEPT")
		}
	}
	if n.egressRuleCreated {
		_ = n.root(ctx, "ip", "-4", "rule", "del", "pref", "2", "iif", n.hostVeth, "lookup", strconv.Itoa(n.routeTable))
	}
	if n.returnRuleCreated {
		_ = n.root(ctx, "ip", "-4", "rule", "del", "pref", "1", "to", n.subnet, "lookup", "main")
	}
	if n.routeTableCreated {
		_ = n.root(ctx, "ip", "-4", "route", "flush", "table", strconv.Itoa(n.routeTable))
	}
	if n.namespaceCreated {
		_ = n.root(ctx, "ip", "netns", "del", n.Name)
	}
	if n.hostVethCreated {
		_ = n.root(ctx, "ip", "link", "del", n.hostVeth)
	}
	if n.resolvDirCreated {
		_ = n.root(ctx, "rm", "-rf", n.resolvDir)
	}
	// Restore ip_forward first: Linux may reset host/router defaults when it
	// changes. Then put back the saved per-interface IPv4 settings in one root
	// shell, including rp_filter on the selected interface.
	if n.ipForwardChanged {
		_ = n.writeRootFile(ctx, "/proc/sys/net/ipv4/ip_forward", n.ipForwardOriginal)
	}
	if len(n.sysctlOriginal) > 0 {
		paths := make([]string, 0, len(n.sysctlOriginal))
		for path := range n.sysctlOriginal {
			paths = append(paths, path)
		}
		sort.Strings(paths)
		var script strings.Builder
		for _, path := range paths {
			fmt.Fprintf(&script, "printf '%%s\\n' %s > %s 2>/dev/null || true\n", shellQuote(n.sysctlOriginal[path]), shellQuote(path))
		}
		_ = n.root(ctx, "sh", "-c", script.String())
	}
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}
