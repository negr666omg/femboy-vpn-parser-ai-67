//go:build linux

package core

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// NetworkNamespace provides an isolated network stack whose default route is
// obtained directly from the selected physical interface.
type NetworkNamespace struct {
	Name       string
	ProxyIP    string
	hostVeth   string
	macvlan    string
	dhcpPID    string
	dhcpLease  string
	resolvDir  string
	namespaceCreated bool
	hostVethCreated bool
	macvlanCreated bool
	dhcpAttempted bool
	resolvDirCreated bool
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
// selects its host-side address. This keeps SubWatch's local proxy transport
// reachable even when policy routing or a VPN changes the default route.
func namespaceProxyDialer(hostInterface string) *net.Dialer {
	if hostInterface == "" {
		return nil
	}
	return &net.Dialer{
		Timeout:   200 * time.Millisecond,
		LocalAddr: &net.TCPAddr{IP: net.ParseIP("10.254.0.1")},
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
	for _, tool := range []string{"ip", "sudo", "dhclient", "setpriv"} {
		if _, err := exec.LookPath(tool); err != nil {
			return nil, fmt.Errorf("Linux network isolation requires %s (install the corresponding system package)", tool)
		}
	}
	ns := &NetworkNamespace{
		Name: fmt.Sprintf("sw-%d", os.Getpid()),
		ProxyIP: "10.254.0.2",
		hostVeth: fmt.Sprintf("swh%d", os.Getpid()%1000000),
		dhcpPID: fmt.Sprintf("/run/dhclient-sw-%d.pid", os.Getpid()),
		dhcpLease: fmt.Sprintf("/var/lib/dhcp/dhclient-sw-%d.leases", os.Getpid()),
		resolvDir: filepath.Join("/etc/netns", fmt.Sprintf("sw-%d", os.Getpid())),
	}
	// Ask for administrator authorization once; all subsequent commands use sudo -n.
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
	if err := run("ip", "netns", "add", ns.Name); err != nil { return fail(fmt.Errorf("create network namespace: %w", err)) }
	ns.namespaceCreated = true
	if err := run("install", "-d", "-m", "755", "/etc/netns"); err != nil { return fail(fmt.Errorf("prepare isolated DNS directory: %w", err)) }
	if err := run("mkdir", "-m", "755", ns.resolvDir); err != nil { return fail(fmt.Errorf("prepare isolated DNS: %w", err)) }
	ns.resolvDirCreated = true
	resolv := filepath.Join(ns.resolvDir, "resolv.conf")
	if err := run("sh", "-c", "printf 'nameserver 1.1.1.1\\nnameserver 9.9.9.9\\n' > "+shellQuote(resolv)); err != nil {
		return fail(fmt.Errorf("prepare isolated DNS: %w", err))
	}
	peer := fmt.Sprintf("swn%d", os.Getpid()%1000000)
	if err := run("ip", "link", "add", ns.hostVeth, "type", "veth", "peer", "name", peer); err != nil { return fail(fmt.Errorf("create local proxy link: %w", err)) }
	ns.hostVethCreated = true
	if err := run("ip", "link", "set", peer, "netns", ns.Name); err != nil { return fail(fmt.Errorf("move proxy link into namespace: %w", err)) }
	if err := run("ip", "addr", "add", "10.254.0.1/30", "dev", ns.hostVeth); err != nil { return fail(fmt.Errorf("configure host proxy link: %w", err)) }
	if err := run("ip", "link", "set", ns.hostVeth, "up"); err != nil { return fail(err) }
	if err := run("ip", "-n", ns.Name, "link", "set", "lo", "up"); err != nil { return fail(err) }
	if err := run("ip", "-n", ns.Name, "addr", "add", "10.254.0.2/30", "dev", peer); err != nil { return fail(err) }
	if err := run("ip", "-n", ns.Name, "link", "set", peer, "up"); err != nil { return fail(err) }

	ns.macvlan = fmt.Sprintf("swm%d", os.Getpid()%1000000)
	if err := run("ip", "link", "add", "link", iface, "name", ns.macvlan, "type", "macvlan", "mode", "bridge"); err != nil {
		return fail(fmt.Errorf("cannot create isolated link on %s (the network adapter or Wi-Fi may not support an additional MAC address): %w", iface, err))
	}
	ns.macvlanCreated = true
	if err := run("ip", "link", "set", ns.macvlan, "netns", ns.Name); err != nil { return fail(fmt.Errorf("move isolated interface into namespace: %w", err)) }
	if err := run("ip", "-n", ns.Name, "link", "set", ns.macvlan, "up"); err != nil { return fail(err) }
	ns.dhcpAttempted = true
	if err := run("ip", "netns", "exec", ns.Name, "dhclient", "-4", "-1", "-v", "-pf", ns.dhcpPID, "-lf", ns.dhcpLease, ns.macvlan); err != nil {
		return fail(fmt.Errorf("DHCP did not obtain an address through %s; check that the network permits a second device/MAC: %w", iface, err))
	}
	route, err := ns.output(ctx, "ip", "-n", ns.Name, "route", "show", "default")
	if err != nil {
		return fail(fmt.Errorf("inspect isolated default route: %w", err))
	}
	if !defaultRouteUsesInterface(route, ns.macvlan) {
		return fail(fmt.Errorf("selected interface %s did not provide the isolated default route (expected dev %s, got %q)", iface, ns.macvlan, strings.TrimSpace(route)))
	}
	fmt.Fprintf(output, "[network] isolated Linux namespace %s is online via %s (%s)\n", ns.Name, iface, strings.TrimSpace(route))
	return ns, nil
}

func defaultRouteUsesInterface(routes, iface string) bool {
	for _, line := range strings.Split(routes, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] != "default" {
			continue
		}
		for i := 0; i+1 < len(fields); i++ {
			if fields[i] == "dev" && fields[i+1] == iface {
				return true
			}
		}
	}
	return false
}

func (n *NetworkNamespace) root(ctx context.Context, args ...string) error {
	cmd := exec.CommandContext(ctx, "sudo", append([]string{"-n"}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil { return fmt.Errorf("%s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out))) }
	return nil
}

func (n *NetworkNamespace) output(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "sudo", append([]string{"-n"}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil { return string(out), fmt.Errorf("%s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out))) }
	return string(out), nil
}

// Command runs a process inside the namespace as the invoking user.
func (n *NetworkNamespace) Command(ctx context.Context, binary string, args ...string) *exec.Cmd {
	uid, gid := strconv.Itoa(os.Getuid()), strconv.Itoa(os.Getgid())
	command := []string{"-n", "ip", "netns", "exec", n.Name, "setpriv", "--reuid", uid, "--regid", gid, "--init-groups", binary}
	command = append(command, args...)
	return exec.CommandContext(ctx, "sudo", command...)
}

func (n *NetworkNamespace) Close() {
	if n == nil { return }
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Release only resources this instance successfully created. This avoids
	// deleting a pre-existing link or namespace when setup fails due to a name
	// collision, and also handles a macvlan that was created but not moved.
	if n.dhcpAttempted && n.namespaceCreated {
		_ = n.root(ctx, "ip", "netns", "exec", n.Name, "dhclient", "-r", "-pf", n.dhcpPID)
	}
	if n.macvlanCreated {
		if n.namespaceCreated {
			_ = n.root(ctx, "ip", "-n", n.Name, "link", "del", n.macvlan)
		}
		_ = n.root(ctx, "ip", "link", "del", n.macvlan)
	}
	if n.namespaceCreated {
		_ = n.root(ctx, "ip", "netns", "del", n.Name)
	}
	if n.hostVethCreated {
		_ = n.root(ctx, "ip", "link", "del", n.hostVeth)
	}
	if n.dhcpAttempted {
		_ = n.root(ctx, "rm", "-f", n.dhcpPID, n.dhcpLease)
	}
	if n.resolvDirCreated {
		_ = n.root(ctx, "rm", "-rf", n.resolvDir)
	}
}

func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'" }
