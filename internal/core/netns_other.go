//go:build !linux

package core

import (
	"context"
	"fmt"
	"io"
	"net"
	"os/exec"
)

type NetworkNamespace struct { Name string; ProxyIP string }

func NewNetworkNamespace(ctx context.Context, iface string, output io.Writer) (*NetworkNamespace, error) {
	if iface != "" && iface != "auto" {
		return nil, fmt.Errorf("isolated interface mode is currently supported on Linux only")
	}
	return nil, nil
}

func (n *NetworkNamespace) Command(ctx context.Context, binary string, args ...string) *exec.Cmd {
	return exec.CommandContext(ctx, binary, args...)
}

func (n *NetworkNamespace) HostInterface() string { return "" }

func namespaceProxyDialer(string) *net.Dialer { return nil }

func (n *NetworkNamespace) Close() {}
