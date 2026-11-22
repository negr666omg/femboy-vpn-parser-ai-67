//go:build linux

package core

import (
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
)

var (
	subWatchIDPattern = regexp.MustCompile(`^[0-9]+[0-9a-z]*$`)
	subWatchNSPattern = regexp.MustCompile(`^sw-([0-9]+[0-9a-z]*)$`)
	subWatchVethPattern = regexp.MustCompile(`^swh([0-9]+[0-9a-z]*)$`)
	subWatchNFTPattern = regexp.MustCompile(`^table ip subwatch([0-9]+[0-9a-z]*)$`)
)

// CleanupNetworkIsolation removes only stale resources whose names match
// SubWatch's private namespace, veth and nftables naming scheme. It deliberately
// leaves shared sysctls, UFW, Throne and every unrelated firewall/routing rule
// untouched.
func CleanupNetworkIsolation(output io.Writer) error {
	if output == nil {
		output = io.Discard
	}
	if _, err := exec.LookPath("ip"); err != nil {
		return fmt.Errorf("iproute2 is required for cleanup: %w", err)
	}
	auth := exec.Command("sudo", "-v")
	auth.Stdin, auth.Stdout, auth.Stderr = os.Stdin, os.Stdout, os.Stderr
	if err := auth.Run(); err != nil {
		return fmt.Errorf("administrator authorization failed: %w", err)
	}

	ids := make(map[string]bool)
	addID := func(id string) {
		if subWatchIDPattern.MatchString(id) {
			ids[id] = true
		}
	}
	nsOut, err := runCleanupRoot("ip", "netns", "list")
	if err != nil {
		return fmt.Errorf("list network namespaces: %w", err)
	}
	namespaces := make(map[string]string)
	for _, line := range strings.Split(nsOut, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		match := subWatchNSPattern.FindStringSubmatch(fields[0])
		if len(match) == 2 {
			addID(match[1])
			namespaces[match[1]] = fields[0]
		}
	}

	linkOut, err := runCleanupRoot("ip", "-o", "link", "show")
	if err != nil {
		return fmt.Errorf("list network links: %w", err)
	}
	links := make(map[string]bool)
	for _, line := range strings.Split(linkOut, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		name := strings.TrimSuffix(fields[1], ":")
		name = strings.SplitN(name, "@", 2)[0]
		match := subWatchVethPattern.FindStringSubmatch(name)
		if len(match) == 2 {
			addID(match[1])
			links[name] = true
		}
	}

	if _, lookErr := exec.LookPath("nft"); lookErr == nil {
		if tableOut, tableErr := runCleanupRoot("nft", "list", "tables"); tableErr == nil {
			for _, line := range strings.Split(tableOut, "\n") {
				match := subWatchNFTPattern.FindStringSubmatch(strings.TrimSpace(line))
				if len(match) == 2 {
					addID(match[1])
				}
			}
		}
	}
	rulesOut, err := runCleanupRoot("ip", "-4", "rule", "show")
	if err != nil {
		return fmt.Errorf("list IPv4 policy rules: %w", err)
	}
	for _, line := range strings.Split(rulesOut, "\n") {
		fields := strings.Fields(line)
		for i := 0; i+1 < len(fields); i++ {
			if fields[i] == "iif" {
				if match := subWatchVethPattern.FindStringSubmatch(fields[i+1]); len(match) == 2 {
					addID(match[1])
				}
			}
		}
	}
	if _, lookErr := exec.LookPath("iptables"); lookErr == nil {
		if filterOut, filterErr := runCleanupRoot("iptables", "-S", "FORWARD"); filterErr == nil {
			for _, line := range strings.Split(filterOut, "\n") {
				fields := strings.Fields(line)
				for i := 0; i+1 < len(fields); i++ {
					if fields[i] == "-i" || fields[i] == "-o" {
						if match := subWatchVethPattern.FindStringSubmatch(fields[i+1]); len(match) == 2 {
							addID(match[1])
						}
					}
				}
			}
		}
	}
	if len(ids) == 0 {
		fmt.Fprintln(output, "[cleanup] no stale SubWatch network resources found")
		return nil
	}

	removed, skipped := 0, 0
	for id := range ids {
		namespace := namespaces[id]
		hostVeth := "swh" + id
		if namespace != "" {
			pidsOut, pidsErr := runCleanupRoot("ip", "netns", "pids", namespace)
			if pidsErr != nil {
				fmt.Fprintf(output, "[cleanup] skipped %s: cannot verify whether it is active: %v\n", namespace, pidsErr)
				skipped++
				continue
			}
			if strings.TrimSpace(pidsOut) != "" {
				fmt.Fprintf(output, "[cleanup] skipped %s: active process(es): %s\n", namespace, strings.TrimSpace(pidsOut))
				skipped++
				continue
			}
		}

		var proxyCIDR, returnSubnet string
		if links[hostVeth] {
			addrOut, addrErr := runCleanupRoot("ip", "-o", "-4", "addr", "show", "dev", hostVeth)
			if addrErr == nil {
				for _, field := range strings.Fields(addrOut) {
					if strings.Contains(field, "/") {
						ip, network, parseErr := net.ParseCIDR(field)
						if parseErr == nil && ip.To4() != nil {
							returnSubnet = network.String()
							proxyIP := append(net.IP(nil), network.IP.To4()...)
							proxyIP[3] += 2 // SubWatch assigns host .1 and namespace .2 in its /30.
							proxyCIDR = proxyIP.String() + "/32"
							break
						}
					}
				}
			}
		}

		// Delete only SubWatch's dedicated nft table, never a shared table.
		if _, lookErr := exec.LookPath("nft"); lookErr == nil {
			if _, deleteErr := runCleanupRoot("nft", "delete", "table", "ip", "subwatch"+id); deleteErr == nil {
				removed++
				fmt.Fprintf(output, "[cleanup] removed nft table ip subwatch%s\n", id)
			}
		}

		// Remove only iptables rules tied to this uniquely named veth or its
		// derived /30 peer address (legacy fallback used by older SubWatch builds).
		if _, lookErr := exec.LookPath("iptables"); lookErr == nil {
			removed += cleanupIPTablesRules(hostVeth, proxyCIDR, output)
		}

		flushedTables := make(map[string]bool)
		for _, rule := range strings.Split(rulesOut, "\n") {
			fields := strings.Fields(rule)
			if len(fields) < 2 || !strings.HasSuffix(fields[0], ":") {
				continue
			}
			pref := strings.TrimSuffix(fields[0], ":")
			if _, parseErr := strconv.Atoi(pref); parseErr != nil {
				continue
			}

			// New versions select egress by the namespace source IP. Keep
			// compatibility with the older ingress-interface selector too.
			var selector []string
			for i := 0; i+1 < len(fields); i++ {
				switch fields[i] {
				case "iif":
					if fields[i+1] == hostVeth {
						selector = []string{"iif", hostVeth}
					}
				case "from":
					proxyIP := strings.TrimSuffix(proxyCIDR, "/32")
					if proxyCIDR != "" && (fields[i+1] == proxyCIDR || fields[i+1] == proxyIP) {
						selector = []string{"from", proxyCIDR}
					}
				}
				if len(selector) == 0 {
					continue
				}

				table := ""
				for j := i + 2; j+1 < len(fields); j++ {
					if fields[j] == "lookup" {
						table = fields[j+1]
						break
					}
				}
				if table == "" {
					continue
				}
				if _, tableErr := strconv.Atoi(table); tableErr != nil {
					continue
				}
				args := []string{"ip", "-4", "rule", "del", "pref", pref}
				args = append(args, selector...)
				args = append(args, "lookup", table)
				if _, delErr := runCleanupRoot(args...); delErr == nil {
					fmt.Fprintf(output, "[cleanup] removed policy rule for %s (table %s)\n", hostVeth, table)
					removed++
				}
				if !flushedTables[table] {
					if _, flushErr := runCleanupRoot("ip", "-4", "route", "flush", "table", table); flushErr == nil {
						fmt.Fprintf(output, "[cleanup] flushed SubWatch route table %s\n", table)
						removed++
					}
					flushedTables[table] = true
				}
				break
			}

			if returnSubnet != "" && strings.Contains(rule, "to "+returnSubnet+" ") && strings.Contains(rule, "lookup main") {
				if _, delErr := runCleanupRoot("ip", "-4", "rule", "del", "pref", pref, "to", returnSubnet, "lookup", "main"); delErr == nil {
					fmt.Fprintf(output, "[cleanup] removed return-route rule for %s\n", returnSubnet)
					removed++
				}
			}
		}

		if namespace != "" {
			if _, delErr := runCleanupRoot("ip", "netns", "del", namespace); delErr == nil {
				fmt.Fprintf(output, "[cleanup] removed stale namespace %s\n", namespace)
				removed++
			}
		}
		if links[hostVeth] {
			if _, delErr := runCleanupRoot("ip", "link", "del", hostVeth); delErr == nil {
				fmt.Fprintf(output, "[cleanup] removed orphaned veth %s\n", hostVeth)
				removed++
			}
		}
		if namespace != "" {
			dir := "/etc/netns/" + namespace
			if _, statErr := runCleanupRoot("test", "-d", dir); statErr == nil {
				if _, rmErr := runCleanupRoot("rm", "-rf", "--", dir); rmErr == nil {
					fmt.Fprintf(output, "[cleanup] removed DNS directory %s\n", dir)
					removed++
				}
			}
		}
	}

	fmt.Fprintf(output, "[cleanup] finished: %d resource(s) removed, %d active/uncertain resource group(s) skipped\n", removed, skipped)
	fmt.Fprintln(output, "[cleanup] shared IPv4 sysctls were left unchanged; older versions did not save a crash-recovery backup")
	return nil
}

func cleanupIPTablesRules(hostVeth, proxyCIDR string, output io.Writer) int {
	removed := 0
	for _, spec := range [][]string{{"-S", "FORWARD"}, {"-t", "nat", "-S", "POSTROUTING"}} {
		list, err := runCleanupRoot(append([]string{"iptables"}, spec...)...)
		if err != nil {
			continue
		}
		tableArgs := []string{"-w", "10"}
		if len(spec) > 0 && spec[0] == "-t" {
			tableArgs = append(tableArgs, spec[0], spec[1])
		}
		for _, line := range strings.Split(list, "\n") {
			fields := strings.Fields(line)
			if len(fields) < 3 || fields[0] != "-A" {
				continue
			}
			isOwned := false
			for i := 0; i+1 < len(fields); i++ {
				if fields[i] == "-i" && fields[i+1] == hostVeth || fields[i] == "-o" && fields[i+1] == hostVeth {
					isOwned = true
				}
				if proxyCIDR != "" && fields[i] == "-s" && fields[i+1] == proxyCIDR {
					isOwned = true
				}
			}
			if !isOwned {
				continue
			}
			args := append(append([]string{}, tableArgs...), "-D", fields[1])
			args = append(args, fields[2:]...)
			if _, delErr := runCleanupRoot(append([]string{"iptables"}, args...)...); delErr == nil {
				fmt.Fprintf(output, "[cleanup] removed legacy iptables rule: %s\n", strings.Join(fields, " "))
				removed++
			}
		}
	}
	return removed
}

func runCleanupRoot(args ...string) (string, error) {
	cmd := exec.Command("sudo", append([]string{"-n"}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("%s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}
