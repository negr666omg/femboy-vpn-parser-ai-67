# SubWatch 0.2.0

Linux-first Go CLI for parsing and deduplicating VLESS/VMess/Trojan/Shadowsocks/Hysteria2/TUIC/SOCKS/HTTP subscription links, then testing them in strict global stages through sing-box.

## Build from source

The repository includes the current source archive as `subwatch-source.tar.gz`; CI verifies it and also publishes it as the `subwatch-source` artifact.

```bash
mkdir -p subwatch-source
tar -xzf subwatch-source.tar.gz -C subwatch-source
cd subwatch-source
go test ./...
go vet ./...
go build -trimpath -ldflags='-s -w' -o sw ./cmd/subwatch
```

## First run

```bash
chmod +x sw
./sw init
# edit sources.txt and config.json
./sw interfaces
./sw run
```

`sw init` creates `sources.txt` and `config.json`. Each non-comment line in `sources.txt` can be a subscription URL, a local subscription file path, or one share URI.

## Strict processing pipeline

Every stage has a global barrier. SubWatch does not speed-test a node while another node is still undergoing its URL test.

1. **Parse** all sources and write every parsed node to `all.json`.
2. **Deduplicate** by outbound configuration and write `unique.json`. Protocols not listed in `protocols.enabled` are excluded from testing and shown in `selected.json`.
3. **URL test** all selected nodes. Only successful nodes are written to `url-test-finished.json`.
4. **Speed test** reads `url-test-finished.json` from disk. A node passes only when both download and upload meet their configured minimums. Passing nodes are written to `speedtest-finished.json`.
5. `tested.json` is an exact byte-for-byte copy of `speedtest-finished.json`.
6. **Service check** reads only `speedtest-finished.json`. Only Gemini `available` nodes are written to `final.json`; `partial`, `unknown`, `blocked`, and `failed` are excluded.

The CLI prints stage progress (`done/total`, passed and failed counts), plus a short summary of common failure reasons. On the first Linux run, sing-box is downloaded and SHA-256 verified; download progress is printed instead of leaving the terminal apparently idle.

## Settings (`config.json`)

See [`config.example.json`](./config.example.json). All settings are optional; missing values use defaults.

- `network.interface`: `auto`, `eno1`, `throne-tun`, or another local interface name.
- `sources.file`: source list path.
- `protocols.enabled`: protocols to test (`vless`, `vmess`, `trojan`, `ss`, `hysteria2`, `tuic`, `socks`, `http`). Aliases `shadowsocks`, `hy2`, and `socks5` are accepted.
- `url_test`: test URL, concurrency, timeout.
- `speed_test`: concurrency, timeout, minimum download/upload Mbps, test payload sizes.
- `service_check`: concurrency, timeout, optional exit-IP lookup.
- `output`: output directory, protocol splitting, and final subscription text output.

CLI flags override the config file. Legacy `--concurrency` and `--timeout` set values for all three stages; individual options `--url-concurrency`, `--speed-concurrency`, `--service-concurrency`, `--url-timeout`, `--speed-timeout`, and `--service-timeout` are also available.

## Output files

- `all.json` — every parsed node, before deduplication.
- `unique.json` — deduplicated nodes.
- `selected.json` — deduplicated nodes using enabled protocols only.
- `url-test-finished.json` — only URL-test successes; `url-test-failed.json` stores rejected nodes and reasons.
- `speedtest-finished.json` — only nodes above both speed thresholds; `speedtest-failed.json` stores rejected nodes and reasons.
- `tested.json` — byte-for-byte copy of `speedtest-finished.json`.
- `service-check-results.json` — all service-check outcomes.
- `final.json` — only nodes with Gemini status `available`.
- `report.csv` — diagnostic report for selected nodes.
- `final.txt` — final subscription links. Recognized metadata headers (`#profile-*`, `#announce`, `#support-url`) are retained from the source subscriptions.

When `output.split_by_protocol` is enabled, stage and final outputs are also split into files such as `url-test-finished-vless.json`, `speedtest-finished-ss.json`, `tested-vless.json`, and `final-vless.json`; final subscription text is split into `final-vless.txt`, etc.

Examples:

```bash
./sw run --config config.json --interface throne-tun
./sw run --protocols vless,ss --url-concurrency 12 --speed-concurrency 3
./sw run --min-download 10 --min-upload 5 --out ./results
```

The Gemini check classifies a public signed-out page response only. It does not authenticate or test account-specific eligibility. Real-world reachability depends on the supplied subscriptions, network, target endpoints, and local routing configuration.
