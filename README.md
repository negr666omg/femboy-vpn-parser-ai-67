# SubWatch 0.5.9

Linux-first Go CLI for parsing and deduplicating VLESS/VMess/Trojan/Shadowsocks/Hysteria2/TUIC/SOCKS/HTTP subscription links, then testing them through sing-box.

## Build from source

```bash
tar -xzf subwatch-source-0.5.0.tar.gz -C subwatch-source
cd subwatch-source
go test ./...
go vet ./...
go build -trimpath -ldflags='-s -w' -o sw ./cmd/subwatch
```

## Linux interface isolation

When `network.interface` is set to a specific interface with an IPv4 address and default gateway, SubWatch creates a temporary Linux network namespace connected to the host by a private veth pair. The host forwards that namespace's traffic through a dedicated policy-routing table, using policy rules ahead of VPN catch-all rules. Before any proxy test starts, SubWatch asks the kernel for the route of a forwarded namespace packet and refuses to continue unless the selected device is the configured interface. It then applies temporary source NAT on that interface. This uses the host interface's existing address: no second MAC address, extra DHCP lease, or macvlan is involved. The host's main default route is not replaced, so an unrelated host VPN does not capture the namespace's forwarded traffic. SubWatch temporarily enables IPv4 forwarding, applies loose reverse-path filtering on the selected interface, and installs narrowly scoped nftables (preferred) or iptables rules; these settings, routes, rules, namespace and links are restored/removed when the run ends. Install `iproute2`, `sudo`, `util-linux` (`setpriv`), and either `nftables` (`nft`) or `iptables` first.

## Cleaning up stale network resources

SubWatch normally removes its temporary namespace, veth pair, policy-routing rules, route table and private firewall table when a run exits. If an older process was force-killed, run:

```bash
./sw cleanup
```

Cleanup only targets resources with SubWatch's private `sw-`, `swh` and `subwatch` names, and skips namespaces that still have processes. It never flushes the system firewall or changes UFW, Throne, unrelated routes, or shared IPv4 sysctls.

## First run

```bash
chmod +x sw
./sw run
```

When `config.json` does not exist, `sw run` creates it automatically with editable defaults. A starter `sources.txt` is also created if it is missing. Put subscription URLs, local file paths, or individual share links into `sources.txt`, one per line. Existing files are never overwritten. `sw init` remains available for explicit initialization.

## Processing pipeline — one pass per stage

1. Parse all subscription sources and write `all.txt`. It is the raw combined list of every supported parsed node, before global deduplication and protocol filtering; duplicate appearances are intentionally retained.
2. Deduplicate globally in memory and apply `protocols.enabled`. These are internal working sets and are not exported.
3. URL-test every selected unique node. Successful nodes are written to `url-tested.txt` and `url-tested-<protocol>.txt`. While the stage is running, these files refresh every 5 seconds or after 100 completed configs, whichever comes first; they are written again at stage completion.
4. After all URL tests finish, speed-test only URL successes. Both download and upload must meet their configured minimums. Passing nodes are written to `speedtested.txt` and `speedtested-<protocol>.txt`. With the optional fallback, Cloudflare is run only for nodes that fail Ookla.
5. After all speed checks finish, check Gemini only for speed-tested nodes. Available nodes receive the `Gemini 🐱` marker in their name in `all.txt`, `url-tested*.txt` and `speedtested*.txt`; no separate Gemini files are created.
6. In continuous mode, nodes receive the `🧱` marker after **three or more consecutive complete cycles** in which their URL test succeeds with latency **below 100 ms**. A failed URL test, latency of 100 ms or more, or a node missing from a cycle resets its streak. The marker is added before `Gemini 🐱`, and is reflected in all applicable result lists. Stability history is kept in memory and starts fresh when SubWatch is restarted; a single non-continuous run cannot earn the marker.
7. The final source-quality summary reports global source overlap and two separate funnels: configs exclusive to one source, and configs shared across sources (counted once globally). Each source is ranked by the number of new, source-exclusive configs that passed the speed test. Per-source rows show exclusive URL, speed and Gemini conversion rates; shared appearances are not double-counted in the global totals. Config identity is based on normalized connection settings, not display names, so renamed copies still match. Source links are displayed without credentials, paths or query tokens.

All subscription files are plain-text URI lists, one entry per line. `failed.txt` is the single diagnostic log: it includes malformed or unsupported input, source download/parse failures, URL and speed test failures, provider/fallback errors, Gemini/service errors, renaming problems and fatal program errors. Invalid URI credentials and URL query tokens are redacted from diagnostic entries. The output directory is cleaned of SubWatch-generated artifacts at the start of each run, so files from older versions do not linger.

## Live terminal status

Each testing stage renders one independently updated terminal row per concurrent worker. With a concurrency of 6, up to six configs are visible and updating at the same time; a worker reuses its row for the next config it receives. A summary row shows total progress, active workers, passed and failed counts. Every worker row is fitted to the current terminal width; the config name and stage details share the available space, while the `ping: N ms` field is kept at the far right. Long details are shortened to prevent wrapping, and ping is shown as `— ms` until the URL test succeeds. This value is the elapsed time of the configured HTTP URL request, not an ICMP echo measurement. After a successful URL test, the URI gets a country flag and English country name (or 🌐 Unknown if GeoIP cannot resolve it); after the speed stage, the final measured download/upload values are propagated back into all.txt and the URL-stage lists, including nodes that miss the configured speed thresholds. During URL-test each row displays its config, protocol, ping and result; during speed-test it displays that config's live download/upload throughput and byte progress; during Gemini check it keeps ping and speed figures visible and adds the Gemini status and region when available. When stdout is redirected instead of attached to a terminal, SubWatch retains the compact single-line progress output.

## Settings (`config.json`)

See [`config.example.json`](./config.example.json). All settings are optional; defaults are used for omitted values.

- `network.interface`: `auto` uses normal system routing. On Linux, selecting an interface with a usable IPv4 address and default route (for example `enp1s0` or `wlan0`) creates an isolated namespace connected through a private veth. A dedicated policy-routing table sends namespace packets through the selected interface using priority-0 rules immediately after the kernel's local-table rule, ahead of ordinary VPN policy rules. Startup verifies that the kernel actually selects the requested device before testing. Temporary NAT rules reuse its existing host address. This bypasses host VPN policy routing without requesting a second IP or MAC. It requires `iproute2`, `sudo`, `setpriv` (util-linux), and either `nftables` (`nft`) or `iptables`. The first run asks for administrator authorization. SubWatch temporarily enables IPv4 forwarding and loose reverse-path filtering on the selected interface, then restores their previous values and removes its firewall/routing rules when the run ends. The host's main default route and DNS configuration are not replaced. Set `network.isolate_source_downloads` to `true` to route remote HTTP(S) subscription downloads through this namespace as well; this requires a specific interface. The source list itself and parsing remain local.
- `network.isolate_source_downloads`: set to `true` to download HTTP(S) subscriptions from `sources.file` inside the same isolated namespace and through the selected `network.interface`. This requires a specific interface (not `auto`) and Linux. The default is `false`, preserving the previous host-network behavior. Reading `sources.file`, reading local source files, and parsing downloaded text still happen in the main process; only remote HTTP(S) downloads are isolated. The first-run sing-box binary download and optional SFTP upload remain outside this setting.
- `sources.file`: source list path. Relative paths are resolved relative to the config file.
- `sources.timeout`: maximum time allowed for each remote HTTP(S) subscription download, including downloads through the isolated interface. Uses Go duration syntax such as `"30s"`, `"2m"`, or `"5m"`; defaults to `"30s"`. Local files and inline links are not affected.
- `protocols.enabled`: protocols to test (`vless`, `vmess`, `trojan`, `ss`, `hysteria2`, `tuic`, `socks`, `http`). Aliases `shadowsocks`, `hy2`, and `socks5` are accepted.
- `url_test`: URL used for the reachability check, concurrency, and timeout.
- `speed_test.provider`: primary speed-test backend, either `cloudflare` (default) or `ookla`. Ookla retrieves the public Speedtest.net server list through each tested node, probes up to 12 candidates in parallel through that same proxy, and selects the lowest-latency reachable server. Each direction runs for up to 8 seconds. The displayed live rate is calculated from bytes as they arrive; the final speed and pass/fail thresholds use the maximum rolling 2-second average. No server ID, API key, or separate Ookla CLI installation is needed.
- `speed_test.cloudflare_fallback.enabled`: optional second test, valid when `provider` is `ookla`. When enabled, SubWatch first runs Ookla on every URL-passed config. Only configs that fail Ookla (connection/test error or download/upload below the configured minimum) are sent to Cloudflare. A Cloudflare pass rescues the config into the combined `speedtested*.txt` results; a Cloudflare failure leaves it failed. Configs that pass Ookla are not tested again. The default is `false`.
- `speed_test`: concurrency, overall `timeout`, `max_duration`, and minimum download/upload Mbps are shared by both providers and the fallback. The Cloudflare-specific `first_byte_timeout`, `sample_interval`, `download_bytes`, `upload_bytes`, `download_url`, and `upload_url` values are used when Cloudflare is the primary provider or the Ookla fallback is enabled; they are ignored when Ookla runs alone. The Cloudflare provider reports the fastest observed sample interval. The download URL may contain `{bytes}`; the upload URL must accept a raw HTTP `POST` body and return 2xx.
- `failed.txt` records provider-specific speed failures and fallback details. A node rescued by Cloudflare is also included in `speedtested*.txt`.
- `service_check`: concurrency, timeout, optional exit-IP lookup, automatic node renaming, GeoIP endpoint, name template (`{flag}`, `{country}`, `{download}`, `{upload}`), country-name display mode and Mbps decimal precision. `country_format` accepts `full` (default, for example `🇨🇳 China`), `code` (`🇨🇳 CN`) or `none` (flag only, `🇨🇳`). The setting applies to both URL-stage and final node names. Existing templates that contain `{flag}` but omit `{country}` automatically get the selected country label inserted after the flag, except in `none` mode. If GeoIP is unavailable, the node is still renamed using `🌐 Unknown`. GeoIP requests always go through the tested proxy and use short, bounded fallbacks.
- `output.gemini_file`: legacy compatibility setting. Separate Gemini files are no longer generated regardless of this value; Gemini-available nodes are marked inline with `Gemini 🐱` in every applicable result list.
- `loop.interval`: pause between complete cycles in continuous mode. Uses Go duration syntax, such as `"60s"` or `"5m"`. The default `"0s"` starts the next cycle immediately. The pause begins after the cycle (including any SFTP upload) finishes, and Ctrl+C interrupts the wait.

The default speed test uses 2 MiB download and 2 MiB upload payloads, a concurrency of 2, and minimums of 8 Mbps in both directions. **8 Mbps is approximately 1 MB/s.** Speed-test concurrency is deliberately lower than URL-test concurrency because simultaneous throughput tests share the machine's internet connection and can reduce measured rates. Set it to `1` for the least contention or increase it when throughput is ample.

Stage timeouts are independently configurable. CLI flags override the config file; for example:

```bash
./sw run --interface eno1
./sw run --url-concurrency 12 --speed-concurrency 1
./sw run --url 'https://www.gstatic.com/generate_204'
./sw run --download-url 'https://speed.cloudflare.com/__down?bytes={bytes}' --upload-url 'https://speed.cloudflare.com/__up'
./sw run --min-download 8 --min-upload 8 --speed-timeout 85s
./sw run --source-timeout 2m
```

For compatibility, an old `config.json` containing a `final_check` object is accepted, but that obsolete setting is ignored. The duplicate stage no longer exists.

## Output files

The output directory has a deliberately small, predictable set of files:

- `all.txt` — every parsed proxy URI from every source, before global deduplication and protocol filtering. Duplicates and protocols disabled for testing remain here. It is useful as a raw combined archive; the actual tests use a separate in-memory unique list.
- `url-tested.txt` — all nodes that passed the URL reachability check.
- `url-tested-<protocol>.txt` — the same URL-passed nodes split by protocol, for example `url-tested-vless.txt`.
- `speedtested.txt` — nodes that passed the complete configured speed pipeline. Cloudflare-rescued nodes are included.
- `speedtested-<protocol>.txt` — speed-passed nodes split by protocol, for example `speedtested-vless.txt`.
- `failed.txt` — one human-readable log of rejected config lines, source load and parser errors, URL/speed/service failures, Ookla and Cloudflare fallback results, rename errors and fatal program errors. This is diagnostic output, not a subscription list.

There are no separate failed-node lists, invalid.txt, speed-test CSV reports, Cloudflare-only files or Gemini-only files. Gemini availability is marked in node names with `Gemini 🐱`. Protocol-specific files appear only for protocols that have results in that stage.

The Gemini check classifies a public signed-out page response only. It does not authenticate or test account-specific eligibility. A proxy can still stop responding immediately after its one-time test, and the destination client may route traffic differently from SubWatch.


### Subscription download failures (0.3.8)

A timeout or HTTP/read error while downloading one subscription now marks only that source as skipped. SubWatch logs the source error and continues loading the rest of `sources.txt`. The run is still interrupted by an explicit Ctrl+C/SIGTERM, or by a fatal global error such as an unreadable source-list file.

## Remote SFTP upload

SubWatch can upload the `.txt` result files from `out/` to a remote directory after each completed cycle. Configure `remote_upload` in `config.json`:

```json
"remote_upload": {
  "enabled": true,
  "method": "sftp",
  "host": "79.72.16.241",
  "port": 22,
  "username": "ubuntu",
  "auth": "private_key",
  "private_key": "~/.ssh/id_ed25519",
  "password": "",
  "remote_directory": "/home/ubuntu/subwatch/out"
}
```

`enabled` is the only upload switch. There is no separate `upload_after_run`: when enabled, the upload happens after each completed test cycle. `auth` accepts `private_key` or `password`. Key authentication uses the local OpenSSH `ssh`/`sftp` commands. For password authentication, install `sshpass` on the machine running SubWatch and set `password` in the config; protect `config.json` because it contains a secret. SSH key authentication is recommended.

On the first run, SubWatch asks whether to repeat complete test cycles continuously. Answer `y` for continuous mode or press Enter / answer `n` for one cycle. In continuous mode, SFTP upload runs after every completed cycle, then SubWatch waits for `loop.interval` before starting the next one. Set it to `0s` to disable the pause. Ctrl+C stops the loop cleanly, including while it is waiting.
