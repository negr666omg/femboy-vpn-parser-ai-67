# SubWatch 0.3.9

Linux-first Go CLI for parsing and deduplicating VLESS/VMess/Trojan/Shadowsocks/Hysteria2/TUIC/SOCKS/HTTP subscription links, then testing them through sing-box.

## Build from source

```bash
git clone https://github.com/negr666omg/femboy-vpn-parser-ai-67.git
cd femboy-vpn-parser-ai-67
go test ./...
go vet ./...
go build -trimpath -ldflags='-s -w' -o sw ./cmd/subwatch
```

## Linux interface isolation

When `network.interface` is set to a specific physical interface, SubWatch creates a temporary Linux network namespace, obtains a separate DHCP lease on a macvlan attached to that interface, and connects the local sing-box listener back to the host over a private veth link. It does not rewrite host routes, policy rules, firewall rules, or the host resolver. The namespace and temporary links are removed when the run ends. Install `iproute2`, `sudo`, `util-linux` (`setpriv`), and `isc-dhcp-client` (`dhclient`) first. A network or Wi-Fi adapter that blocks additional MAC addresses cannot use this mode; SubWatch reports setup failure rather than silently falling back to the host VPN.

## First run

```bash
chmod +x sw
./sw run
```

When `config.json` does not exist, `sw run` creates it automatically with editable defaults. A starter `sources.txt` is also created if it is missing. Put subscription URLs, local file paths, or individual share links into `sources.txt`, one per line. Existing files are never overwritten. `sw init` remains available for explicit initialization.

## Processing pipeline — one pass per stage

1. Parse every source and write `all.txt` (all parsed proxy URIs, including duplicates).
2. Deduplicate in memory and apply the enabled-protocol filter. These are internal working sets and are not exported.
3. URL-test every selected unique node. As soon as a node passes, SubWatch atomically updates `url-tested.txt` and the matching protocol file (for example, `url-tested-vless.txt`). These files contain only URL-test-passed nodes and grow during the URL-test stage.
4. After the complete URL-test barrier, speed-test only URL successes. Both download and upload must meet their configured minimums. Write `speedtested.txt` and per-protocol files such as `speedtested-vless.txt`.
5. After the complete speed-test barrier, check Gemini only for speed-test successes. With `output.gemini_file: true`, write `gemini.txt` and matching protocol files such as `gemini-vless.txt`, containing only nodes classified as `available`. With the setting disabled, do not create separate Gemini files; instead append `Gemini 🐱` to each available node's remark in `all.txt`, `url-tested*.txt` and `speedtested*.txt`, including their protocol-specific lists.
6. At the end of the run, print a source-quality breakdown. Each source separates configs found only there from configs shared with other sources. URL, speed and Gemini pass rates are shown separately for those two groups; speed success is measured among URL-passed configs, and Gemini success among speed-passed configs. Sources are ranked by the number of source-exclusive URL-passed configs, so copied configs cannot inflate a source's ranking. The summary also reports global cross-source overlap. Config identity is based on normalized connection settings, not display names, so renamed copies still match. Source URLs are displayed without credentials, paths or query tokens.

Successful stage lists are plain-text subscriptions, one URI per line. `failed.txt` is a combined diagnostic report containing source-download/parse errors, rejected config lines, URL-test failures, speed-test failures, Gemini/service failures and node-renaming errors. Temporary `selected`/`unique` lists, separate failed-stage lists, duplicate `tested` lists and CSV reports are not exported. After the current source list has been loaded and parsed, the output directory is cleaned of known SubWatch-generated artifacts before new results are written. An early setup or source-list error therefore leaves the previous successful results intact. Each invocation still performs one pass only; continuous/infinite cycling is not enabled.

## Live terminal status

Each testing stage renders one independently updated terminal row per concurrent worker. With a concurrency of 6, up to six configs are visible and updating at the same time; a worker reuses its row for the next config it receives. A summary row shows total progress, active workers, passed and failed counts. Every worker row is fitted to the current terminal width; the config name and stage details share the available space, while the `ping: N ms` field is kept at the far right. Long details are shortened to prevent wrapping, and ping is shown as `— ms` until the URL test succeeds. After a successful URL test, the URI gets a country flag and English country name (or 🌐 Unknown if GeoIP cannot resolve it); after the speed stage, the final measured download/upload values are propagated back into all.txt and the URL-stage lists, including nodes that miss the configured speed thresholds. During URL-test each row displays its config, protocol, ping and result; during speed-test it displays that config's live download/upload throughput and byte progress; during Gemini check it keeps ping and speed figures visible and adds the Gemini status and region when available. When stdout is redirected instead of attached to a terminal, SubWatch retains the compact single-line progress output.

## Settings (`config.json`)

See [`config.example.json`](./config.example.json). All settings are optional; defaults are used for omitted values.

- `network.interface`: `auto` uses normal system routing. On Linux, selecting a physical interface (for example `eno1`) creates an isolated network namespace with a separate macvlan address obtained by DHCP; node traffic leaves through that interface without using the host VPN routing rules. This requires `iproute2`, `sudo`, `setpriv` (util-linux), and an installed DHCP client (`dhclient`). The first run asks for administrator authorization. The network must permit a second MAC address; some Wi-Fi adapters/access points do not. The host network configuration and DNS are not changed. Set `network.isolate_source_downloads` to `true` to route remote HTTP(S) subscription downloads through this namespace as well; this requires a specific interface. The source list itself and parsing remain local.
- `network.isolate_source_downloads`: set to `true` to download HTTP(S) subscriptions from `sources.file` inside the same isolated namespace and through the selected `network.interface`. This requires a specific interface (not `auto`) and Linux. The default is `false`, preserving the previous host-network behavior. Reading `sources.file`, reading local source files, and parsing downloaded text still happen in the main process; only remote HTTP(S) downloads are isolated. The first-run sing-box binary download and optional SFTP upload remain outside this setting.
- `sources.file`: source list path. Relative paths are resolved relative to the config file.
- `sources.timeout`: maximum time allowed for each remote HTTP(S) subscription download, including downloads through the isolated interface. Uses Go duration syntax such as `"30s"`, `"2m"`, or `"5m"`; defaults to `"30s"`. Local files and inline links are not affected.
- `protocols.enabled`: protocols to test (`vless`, `vmess`, `trojan`, `ss`, `hysteria2`, `tuic`, `socks`, `http`). Aliases `shadowsocks`, `hy2`, and `socks5` are accepted.
- `url_test`: URL used for the reachability check, concurrency, and timeout.
- `speed_test.provider`: speed-test backend, either `cloudflare` (default) or `ookla`. Ookla retrieves the public Speedtest.net server list through each tested node, probes up to 12 candidates in parallel through that same proxy, and selects the lowest-latency reachable server. Each direction runs for up to 8 seconds. The displayed live rate is calculated from bytes as they arrive; the final speed and pass/fail thresholds use the maximum rolling 2-second average, so individual request boundaries do not create artificial zero readings or make one failed connection invalidate the other workers. No server ID, API key, or separate Ookla CLI installation is needed. When `ookla` is selected, Cloudflare-only settings are automatically ignored; you do not need to delete or comment them out.
- `speed_test`: concurrency, overall `timeout`, `max_duration`, and minimum download/upload Mbps are shared by both providers. Ookla is capped at 8 seconds per direction (or the lower configured `max_duration`) to avoid unnecessarily long tests. `first_byte_timeout`, `sample_interval`, `download_bytes`, `upload_bytes`, `download_url`, and `upload_url` apply only to `cloudflare`; they are automatically disabled for `ookla`. The Cloudflare provider reports the fastest observed sample interval. The download URL may contain `{bytes}`; the upload URL must accept a raw HTTP `POST` body and return 2xx.
- `service_check`: concurrency, timeout, optional exit-IP lookup, automatic node renaming, GeoIP endpoint and name template (`{flag}`, `{country}`, `{download}`, `{upload}`). Speed values in node names are rounded to whole Mbps. The default name is `🇩🇪 Germany 70↓34↑`. Existing templates that contain `{flag}` but omit `{country}` automatically get the country name inserted after the flag. If GeoIP is unavailable, the node is still renamed using `🌐 Unknown`. GeoIP requests always go through the tested proxy and use short, bounded fallbacks.
- `output.directory`: output directory. Stage result files are always plain URI-only `.txt` files and are split by protocol, for example `url-tested-vless.txt`, `speedtested-vless.txt` and `gemini-vless.txt`.
- `output.gemini_file`: when `true` (default), create a separate `gemini.txt` plus `gemini-<protocol>.txt` lists. When `false`, do not create a Gemini-only file; instead rename Gemini-available nodes by appending `Gemini 🐱` to their remark and refresh all earlier files in which those nodes appear, including `all.txt` and both stage/protocol lists.

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

For compatibility, older config files may still contain `final_check`, `service_check.speed_decimals`, `output.split_by_protocol` or `output.subscription_text`; these obsolete settings are accepted but ignored. Speed labels are rounded to whole Mbps, and stage/protocol URI files are always written. The duplicate stage no longer exists.

## Output files

The output directory contains only these result groups:

- `all.txt` — every parsed proxy URI from all sources (duplicates included).
- `url-tested.txt` and `url-tested-<protocol>.txt` — all URL-test-passed nodes and their per-protocol subsets.
- `speedtested.txt` and `speedtested-<protocol>.txt` — nodes that passed both download and upload thresholds and their per-protocol subsets.
- `failed.txt` — a combined report of all recorded failures: source download/parse errors, malformed or unsupported config lines, URL-test failures, speed-test failures, Gemini/service failures and node-renaming errors. Each entry includes its stage, reason and URI when available.
- `gemini.txt` and `gemini-<protocol>.txt` — nodes classified as Gemini `available`, created when `output.gemini_file` is `true`. When it is `false`, Gemini availability is marked in the node name across all applicable earlier files instead.

`selected` means the internal list after deduplication and filtering by `protocols.enabled`; it does not mean URL-test-passed or working. It is an intermediate implementation detail and is no longer written to disk. Failed lists, `unique.txt`, `tested.txt`, `final.txt`, service-check dumps and `report.csv` are intentionally omitted to keep the output directory minimal.

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

On the first run, SubWatch asks whether to repeat complete test cycles continuously. Answer `y` for continuous mode or press Enter / answer `n` for one cycle. In continuous mode, SFTP upload runs after every completed cycle. Ctrl+C stops the loop cleanly.
