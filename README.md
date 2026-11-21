# SubWatch 0.3.9

Linux-first Go CLI for parsing and deduplicating VLESS/VMess/Trojan/Shadowsocks/Hysteria2/TUIC/SOCKS/HTTP subscription links, then testing them through sing-box.

## Build from source

```bash
tar -xzf subwatch-source-0.3.8.tar.gz -C subwatch-source
cd subwatch-source
go test ./...
go vet ./...
go build -trimpath -ldflags='-s -w' -o sw ./cmd/subwatch
```

## Linux interface isolation

When `network.interface` is set to a specific physical interface, SubWatch creates a temporary Linux network namespace, obtains a separate DHCP lease on a macvlan attached to that interface, and connects the local sing-box listener back to the host over a private veth link. It does not rewrite host routes, policy rules, firewall rules, or the host resolver. The namespace and temporary links are removed when the run ends. Install `iproute2`, `sudo`, `util-linux` (`setpriv`), and `isc-dhcp-client` (`dhclient`) first. A network or Wi-Fi adapter that blocks additional MAC addresses cannot use this mode; SubWatch reports setup failure rather than silently falling back to the host VPN.\n\n## First run

```bash
chmod +x sw
./sw run
```

When `config.json` does not exist, `sw run` creates it automatically with editable defaults. A starter `sources.txt` is also created if it is missing. Put subscription URLs, local file paths, or individual share links into `sources.txt`, one per line. Existing files are never overwritten. `sw init` remains available for explicit initialization.

## Processing pipeline — one pass per stage

1. Parse every source and write `all.txt` (all parsed proxy URIs, including duplicates).
2. Deduplicate in memory and apply the enabled-protocol filter. These are internal working sets and are not exported.
3. URL-test every selected unique node. As soon as a node passes, SubWatch atomically updates `url-test-finished.txt` and the corresponding protocol file (`vless.txt`, `ss.txt`, `hysteria2.txt`, etc.). Protocol files therefore contain only URL-test-passed nodes and grow during the URL-test stage; they are never prefilled with untested nodes.
4. After the complete URL-test barrier, speed-test only URL successes. Both download and upload must meet their configured minimums. Write `speedtest-finished.txt`.
5. After the complete speed-test barrier, check Gemini only for speed-test successes. Write `gemini-available.txt` with only nodes classified as `available`.
6. At the end of the run, print each source subscription and how many unique URL-passed nodes it contributed, sorted from highest count to lowest. If a passing node occurs in multiple sources, it is credited to each of those sources while being tested only once globally.

All output lists are plain-text URI subscriptions, one URI per line. Failed-stage lists, temporary `selected`/`unique` lists, duplicate `tested` lists, service diagnostics and CSV reports are not exported. The output directory is cleaned of SubWatch-generated artifacts at the start of each run so stale files from older versions do not remain. Each invocation still performs one pass only; continuous/infinite cycling is not enabled.

## Live terminal status

Each testing stage uses one replaceable terminal status line instead of printing a separate line for every proxy. The status is fitted to the current terminal width; the config name and stage details share the available space, while the `ping: N ms` field is kept at the far right and always remains the final field. Long details are shortened to prevent wrapping, and ping is shown as `— ms` until the URL test succeeds. It shows the current config name and protocol. During URL-test it displays the measured ping and pass/fail result; during speed-test it displays ping plus live download/upload throughput and byte progress; during Gemini check it keeps ping and speed figures visible and adds the Gemini status and region when available. With parallel workers, the display follows the most recently started config; completion of an older parallel task cannot replace that current display. The test counters remain on the same line.

## Settings (`config.json`)

See [`config.example.json`](./config.example.json). All settings are optional; defaults are used for omitted values.

- `network.interface`: `auto` uses normal system routing. On Linux, selecting a physical interface (for example `eno1`) creates an isolated network namespace with a separate macvlan address obtained by DHCP; node traffic leaves through that interface without using the host VPN routing rules. This requires `iproute2`, `sudo`, `setpriv` (util-linux), and an installed DHCP client (`dhclient`). The first run asks for administrator authorization. The network must permit a second MAC address; some Wi-Fi adapters/access points do not. The host network configuration and DNS are not changed. Set `network.isolate_source_downloads` to `true` to route remote HTTP(S) subscription downloads through this namespace as well; this requires a specific interface. The source list itself and parsing remain local.
- `network.isolate_source_downloads`: set to `true` to download HTTP(S) subscriptions from `sources.file` inside the same isolated namespace and through the selected `network.interface`. This requires a specific interface (not `auto`) and Linux. The default is `false`, preserving the previous host-network behavior. Reading `sources.file`, reading local source files, and parsing downloaded text still happen in the main process; only remote HTTP(S) downloads are isolated. The first-run sing-box binary download and optional SFTP upload remain outside this setting.
- `sources.file`: source list path. Relative paths are resolved relative to the config file.
- `protocols.enabled`: protocols to test (`vless`, `vmess`, `trojan`, `ss`, `hysteria2`, `tuic`, `socks`, `http`). Aliases `shadowsocks`, `hy2`, and `socks5` are accepted.
- `url_test`: URL used for the reachability check, concurrency, and timeout.
- `speed_test`: separate concurrency and overall timeout, first-byte timeout, maximum measurement duration after the first byte (per direction), sampling interval, independent download/upload Mbps thresholds, payload sizes, and custom HTTP URLs. The reported speed is the **fastest observed sample interval**, not the average across the full transfer. The download URL may contain `{bytes}`; the upload URL must accept a raw HTTP `POST` body and return 2xx.
- `service_check`: concurrency, timeout, optional exit-IP lookup, automatic node renaming, GeoIP endpoint, name template (`{flag}`, `{download}`, `{upload}`), and Mbps decimal precision. By default, speed-passed nodes are renamed in their exported URI remark, for example `🇩🇪 70↓34↑`; GeoIP is requested through the tested proxy.
- `output`: output directory. The five result groups are always written as URI-only `.txt` files; protocol-specific working lists are always enabled.

The default speed test uses 2 MiB download and 2 MiB upload payloads, a concurrency of 2, and minimums of 8 Mbps in both directions. **8 Mbps is approximately 1 MB/s.** Speed-test concurrency is deliberately lower than URL-test concurrency because simultaneous throughput tests share the machine's internet connection and can reduce measured rates. Set it to `1` for the least contention or increase it when throughput is ample.

Stage timeouts are independently configurable. CLI flags override the config file; for example:

```bash
./sw run --interface eno1
./sw run --url-concurrency 12 --speed-concurrency 1
./sw run --url 'https://www.gstatic.com/generate_204'
./sw run --download-url 'https://speed.cloudflare.com/__down?bytes={bytes}' --upload-url 'https://speed.cloudflare.com/__up'
./sw run --min-download 8 --min-upload 8 --speed-timeout 85s
```

For compatibility, an old `config.json` containing a `final_check` object is accepted, but that obsolete setting is ignored. The duplicate stage no longer exists.

## Output files

The output directory contains only these result groups:

- `all.txt` — every parsed proxy URI from all sources (duplicates included).
- `vless.txt`, `vmess.txt`, `trojan.txt`, `ss.txt`, `hysteria2.txt`, `tuic.txt`, `socks.txt`, `http.txt` — only nodes that passed URL-test, grouped by protocol. A protocol file appears only when it has at least one passing node and is updated as URL tests finish.
- `url-test-finished.txt` — all URL-test-passed nodes.
- `speedtest-finished.txt` — nodes that passed both download and upload thresholds.
- `gemini-available.txt` — nodes classified as Gemini `available`.

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
