# packet-slapper
Packet Slapper runs an official Ookla speedtest directly from inside Dispatcharr, so it measures whatever network path Dispatcharr itself uses, VPN included, without needing a separate container or docker socket.

## Which version to use

| Your Dispatcharr | Packet Slapper |
| --- | --- |
| 0.34.0 or newer | 2.x |
| older than 0.34.0 | 1.3.0, the last version for older Dispatcharr |

2.x uses the plugin features Dispatcharr added in 0.34: the scheduler runs as a Dispatcharr-managed background service, **Run speedtest now** shows live progress and keeps a run history, settings are grouped into sections, and the plugin's files live in Dispatcharr's own data folder for plugins. The plugin hub lists which Dispatcharr versions each release supports. 1.3.0 is the final release for Dispatcharr older than 0.34: it stays installable but won't get further updates.

After updating from 1.x, restart Dispatcharr once, so no 1.x scheduler is left running in the background.

When you first enable 2.x (or update to it from 1.x), Dispatcharr asks you to approve what the plugin needs:

- **Run a persistent background service:** the scheduler.
- **Run background tasks:** Run speedtest now runs on Dispatcharr's task queue.
- **Run host processes:** the Ookla Speedtest CLI.
- **Access external network services:** downloading the CLI from install.speedtest.net, the speedtest itself, and Discord posts.
- **Use live proxy internals:** reading Dispatcharr's live, catch-up and VOD viewer counts in Redis, so scheduled tests can skip while someone is watching. Packet Slapper only reads these; it never changes them.

Run it on demand or on a schedule. Click **Run speedtest now** for a one-off test: Dispatcharr shows its progress, and about 30 seconds later the result pops up (yellow if an alert tripped, red if the test failed). It also goes to Discord, the log, Status and the plugin's run history, where each result can be downloaded as JSON. Or turn on the scheduler, either every N minutes or hours, or **At set times** such as `03:00, 15:00`. Scheduled tests show up in the same run history, marked as scheduled. They skip themselves automatically if anyone's watching live TV, VOD, or catch-up, or while a DVR recording is running or about to start (within 3 minutes), since a speedtest eats real bandwidth.

Results show right in Dispatcharr, no external service required. You get download, upload, latency, jitter, loaded latency (ping while the line is busy, which shows bufferbloat), packet loss, which server it hit, a link to the result on speedtest.net, and the exit IP the test went out through, so you can see at a glance whether the VPN was in use. If the exit IP changes from one test to the next (usually because the VPN switched servers), the result says so, which often explains a sudden change in the numbers. Add a Discord webhook if you want, and every result also posts there, either as a plain text message or a colored embed card. The exit IP is left out of Discord posts unless you turn on **Include exit IP in Discord posts**.

A few extras, too. You can pin tests to one Ookla server so results stay comparable over time: click **Find servers**, and **Speedtest server** becomes a list of the nearest servers to pick from (click it again after changing VPN location), pick your display timezone from a dropdown, and use Check active streams or Status any time to see what the plugin is doing right now and when the next run is due. Run speedtest now always runs, but if someone was watching at the time, the result says so.

The Ookla Speedtest CLI is downloaded from install.speedtest.net in the background shortly after Dispatcharr starts, so Dispatcharr needs to be able to reach that host. The plugin checks for a newer CLI at most once a day and test-starts it before switching over. Turn off **Keep the Speedtest CLI up to date** to stay on the version you have. The CLI is kept in the data folder Dispatcharr gives the plugin (normally `/data/plugins/_data/packet-slapper`), so it survives plugin updates, and when you uninstall, Dispatcharr asks whether to keep or delete it. Updating from 1.x copies the CLI over from 1.x's hidden `/data/plugins/.packet_slapper` folder, so nothing is downloaded again. 2.x doesn't use that old folder; you can delete it once you're on 2.x for good. The plugin accepts Ookla's license and GDPR terms on your behalf when it runs the CLI, so Ookla's terms and privacy policy apply to every test, and results are sent to Ookla as with any speedtest.

Packet Slapper can also tell you when something's wrong. Every test reports whether it went through a VPN tunnel (like Gluetun's `tun0` or `wg0`). With **Alert: VPN check** on **Auto** (the default), once your tests have used a VPN, a test that doesn't gets a "VPN off" alert. There's nothing to set up. This works when Dispatcharr runs on a VPN container's network (like Gluetun with `network_mode: service:gluetun`) or uses host networking on a machine running the VPN. A VPN on your router can't be seen from inside Dispatcharr, so with a router VPN, or no VPN at all, this check simply never alerts. If you stop using a VPN on purpose, set the check to **Off**; that also makes Auto forget, so switching back to Auto later starts fresh. You can also set limits for download, upload, packet loss and loaded ping. Alerts show in the popup and in Status, and are highlighted in Discord. Set **Discord posts** to **Only alerts and recoveries** to hear from the plugin only when an alert starts, changes or clears.

If you scrape Dispatcharr with Prometheus, Packet Slapper can hand each result to the [Dispatcharr Exporter](https://github.com/swvn-dispatch/dispatcharr-exporter) plugin, which serves it as `dispatcharr_speedtest_*` metrics. It takes two switches:

1. In Packet Slapper, turn on **Publish results for Prometheus**.
2. In the Dispatcharr Exporter, turn on its Packet Slapper integration (**[Integration: Packet Slapper] Include Statistics**). This needs Dispatcharr Exporter 3.2.0 or later; the loaded latency and skip metrics need 3.3.0 or later.

Packet Slapper saves the latest result in Redis under `packet_slapper:prometheus:latest`:

```json
{
  "last_attempt_timestamp_seconds": 1761350400.12,
  "last_attempt_success": 1,
  "last_success_timestamp_seconds": 1761350400.12,
  "result": {"download_mbps": 912.4, "upload_mbps": 41.8, "latency_ms": 9.1, "jitter_ms": 0.7, "packet_loss": 0.0, "isp": "...", "server_name": "...", "...": "..."}
}
```

A failed test only updates the two `last_attempt_*` fields, so the last good numbers stay put and you can alert on `last_attempt_success == 0` or on `last_success_timestamp_seconds` getting old. `packet_loss` is `null` when the Ookla server doesn't report it, and the exporter leaves that metric out. Runs skipped because someone is watching don't count as attempts, so `last_success_timestamp_seconds` can age during long viewing sessions. Each skip sets `last_skip_timestamp_seconds`, so a staleness alert can stay quiet while recent skips explain the gap. Dispatcharr Exporter 3.3.0 and later serve it as `dispatcharr_speedtest_last_skip_timestamp_seconds`, and loaded latency as `dispatcharr_speedtest_download_latency_ms` and `dispatcharr_speedtest_upload_latency_ms`.

## Development

```sh
pip install pytest ruff
ruff check --select F,E9,B plugin.py tests
pytest -q tests
```

The tests use an in-memory stand-in for Redis and a fake speedtest, so they need neither Dispatcharr nor network access. GitHub Actions runs them on every push and pull request.
