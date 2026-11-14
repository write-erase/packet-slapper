# packet-slapper
Packet Slapper runs an official Ookla speedtest directly from inside Dispatcharr, so it measures whatever network path Dispatcharr itself uses, VPN included, without needing a separate container or docker socket.

Run it on demand or on a schedule. Click Run Now for a one-off test, or turn on the scheduler. Scheduled runs skip themselves automatically if anyone's watching live TV, VOD, or catch-up, since a speedtest eats real bandwidth.

Results show right in Dispatcharr, no external service required. You get download, upload, latency, jitter, loaded latency (ping while the line is busy, which shows bufferbloat), packet loss, which server it hit, a link to the result on speedtest.net, and the exit IP the test went out through, so you can see at a glance whether the VPN was in use. Add a Discord webhook if you want, and every result also posts there, either as a plain text message or a colored embed card. The exit IP is left out of Discord posts unless you turn on **Include exit IP in Discord posts**.

A few extras, too. You can pin tests to one specific Ookla server ID so results stay comparable over time (Find Servers lists the nearest IDs), pick your display timezone from a dropdown, and use Check Active Streams or Scheduler Status any time to see what the plugin is doing right now and when the next run is due. Run Now always runs, but if someone was watching at the time, the result says so.

The Ookla speedtest CLI is downloaded from install.speedtest.net in the background shortly after Dispatcharr starts, so Dispatcharr needs to be able to reach that host. The plugin checks for a newer CLI at most once a day and test-starts it before switching over. Turn off **Keep the Speedtest CLI up to date** to stay on the version you have. The CLI is kept in a hidden `.packet_slapper` folder next to the plugin's own folder (normally `/data/plugins/.packet_slapper`), so it survives plugin updates. Removing the plugin leaves that folder behind. It's only a few MB, but you can delete it by hand. The plugin accepts Ookla's license and GDPR terms on your behalf when it runs the CLI, so Ookla's terms and privacy policy apply to every test, and results are sent to Ookla as with any speedtest.

Packet Slapper can also tell you when something's wrong. Every test reports whether it went through a VPN tunnel (like Gluetun's `tun0` or `wg0`). With **Alert: VPN check** on **Auto** (the default), once your tests have used a VPN, a test that doesn't gets a "VPN off" alert. There's nothing to set up. This works when Dispatcharr runs on a VPN container's network (like Gluetun with `network_mode: service:gluetun`) or uses host networking on a machine running the VPN. A VPN on your router can't be seen from inside Dispatcharr, so with a router VPN, or no VPN at all, this check simply never alerts. If you stop using a VPN on purpose, set the check to **Off**; that also makes Auto forget, so switching back to Auto later starts fresh. You can also set limits for download, upload, packet loss and loaded ping. Alerts show in the Run Now popup and in Status, and are highlighted in Discord. Set **Discord posts** to **Only alerts and recoveries** to hear from the plugin only when an alert starts, changes or clears.

If you scrape Dispatcharr with Prometheus, Packet Slapper can hand each result to the [Dispatcharr Exporter](https://github.com/swvn-dispatch/dispatcharr-exporter) plugin, which serves it as `dispatcharr_speedtest_*` metrics. It takes two switches:

1. In Packet Slapper, turn on **Publish results for Prometheus**.
2. In the Dispatcharr Exporter, turn on its Packet Slapper integration (**[Integration: Packet Slapper] Include Statistics**). This needs an exporter release that includes the Packet Slapper integration.

Packet Slapper saves the latest result in Redis under `packet_slapper:prometheus:latest`:

```json
{
  "last_attempt_timestamp_seconds": 1761350400.12,
  "last_attempt_success": 1,
  "last_success_timestamp_seconds": 1761350400.12,
  "result": {"download_mbps": 912.4, "upload_mbps": 41.8, "latency_ms": 9.1, "jitter_ms": 0.7, "packet_loss": 0.0, "isp": "...", "server_name": "...", "...": "..."}
}
```

A failed test only updates the two `last_attempt_*` fields, so the last good numbers stay put and you can alert on `last_attempt_success == 0` or on `last_success_timestamp_seconds` getting old. `packet_loss` is `null` when the Ookla server doesn't report it, and the exporter leaves that metric out. Runs skipped because someone is watching don't count as attempts, so `last_success_timestamp_seconds` can age during long viewing sessions. Each skip sets `last_skip_timestamp_seconds`, so a staleness alert can stay quiet while recent skips explain the gap. The exporter only serves this as a metric once its Packet Slapper integration reads the field.

## Development

```sh
pip install pytest ruff
ruff check --select F,E9,B plugin.py tests
pytest -q tests
```

The tests use an in-memory stand-in for Redis and a fake speedtest, so they need neither Dispatcharr nor network access. GitHub Actions runs them on every push and pull request.
