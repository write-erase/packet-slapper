# packet-slapper
Packet Slapper runs an official Ookla speedtest directly from inside Dispatcharr, so it measures whatever network path Dispatcharr itself uses, VPN included, without needing a separate container or docker socket.

Run it on demand or on a schedule. Click Run Now for a one-off test, or turn on the scheduler. Scheduled runs skip themselves automatically if anyone's watching live TV, VOD, or catch-up, since a speedtest eats real bandwidth.

Results show right in Dispatcharr, no external service required. You get download, upload, latency, jitter, packet loss, and which server it hit. Add a Discord webhook if you want, and every result also posts there, either as a plain text message or a colored embed card.

A few extras, too. You can pin tests to one specific Ookla server ID so results stay comparable over time, pick your display timezone from a dropdown, and use Check Active Streams or Scheduler Status any time to see what the plugin is doing right now and when the next run is due.

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

A failed test only updates the two `last_attempt_*` fields, so the last good numbers stay put and you can alert on `last_attempt_success == 0` or on `last_success_timestamp_seconds` getting old. `packet_loss` is `null` when the Ookla server doesn't report it, and the exporter leaves that metric out. Runs skipped because someone is watching aren't published, so `last_success_timestamp_seconds` can age during long viewing sessions. Allow for that in staleness alerts.
