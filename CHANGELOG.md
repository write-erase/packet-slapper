# Changelog

## 1.1.0

- New "Publish results for Prometheus" setting (off by default). When it's on, every speedtest is saved to the Redis key `packet_slapper:prometheus:latest`, which the Dispatcharr Exporter plugin serves as `dispatcharr_speedtest_*` metrics. This requires an exporter release that includes the Packet Slapper integration, and its Packet Slapper setting turned on there too. Scheduled and Run Now tests both publish. A failed test records the failed attempt but keeps the last good result. Runs skipped because someone is watching are not published. Discord posting is unchanged and works alongside it.
- The write is read back to confirm it landed and retried up to three times. If it still fails, the popup says "Prometheus metrics update FAILED" and Status shows the run with a warning icon. The test is not re-run early because of this, so a Redis outage can't turn into a speedtest every minute.
- A result that can't be stored as valid JSON now fails immediately instead of being retried.

## 1.0.4

Small cleanups and hardening.

- The plugin now keeps the Ookla speedtest CLI up to date by itself. On the first speedtest after Dispatcharr starts, it checks Ookla's download server for a newer release and installs it, so a restart is all it takes. A new release is test-started before it replaces the working one, and if the check or download fails the installed CLI keeps being used. `SPEEDTEST_CLI_VERSION` is gone; `SPEEDTEST_CLI_BASE_VERSION` is only the starting point for a first install.
- The default timezone is now UTC instead of US Central. Installs that already saved a timezone keep it; installs that never touched the setting will now see UTC.
- The Discord webhook response is now closed after each post instead of being left for garbage collection.
- The "can't read saved settings" warning now says what it actually knows (no saved settings found, or the database is unreachable), and it can fire again if the problem comes back.
- Removed an unused `stat` import.
- A scheduled run that is skipped because someone is watching now retries after 30 minutes instead of waiting a full interval (never longer than the interval itself).
- Skipped runs no longer replace the last real result in Status. Status shows "Last run" for the last actual test, plus a separate note such as "Skipped 3 times in a row because someone was watching". After three skips in a row it suggests the Check Active Streams button, in case something is being detected that shouldn't be. A real run resets the count.
- Status now shows which speedtest CLI version is installed.
- The "check that Dispatcharr can reach install.speedtest.net" hint now appears only on download failures, not when a new CLI fails its startup test.
- The settings the background scheduler falls back on are now swapped in one step instead of cleared and refilled, which removes a very small window where the scheduler could see them empty.

## 1.0.3

- Fixed an issue where the plugin in Dispatcharr still showed version 1.0.1.

## 1.0.2

- Changed to a new logo.

## 1.0.1

Bug fixes from a read-through of 1.0.0.

- Packet loss shows N/A when the Ookla server doesn't report it. It used to show 0%, which isn't the same thing.
- Discord embed fields are inline now, so the card is two rows of three instead of one tall stack. That was the original layout, it just never got switched on.
- Plain-text Discord error messages are cut at 1900 characters. A long error could go over Discord's 2000 limit and get rejected.
- Server ID only accepts 0-9. `str.isdigit()` lets things like superscript digits through.
- The speedtest CLI gets re-downloaded when `SPEEDTEST_CLI_VERSION` changes. The installed version is saved in `bin/speedtest.version`. Upgrading from 1.0.0 does one fresh download since there's no version file yet.
- The CLI is written to a temp file in `bin/` and swapped in with `os.replace`, so a download that dies partway can't leave a broken `speedtest` that passes the executable check forever.
- The downloaded CLI is created with mode 0700 instead of being chmod'd to 0755 afterwards, so it isn't world-readable or executable. This was the CodeQL `py/overly-permissive-file` alert.
- The speedtest CLI is read out of the tarball with `extractfile` instead of `extract`, and anything that isn't a regular file named `speedtest` is rejected. This was the CodeQL tarfile extraction alert, and it also drops the fallback for Pythons without extraction filters.
- If the settings can't be read from the DB and nobody has clicked a button yet, the scheduler waits and tries again instead of assuming it's enabled. `stop()` also clears the cached settings.
- A scheduled run that finds another test already running retries after 60 seconds. Before, it waited a full interval, which on a 24 hour schedule means a lost day.
- Removed the Tips entry from `actions` in plugin.json. It isn't an action, so it showed up as a button that did nothing but return "Unknown action". The Scheduler Status and Run Now descriptions already say the same thing.

## 1.0.0

Initial release.
