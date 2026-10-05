# Changelog

## 1.0.1

Bug fixes from a read-through of 1.0.0.

- Packet loss shows N/A when the Ookla server doesn't report it. It used to show 0%, which isn't the same thing.
- Discord embed fields are inline now, so the card is two rows of three instead of one tall stack. That was the original layout, it just never got switched on.
- Plain-text Discord error messages are cut at 1900 characters. A long error could go over Discord's 2000 limit and get rejected.
- Server ID only accepts 0-9. `str.isdigit()` lets things like superscript digits through.
- The speedtest CLI gets re-downloaded when `SPEEDTEST_CLI_VERSION` changes. The installed version is saved in `bin/speedtest.version`. Upgrading from 1.0.0 does one fresh download since there's no version file yet.
- The CLI is written to a temp file in `bin/` and swapped in with `os.replace`, so a download that dies partway can't leave a broken `speedtest` that passes the executable check forever.
- If the settings can't be read from the DB and nobody has clicked a button yet, the scheduler waits and tries again instead of assuming it's enabled. `stop()` also clears the cached settings.
- A scheduled run that finds another test already running retries after 60 seconds. Before, it waited a full interval, which on a 24 hour schedule means a lost day.
- Removed the Tips entry from `actions` in plugin.json. It isn't an action, so it showed up as a button that did nothing but return "Unknown action". The Scheduler Status and Run Now descriptions already say the same thing.

## 1.0.0

Initial release.
