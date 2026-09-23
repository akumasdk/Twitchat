# Kotlin Twitch IRC client (tmi.js parity example)

This standalone Kotlin/JVM example re-implements the Twitch IRC pieces usually handled by `tmi.js`:

- TLS IRC connection and reconnect loop
- `PASS`, `NICK`, and `CAP REQ` handshake (`tags`, `commands`, `membership`)
- `JOIN`/`PART`
- IRC line parser with tag decoding
- typed events (`PRIVMSG`, `USERNOTICE`, `CLEARCHAT`, `CLEARMSG`, `JOIN`, `PART`) and raw fallback
- send helpers (message, reply via `reply-parent-msg-id`, timeout, ban, delete)
- token refresh via reconnect

## Run

```bash
cd /home/runner/work/Twitchat/Twitchat/examples/kotlin/twitch-irc-client
TWITCH_LOGIN=your_bot_login \
TWITCH_OAUTH=your_token_without_oauth_prefix \
TWITCH_CHANNELS=channel_one,channel_two \
gradle run
```

You can use `TWITCH_OAUTH=oauth:xxxxx` too; the prefix is normalized.

## Validate parity with tmi.js

1. Run this Kotlin client and a known-good `tmi.js` session on the same channel.
2. Compare raw tags and mapped fields for:
   - GIF payloads (`gifs` tag, `url=...` extraction)
   - replies (`reply-parent-msg-id`)
   - emotes (`emotes`), badges (`badges`), and standard identity tags (`login`, `user-id`)
3. Confirm both clients emit matching semantics for `PRIVMSG`, `USERNOTICE`, `CLEARCHAT`, and `CLEARMSG`.
