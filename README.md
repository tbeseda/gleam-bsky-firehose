# bsky_firehose

A Gleam server that connects to the Bluesky firehose via WebSocket and re-broadcasts posts as Server-Sent Events (SSE).

Connects to [Jetstream](https://docs.bsky.app/blog/jetstream), filters for `app.bsky.feed.post` events, and fans them out to any number of SSE clients through a hub actor.

## Key dependencies

- **mist** - HTTP server with SSE support
- **stratus** - WebSocket client for the Jetstream firehose connection
- **gleam_otp** - OTP actors for the hub that manages subscriber fan-out
- **gleam_json** - Decoding firehose messages
- **birl** - Timestamps

## Usage

```sh
gleam run   # Starts the server on port 8000
gleam test  # Run tests
```

### Endpoints

- `GET /` - Server info and post count
- `GET /firehose` - SSE stream of Bluesky posts

```sh
curl -N http://localhost:8000/firehose
```
