import birl
import gleam/bytes_tree
import gleam/dynamic/decode
import gleam/erlang/process.{type Subject}
import gleam/http/request.{type Request}
import gleam/http/response
import gleam/int
import gleam/json
import gleam/list
import gleam/option.{type Option, None}
import gleam/otp/actor
import gleam/string
import gleam/string_tree
import logging
import mist.{type Connection, type SSEConnection}
import stratus

// --- Bluesky Post types ------------------------------------------------------

pub type Post {
  Post(did: String, time_us: Int, kind: String, commit: Option(Commit))
}

pub type Commit {
  Commit(
    rev: String,
    operation: String,
    collection: String,
    rkey: String,
    record: Option(PostRecord),
    cid: Option(String),
  )
}

pub type PostRecord {
  PostRecord(
    record_type: String,
    created_at: String,
    langs: List(String),
    text: String,
  )
}

// --- JSON decoders -----------------------------------------------------------

pub fn post_record_decoder() -> decode.Decoder(PostRecord) {
  use record_type <- decode.field("$type", decode.string)
  use created_at <- decode.field("createdAt", decode.string)
  use langs <- decode.optional_field("langs", [], decode.list(decode.string))
  use text <- decode.field("text", decode.string)
  decode.success(PostRecord(record_type, created_at, langs, text))
}

pub fn commit_decoder() -> decode.Decoder(Commit) {
  use rev <- decode.field("rev", decode.string)
  use operation <- decode.field("operation", decode.string)
  use collection <- decode.field("collection", decode.string)
  use rkey <- decode.field("rkey", decode.string)
  use record <- decode.optional_field(
    "record",
    None,
    decode.optional(post_record_decoder()),
  )
  use cid <- decode.optional_field("cid", None, decode.optional(decode.string))
  decode.success(Commit(rev, operation, collection, rkey, record, cid))
}

pub fn post_decoder() -> decode.Decoder(Post) {
  use did <- decode.field("did", decode.string)
  use time_us <- decode.field("time_us", decode.int)
  use kind <- decode.field("kind", decode.string)
  use commit <- decode.optional_field(
    "commit",
    None,
    decode.optional(commit_decoder()),
  )
  decode.success(Post(did, time_us, kind, commit))
}

/// Extract the text content from a post, if present.
pub fn post_text(post: Post) -> String {
  post.commit
  |> option.then(fn(commit) { commit.record })
  |> option.map(fn(record) { record.text })
  |> option.unwrap("")
}

// --- Hub actor ---------------------------------------------------------------
// Receives posts from the firehose and broadcasts them to SSE subscribers.

pub type HubMsg {
  Subscribe(Subject(Post))
  Unsubscribe(Subject(Post))
  NewPost(Post)
  GetStats(Subject(HubStats))
}

pub type HubStats {
  HubStats(count: Int, started_at: String)
}

type HubState {
  HubState(subscribers: List(Subject(Post)), count: Int, started_at: String)
}

pub fn start_hub() -> Subject(HubMsg) {
  let started_at = birl.to_iso8601(birl.now())
  let assert Ok(actor) =
    actor.new(HubState(subscribers: [], count: 0, started_at: started_at))
    |> actor.on_message(handle_hub_message)
    |> actor.start
  actor.data
}

fn handle_hub_message(state: HubState, msg: HubMsg) {
  case msg {
    Subscribe(sub) ->
      actor.continue(HubState(..state, subscribers: [sub, ..state.subscribers]))
    Unsubscribe(sub) -> {
      let filtered = list.filter(state.subscribers, fn(s) { s != sub })
      actor.continue(HubState(..state, subscribers: filtered))
    }
    NewPost(post) -> {
      list.each(state.subscribers, fn(sub) { process.send(sub, post) })
      actor.continue(HubState(..state, count: state.count + 1))
    }
    GetStats(reply) -> {
      process.send(
        reply,
        HubStats(count: state.count, started_at: state.started_at),
      )
      actor.continue(state)
    }
  }
}

/// Query the hub's current post count.
fn get_hub_count(hub: Subject(HubMsg)) -> Int {
  let subj = process.new_subject()
  process.send(hub, GetStats(subj))
  case process.receive(subj, 1000) {
    Ok(stats) -> stats.count
    Error(_) -> 0
  }
}

// --- Firehose ----------------------------------------------------------------
// Connects to Bluesky's Jetstream WebSocket and forwards posts to the hub.

const firehose_url = "https://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"

/// Connect to the firehose, retrying until data actually flows.
///
/// Stratus v2 has a race condition where the WebSocket handshake succeeds
/// but the actor never receives data — it sets {active, once} on the socket
/// before transferring ownership to the actor process, so on a high-throughput
/// stream the first message often goes to the wrong process and the socket
/// goes permanently silent.
///
/// We work around this by verifying posts arrive within a few seconds.
/// This also gives us resilience against network drops.
fn start_firehose(hub: Subject(HubMsg)) -> Nil {
  let count_before = get_hub_count(hub)

  logging.log(logging.Info, "Firehose: connecting...")
  let assert Ok(req) = request.to(firehose_url)

  let connected =
    stratus.new(request: req, state: 0)
    |> stratus.on_message(fn(count, msg, _conn) {
      case msg {
        stratus.Text(text) -> handle_firehose_text(hub, count, text)
        stratus.Binary(_) -> stratus.continue(count)
        stratus.User(_) -> stratus.continue(count)
      }
    })
    |> stratus.on_close(fn(_state, _reason) {
      logging.log(logging.Warning, "Firehose: WebSocket closed")
    })
    |> stratus.start

  case connected {
    Error(_) -> {
      logging.log(logging.Warning, "Firehose: failed to connect, retrying...")
      process.sleep(1000)
      start_firehose(hub)
    }
    Ok(_) -> {
      // Verify data is actually flowing — if not, retry
      process.sleep(2000)
      case get_hub_count(hub) > count_before {
        True -> logging.log(logging.Info, "Firehose: connected")
        False -> {
          logging.log(
            logging.Warning,
            "Firehose: connection stalled, retrying...",
          )
          start_firehose(hub)
        }
      }
    }
  }
}

fn handle_firehose_text(hub, count, text) {
  case json.parse(text, post_decoder()) {
    Ok(post) -> {
      process.send(hub, NewPost(post))
      let new_count = count + 1
      case new_count {
        1 -> logging.log(logging.Info, "Firehose: receiving posts")
        n if n % 5000 == 0 ->
          logging.log(
            logging.Info,
            "Firehose: processed " <> int.to_string(n) <> " posts",
          )
        _ -> Nil
      }
      stratus.continue(new_count)
    }
    Error(err) -> {
      case count {
        0 ->
          logging.log(
            logging.Warning,
            "Firehose: decode error on first message: " <> string.inspect(err),
          )
        _ -> Nil
      }
      stratus.continue(count)
    }
  }
}

// --- SSE handler -------------------------------------------------------------
// Each connected client gets its own actor that subscribes to the hub.

type SSEMsg {
  PostReceived(Post)
}

type SSEState {
  SSEState(count: Int, subscription: Subject(Post), hub: Subject(HubMsg))
}

fn handle_sse(
  req: Request(Connection),
  hub: Subject(HubMsg),
) -> response.Response(mist.ResponseData) {
  mist.server_sent_events(
    request: req,
    initial_response: response.new(200),
    init: fn(_) {
      let post_subject = process.new_subject()
      process.send(hub, Subscribe(post_subject))

      // Map incoming Post messages to SSEMsg for the actor's selector
      let selector =
        process.new_selector()
        |> process.select_map(post_subject, PostReceived)

      Ok(
        actor.initialised(SSEState(
          count: 0,
          subscription: post_subject,
          hub: hub,
        ))
        |> actor.selecting(selector),
      )
    },
    loop: fn(state: SSEState, msg: SSEMsg, conn: SSEConnection) {
      case msg {
        PostReceived(post) -> {
          let new_count = state.count + 1

          let event_data =
            json.object([
              #("did", json.string(post.did)),
              #("kind", json.string(post.kind)),
              #("time_us", json.int(post.time_us)),
              #("text", json.string(post_text(post))),
              #("count", json.int(new_count)),
            ])
            |> json.to_string

          let event =
            mist.event(string_tree.from_string(event_data))
            |> mist.event_name("post")

          case mist.send_event(conn, event) {
            Ok(_) -> actor.continue(SSEState(..state, count: new_count))
            Error(_) -> {
              process.send(state.hub, Unsubscribe(state.subscription))
              logging.log(logging.Info, "SSE client disconnected")
              actor.stop()
            }
          }
        }
      }
    },
  )
}

// --- HTTP server -------------------------------------------------------------

pub fn main() {
  logging.configure()
  logging.set_level(logging.Info)

  let hub = start_hub()
  logging.log(logging.Info, "Hub started")

  start_firehose(hub)

  let assert Ok(_) =
    mist.new(fn(req) { handle_request(req, hub) })
    |> mist.port(8000)
    |> mist.start

  logging.log(logging.Info, "Listening on http://localhost:8000")
  logging.log(logging.Info, "  GET /          - server info")
  logging.log(logging.Info, "  GET /firehose  - SSE stream of posts")

  process.sleep_forever()
}

fn handle_request(
  req: Request(Connection),
  hub: Subject(HubMsg),
) -> response.Response(mist.ResponseData) {
  case req.path {
    "/firehose" -> handle_sse(req, hub)
    _ -> {
      let stats_subject = process.new_subject()
      process.send(hub, GetStats(stats_subject))
      let stats = case process.receive(stats_subject, 1000) {
        Ok(s) -> s
        Error(_) -> HubStats(count: 0, started_at: "unknown")
      }

      let body =
        "Bluesky Firehose SSE Server\n\nSource: "
        <> firehose_url
        <> "\nStarted: "
        <> stats.started_at
        <> "\nProcessed: "
        <> int.to_string(stats.count)
        <> " posts\n\nGET /firehose - SSE stream of posts\n\ncurl -N http://localhost:8000/firehose"

      response.new(200)
      |> response.set_header("content-type", "text/plain")
      |> response.set_body(mist.Bytes(bytes_tree.from_string(body)))
    }
  }
}
