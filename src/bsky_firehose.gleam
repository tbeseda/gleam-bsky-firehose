import birl
import gleam/bytes_tree
import gleam/dynamic/decode
import gleam/erlang/process.{type Subject}
import gleam/http/request.{type Request}
import gleam/http/response
import gleam/int
import gleam/io
import gleam/json
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/string
import gleam/string_tree
import logging
import mist.{type Connection, type SSEConnection}
import stratus

// ============================================================================
// Bluesky Post Types
// ============================================================================

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

// ============================================================================
// JSON Decoders
// ============================================================================

fn post_record_decoder() -> decode.Decoder(PostRecord) {
  use record_type <- decode.field("$type", decode.string)
  use created_at <- decode.field("createdAt", decode.string)
  use langs <- decode.optional_field("langs", [], decode.list(decode.string))
  use text <- decode.field("text", decode.string)
  decode.success(PostRecord(record_type, created_at, langs, text))
}

fn commit_decoder() -> decode.Decoder(Commit) {
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

fn post_decoder() -> decode.Decoder(Post) {
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

// ============================================================================
// Hub - broadcasts posts to all SSE subscribers
// ============================================================================

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

fn start_hub() -> Subject(HubMsg) {
  let started_at = birl.to_iso8601(birl.now())
  let assert Ok(actor) =
    actor.new(HubState(subscribers: [], count: 0, started_at: started_at))
    |> actor.on_message(fn(state: HubState, msg: HubMsg) {
      case msg {
        Subscribe(sub) ->
          actor.continue(
            HubState(..state, subscribers: [sub, ..state.subscribers]),
          )
        Unsubscribe(sub) -> {
          let filtered = do_filter(state.subscribers, sub, [])
          actor.continue(HubState(..state, subscribers: filtered))
        }
        NewPost(post) -> {
          do_broadcast(state.subscribers, post)
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
    })
    |> actor.start
  actor.data
}

// Helper: filter out a specific subject
fn do_filter(
  list: List(Subject(Post)),
  target: Subject(Post),
  acc: List(Subject(Post)),
) -> List(Subject(Post)) {
  case list {
    [] -> acc
    [first, ..rest] if first == target -> do_filter(rest, target, acc)
    [first, ..rest] -> do_filter(rest, target, [first, ..acc])
  }
}

// Helper: send post to all subscribers
fn do_broadcast(subscribers: List(Subject(Post)), post: Post) -> Nil {
  case subscribers {
    [] -> Nil
    [sub, ..rest] -> {
      process.send(sub, post)
      do_broadcast(rest, post)
    }
  }
}

// ============================================================================
// Firehose - connects to Bluesky and sends posts to hub
// ============================================================================

const firehose_url = "https://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"

fn start_firehose(hub: Subject(HubMsg)) -> Nil {
  let assert Ok(req) = request.to(firehose_url)

  let assert Ok(_) =
    stratus.new(request: req, state: 0)
    |> stratus.on_message(fn(count, msg, _conn) {
      case msg {
        stratus.Text(text) -> {
          case json.parse(text, post_decoder()) {
            Ok(post) -> {
              process.send(hub, NewPost(post))
              let new_count = count + 1
              case new_count % 500 {
                0 ->
                  io.println(
                    "Firehose: processed "
                    <> string.inspect(new_count)
                    <> " posts",
                  )
                _ -> Nil
              }
              stratus.continue(new_count)
            }
            Error(_) -> stratus.continue(count)
          }
        }
        stratus.Binary(_) -> stratus.continue(count)
        stratus.User(_) -> stratus.continue(count)
      }
    })
    |> stratus.on_close(fn(_state, _reason) {
      logging.log(logging.Warning, "Firehose WebSocket closed")
    })
    |> stratus.start

  logging.log(logging.Info, "Connected to Bluesky firehose")
  Nil
}

// ============================================================================
// SSE Handler
// ============================================================================

pub type SSEMsg {
  PostReceived(Post)
}

pub type SSEState {
  SSEState(count: Int, subscription: Subject(Post), hub: Subject(HubMsg))
}

fn handle_sse(
  req: Request(Connection),
  hub: Subject(HubMsg),
) -> response.Response(mist.ResponseData) {
  mist.server_sent_events(
    request: req,
    initial_response: response.new(200),
    init: fn(_sse_subject: Subject(SSEMsg)) {
      // Create a subject to receive posts - this will be selected by the actor
      let post_subject: Subject(Post) = process.new_subject()

      // Subscribe to the hub
      process.send(hub, Subscribe(post_subject))

      // Create selector that maps Post -> SSEMsg
      let selector =
        process.new_selector()
        |> process.select_map(post_subject, fn(post) { PostReceived(post) })

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

          // Extract text from post
          let text = case post.commit {
            Some(commit) ->
              case commit.record {
                Some(record) -> record.text
                None -> ""
              }
            None -> ""
          }

          // Build JSON event data
          let event_data =
            json.object([
              #("did", json.string(post.did)),
              #("kind", json.string(post.kind)),
              #("time_us", json.int(post.time_us)),
              #("text", json.string(text)),
              #("count", json.int(new_count)),
            ])
            |> json.to_string

          let event =
            mist.event(string_tree.from_string(event_data))
            |> mist.event_name("post")

          case mist.send_event(conn, event) {
            Ok(_) -> actor.continue(SSEState(..state, count: new_count))
            Error(_) -> {
              // Unsubscribe on disconnect
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

// ============================================================================
// Main
// ============================================================================

pub fn main() {
  logging.configure()
  logging.set_level(logging.Info)

  // Start the hub
  let hub = start_hub()
  logging.log(logging.Info, "Hub started")

  // Start the firehose connection
  start_firehose(hub)

  // Start HTTP server
  let assert Ok(_) =
    mist.new(fn(req) { handle_request(req, hub) })
    |> mist.port(8000)
    |> mist.start

  logging.log(logging.Info, "Server running on http://localhost:8000")
  io.println("\nEndpoints:")
  io.println("  GET /          - Info page")
  io.println("  GET /firehose  - SSE stream of Bluesky posts")
  io.println("\nTry: curl -N https://egg-water.exe.xyz:8000/firehose")

  process.sleep_forever()
}

fn handle_request(
  req: Request(Connection),
  hub: Subject(HubMsg),
) -> response.Response(mist.ResponseData) {
  case req.path {
    "/firehose" -> handle_sse(req, hub)
    _ -> {
      // Get stats from hub
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
        <> " posts\n\nGET /firehose - Stream of Bluesky posts\n\ncurl -N https://egg-water.exe.xyz:8000/firehose"
      response.new(200)
      |> response.set_header("content-type", "text/plain")
      |> response.set_body(mist.Bytes(bytes_tree.from_string(body)))
    }
  }
}
