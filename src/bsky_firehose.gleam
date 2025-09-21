import gleam/dynamic/decode
import gleam/erlang/process
import gleam/function
import gleam/http/request
import gleam/int
import gleam/io
import gleam/json
import gleam/option.{type Option, None}
import gleam/string
import logging
import stratus

const ws_url = "https://jetstream1.us-west.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"

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

pub fn post_record_decoder() -> decode.Decoder(PostRecord) {
  {
    use record_type <- decode.field("$type", decode.string)
    use created_at <- decode.field("createdAt", decode.string)
    use langs <- decode.optional_field("langs", [], decode.list(decode.string))
    use text <- decode.field("text", decode.string)
    decode.success(PostRecord(record_type, created_at, langs, text))
  }
}

pub fn commit_decoder() -> decode.Decoder(Commit) {
  {
    use rev <- decode.field("rev", decode.string)
    use operation <- decode.field("operation", decode.string)
    use collection <- decode.field("collection", decode.string)
    use rkey <- decode.field("rkey", decode.string)
    use record <- decode.optional_field(
      "record",
      None,
      decode.optional(post_record_decoder()),
    )
    use cid <- decode.optional_field(
      "cid",
      None,
      decode.optional(decode.string),
    )
    decode.success(Commit(rev, operation, collection, rkey, record, cid))
  }
}

pub fn post_decoder() -> decode.Decoder(Post) {
  {
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
}

pub fn main() -> Nil {
  logging.configure()
  logging.set_level(logging.Debug)
  logging.log(logging.Info, "Starting bsky_firehose!")

  let assert Ok(req) = request.to(ws_url)

  let builder =
    stratus.websocket(
      request: req,
      init: fn() { #(0, None) },
      loop: fn(state, msg, _conn) {
        case msg {
          stratus.Text(msg) -> {
            case json.parse(msg, using: post_decoder()) {
              Ok(post) -> {
                let new_count = state + 1
                io.println(post.did <> " count: " <> int.to_string(new_count))
                stratus.continue(new_count)
              }
              Error(decode_err) -> {
                logging.log(
                  logging.Error,
                  "Failed to decode message: " <> string.inspect(decode_err),
                )
                logging.log(logging.Error, "Raw message: " <> msg)
                stratus.stop()
              }
            }
          }
          stratus.Binary(_msg) -> stratus.continue(state)
          stratus.User(_msg) -> stratus.continue(state)
        }
      },
    )
    |> stratus.on_close(fn(_state) {
      logging.log(logging.Info, "WebSocket connection closed")
    })

  let assert Ok(subj) = stratus.initialize(builder)

  let assert Ok(owner) = process.subject_owner(subj.data)
  let done =
    process.new_selector()
    |> process.select_specific_monitor(
      process.monitor(owner),
      function.identity,
    )
    |> process.selector_receive_forever

  logging.log(
    logging.Info,
    "WebSocket process exited: " <> string.inspect(done),
  )
}
