import bsky_firehose.{
  type HubStats, Commit, GetStats, NewPost, Post, PostRecord, Subscribe,
  Unsubscribe,
}
import gleam/erlang/process
import gleam/json
import gleam/option.{None, Some}
import gleeunit

pub fn main() -> Nil {
  gleeunit.main()
}

// --- Decoder tests -----------------------------------------------------------

pub fn decode_full_post_test() {
  let input =
    "
    {
      \"did\": \"did:plc:abc123\",
      \"time_us\": 1700000000000,
      \"kind\": \"commit\",
      \"commit\": {
        \"rev\": \"abc\",
        \"operation\": \"create\",
        \"collection\": \"app.bsky.feed.post\",
        \"rkey\": \"xyz789\",
        \"cid\": \"bafyabc\",
        \"record\": {
          \"$type\": \"app.bsky.feed.post\",
          \"createdAt\": \"2024-01-01T00:00:00.000Z\",
          \"langs\": [\"en\"],
          \"text\": \"Hello Bluesky!\"
        }
      }
    }
  "

  let assert Ok(post) = json.parse(input, bsky_firehose.post_decoder())

  assert post.did == "did:plc:abc123"
  assert post.time_us == 1_700_000_000_000
  assert post.kind == "commit"

  let assert Some(commit) = post.commit
  assert commit.rev == "abc"
  assert commit.operation == "create"
  assert commit.collection == "app.bsky.feed.post"
  assert commit.rkey == "xyz789"
  assert commit.cid == Some("bafyabc")

  let assert Some(record) = commit.record
  assert record.record_type == "app.bsky.feed.post"
  assert record.created_at == "2024-01-01T00:00:00.000Z"
  assert record.langs == ["en"]
  assert record.text == "Hello Bluesky!"
}

pub fn decode_post_without_commit_test() {
  let input =
    "
    {
      \"did\": \"did:plc:abc123\",
      \"time_us\": 1700000000000,
      \"kind\": \"identity\"
    }
  "

  let assert Ok(post) = json.parse(input, bsky_firehose.post_decoder())

  assert post.did == "did:plc:abc123"
  assert post.kind == "identity"
  assert post.commit == None
}

pub fn decode_commit_without_record_test() {
  let input =
    "
    {
      \"did\": \"did:plc:abc123\",
      \"time_us\": 1700000000000,
      \"kind\": \"commit\",
      \"commit\": {
        \"rev\": \"abc\",
        \"operation\": \"delete\",
        \"collection\": \"app.bsky.feed.post\",
        \"rkey\": \"xyz789\"
      }
    }
  "

  let assert Ok(post) = json.parse(input, bsky_firehose.post_decoder())

  let assert Some(commit) = post.commit
  assert commit.operation == "delete"
  assert commit.record == None
  assert commit.cid == None
}

pub fn decode_record_without_langs_test() {
  let input =
    "
    {
      \"$type\": \"app.bsky.feed.post\",
      \"createdAt\": \"2024-01-01T00:00:00.000Z\",
      \"text\": \"No language tags\"
    }
  "

  let assert Ok(record) = json.parse(input, bsky_firehose.post_record_decoder())

  assert record.langs == []
  assert record.text == "No language tags"
}

pub fn decode_record_with_multiple_langs_test() {
  let input =
    "
    {
      \"$type\": \"app.bsky.feed.post\",
      \"createdAt\": \"2024-01-01T00:00:00.000Z\",
      \"langs\": [\"en\", \"pt\"],
      \"text\": \"Bilingual post\"
    }
  "

  let assert Ok(record) = json.parse(input, bsky_firehose.post_record_decoder())

  assert record.langs == ["en", "pt"]
}

pub fn decode_invalid_json_test() {
  let result = json.parse("not json", bsky_firehose.post_decoder())
  case result {
    Ok(_) -> panic as "Should not parse invalid JSON"
    Error(_) -> Nil
  }
}

// --- post_text helper --------------------------------------------------------

pub fn post_text_with_record_test() {
  let post =
    Post(
      did: "did:plc:test",
      time_us: 100,
      kind: "commit",
      commit: Some(Commit(
        rev: "abc",
        operation: "create",
        collection: "app.bsky.feed.post",
        rkey: "xyz",
        cid: None,
        record: Some(PostRecord(
          record_type: "app.bsky.feed.post",
          created_at: "2024-01-01T00:00:00.000Z",
          langs: ["en"],
          text: "Hello!",
        )),
      )),
    )
  assert bsky_firehose.post_text(post) == "Hello!"
}

pub fn post_text_without_record_test() {
  let post =
    Post(did: "did:plc:test", time_us: 100, kind: "commit", commit: None)
  assert bsky_firehose.post_text(post) == ""
}

// --- Hub actor tests ---------------------------------------------------------

pub fn hub_subscribe_and_receive_test() {
  let hub = bsky_firehose.start_hub()
  let post_subject = process.new_subject()
  process.send(hub, Subscribe(post_subject))

  let post =
    Post(did: "did:plc:test", time_us: 123_456, kind: "commit", commit: None)
  process.send(hub, NewPost(post))

  let assert Ok(received) = process.receive(post_subject, 1000)
  assert received.did == "did:plc:test"
  assert received.time_us == 123_456
}

pub fn hub_unsubscribe_test() {
  let hub = bsky_firehose.start_hub()
  let post_subject = process.new_subject()

  process.send(hub, Subscribe(post_subject))
  process.send(hub, Unsubscribe(post_subject))
  process.sleep(50)

  let post =
    Post(did: "did:plc:test", time_us: 123_456, kind: "commit", commit: None)
  process.send(hub, NewPost(post))

  case process.receive(post_subject, 200) {
    Error(_) -> Nil
    Ok(_) -> panic as "Should not receive after unsubscribing"
  }
}

pub fn hub_stats_test() {
  let hub = bsky_firehose.start_hub()
  let stats_subject: process.Subject(HubStats) = process.new_subject()

  process.send(hub, GetStats(stats_subject))
  let assert Ok(stats) = process.receive(stats_subject, 1000)
  assert stats.count == 0

  let post =
    Post(did: "did:plc:test", time_us: 100, kind: "commit", commit: None)
  process.send(hub, NewPost(post))
  process.send(hub, NewPost(post))
  process.send(hub, NewPost(post))
  process.sleep(50)

  let stats_subject2: process.Subject(HubStats) = process.new_subject()
  process.send(hub, GetStats(stats_subject2))
  let assert Ok(stats2) = process.receive(stats_subject2, 1000)
  assert stats2.count == 3
}

pub fn hub_multiple_subscribers_test() {
  let hub = bsky_firehose.start_hub()
  let sub1 = process.new_subject()
  let sub2 = process.new_subject()

  process.send(hub, Subscribe(sub1))
  process.send(hub, Subscribe(sub2))

  let post =
    Post(did: "did:plc:multi", time_us: 999, kind: "commit", commit: None)
  process.send(hub, NewPost(post))

  let assert Ok(r1) = process.receive(sub1, 1000)
  let assert Ok(r2) = process.receive(sub2, 1000)
  assert r1.did == "did:plc:multi"
  assert r2.did == "did:plc:multi"
}
