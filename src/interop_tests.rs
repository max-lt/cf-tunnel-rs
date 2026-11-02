//! Compares the codec with the reference Cap'n Proto tool, `capnp`, from the
//! C++ implementation. The schemas live in `tests/schema`. Each test skips when
//! the tool is not installed, so the crate builds and tests without it.

use std::{
    io::Write,
    process::{Command, Stdio},
};

use base64::{Engine, engine::general_purpose::STANDARD};
use uuid::Uuid;

use crate::{
    ClientConfig, Error, StreamKind, proto,
    proto::{Caps, Incoming, fake_edge},
    stream::{parse_request, response_bytes},
    wire::{self, Builder, Message},
};

const SCHEMAS: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/schema");

/// Runs `capnp` with `args` and `input` on its standard input. Returns `None`
/// when the tool is missing.
fn capnp(args: &[&str], input: &[u8]) -> Option<Vec<u8>> {
    let mut child = Command::new("capnp")
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .ok()?;

    // The temporary standard input closes at the end of the statement.
    child
        .stdin
        .take()
        .expect("piped input")
        .write_all(input)
        .expect("write to capnp");

    let output = child.wait_with_output().expect("wait for capnp");
    assert!(
        output.status.success(),
        "capnp {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    Some(output.stdout)
}

/// What the tool prints for `bytes` read as `ty`, in its short text form.
fn decode(schema: &str, ty: &str, bytes: &[u8]) -> Option<String> {
    let file = format!("{SCHEMAS}/{schema}.capnp");
    let out = capnp(&["decode", "--short", "-I", SCHEMAS, &file, ty], bytes)?;

    Some(
        String::from_utf8(out)
            .expect("UTF-8 text")
            .trim()
            .to_owned(),
    )
}

/// The message the tool builds from `text`.
fn encode(schema: &str, ty: &str, text: &str) -> Option<Vec<u8>> {
    let file = format!("{SCHEMAS}/{schema}.capnp");

    capnp(&["encode", "-I", SCHEMAS, &file, ty], text.as_bytes())
}

/// Unwraps a tool result, or ends the test when the tool is missing.
macro_rules! reference {
    ($call:expr) => {
        match $call {
            Some(value) => value,
            None => {
                eprintln!("capnp is not installed: skipping");
                return;
            }
        }
    };
}

async fn parse(bytes: &[u8]) -> Message {
    wire::read_message(&mut &bytes[..])
        .await
        .expect("a message")
}

fn config() -> ClientConfig {
    let tunnel = Uuid::from_bytes(*b"0123456789abcdef");
    let json = format!(
        r#"{{"a":"acct","s":"{}","t":"{tunnel}"}}"#,
        STANDARD.encode(b"secret")
    );

    let mut config = ClientConfig::new(
        STANDARD.encode(json).parse().expect("token"),
        ([127, 0, 0, 1], 7844).into(),
    );
    config.client_id = Uuid::from_bytes(*b"ABCDEFGHIJKLMNOP");
    config.version = "test".into();
    config.conn_index = 2;

    config
}

/// Bytes of a message whose root is the struct that `fill` writes.
fn rooted(fill: impl FnOnce(&mut Builder, wire::Slot)) -> Vec<u8> {
    let mut b = Builder::new();
    let root = b.root_slot();
    fill(&mut b, root);

    b.finish()
}

#[test]
fn the_tool_reads_our_rpc_messages() {
    let rpc = |bytes: Vec<u8>| decode("rpc", "Message", &bytes);

    assert_eq!(
        reference!(rpc(proto::bootstrap(5))),
        "(bootstrap = (questionId = 5))"
    );

    assert_eq!(
        reference!(rpc(proto::finish(3, Caps::Keep))),
        "(finish = (questionId = 3, releaseResultCaps = false, \
         requireEarlyCancellationWorkaround = true))"
    );

    assert_eq!(
        reference!(rpc(proto::finish(3, Caps::Release))),
        "(finish = (questionId = 3, releaseResultCaps = true, \
         requireEarlyCancellationWorkaround = true))"
    );

    let call = proto::call(1, 7, proto::REGISTER_CONNECTION, proto::unregister_params);
    assert_eq!(
        reference!(rpc(call)),
        "(call = (questionId = 1, target = (importedCap = 7), \
         interfaceId = 17804583019846587543, methodId = 0, \
         params = (content = <opaque pointer>), \
         sendResultsTo = (caller = void), allowThirdPartyTailCall = false, \
         noPromisePipelining = false, onlyPromisePipeline = false))"
    );

    assert_eq!(
        reference!(rpc(fake_edge::bootstrap_answer(0, 7))),
        "(return = (answerId = 0, releaseParamCaps = true, \
         results = (capTable = [(senderHosted = 7, attachedFd = 255)]), \
         noFinishNeeded = false))"
    );
}

#[test]
fn the_tool_reads_our_registration_messages() {
    let config = config();
    let params = rooted(|b, slot| proto::register_params(b, slot, &config));

    assert_eq!(
        reference!(decode("bench", "RegisterParams", &params)),
        format!(
            "(auth = (accountTag = \"acct\", tunnelSecret = \"secret\"), \
             tunnelId = \"0123456789abcdef\", connIndex = 2, \
             options = (client = (clientId = \"ABCDEFGHIJKLMNOP\", \
             version = \"test\", arch = \"{}\"), \
             replaceExisting = false, compressionQuality = 0, numPreviousAttempts = 0))",
            std::env::consts::ARCH
        )
    );

    let uuid = Uuid::from_bytes(*b"0123456789abcdef");
    let registered = rooted(|b, slot| fake_edge::registered_fill("mrs04", uuid)(b, slot));
    assert_eq!(
        reference!(decode("bench", "RegisterResults", &registered)),
        "(result = (result = (connectionDetails = (uuid = \"0123456789abcdef\", \
         locationName = \"mrs04\", tunnelIsRemotelyManaged = true))))"
    );

    let refused = rooted(|b, slot| fake_edge::refused_fill("EDUPCONN", true)(b, slot));
    assert_eq!(
        reference!(decode("bench", "RegisterResults", &refused)),
        "(result = (result = (error = (cause = \"EDUPCONN\", retryAfter = 0, shouldRetry = true))))"
    );
}

#[test]
fn the_tool_reads_our_stream_responses() {
    let strip = |bytes: Vec<u8>| bytes[8..].to_vec();
    let response =
        |bytes: Vec<u8>| decode("quic_metadata_protocol", "ConnectResponse", &strip(bytes));

    assert_eq!(
        reference!(response(response_bytes(
            Some("boom"),
            &[("HttpStatus", "502"), ("a", "a text longer than a word")]
        ))),
        "(error = \"boom\", metadata = [(key = \"HttpStatus\", val = \"502\"), \
         (key = \"a\", val = \"a text longer than a word\")])"
    );

    assert_eq!(reference!(response(response_bytes(None, &[]))), "()");
}

#[tokio::test]
async fn we_read_the_tools_rpc_messages() {
    let rpc = |text: &str| encode("rpc", "Message", text);

    // A bootstrap answer exports capability 7.
    let answer = reference!(rpc(
        "(return = (answerId = 0, results = (capTable = [(senderHosted = 7)])))"
    ));
    let message = parse(&answer).await;
    assert!(matches!(
        proto::classify(&message).unwrap(),
        Incoming::Return(0)
    ));
    assert_eq!(proto::bootstrap_cap(&message).unwrap(), 7);

    // An exception answer becomes an error that carries the reason.
    let exception = reference!(rpc(
        "(return = (answerId = 1, exception = (reason = \"nope\", type = failed)))"
    ));
    let message = parse(&exception).await;
    assert!(matches!(
        proto::classify(&message).unwrap(),
        Incoming::Return(1)
    ));
    assert!(matches!(
        proto::bootstrap_cap(&message),
        Err(Error::Rpc(reason)) if reason == "nope"
    ));

    // The peer can end the connection.
    let abort = reference!(rpc("(abort = (reason = \"bye\", type = disconnected))"));
    let message = parse(&abort).await;
    assert!(matches!(
        proto::classify(&message).unwrap(),
        Incoming::Abort(reason) if reason == "bye"
    ));

    // Messages a client does not serve are skipped, not failed.
    let finish = reference!(rpc("(finish = (questionId = 4))"));
    let message = parse(&finish).await;
    assert!(matches!(
        proto::classify(&message).unwrap(),
        Incoming::Other
    ));
}

#[tokio::test]
async fn we_read_the_tools_registration_results() {
    let results = |text: &str| encode("bench", "RegisterResults", text);

    let details = reference!(results(
        "(result = (result = (connectionDetails = (uuid = \"0123456789abcdef\", \
         locationName = \"mrs04\", tunnelIsRemotelyManaged = true))))"
    ));
    let message = parse(&details).await;
    let details = proto::registration_from(message.root().unwrap()).unwrap();
    assert_eq!(details.location, "mrs04");
    assert_eq!(details.uuid, Uuid::from_bytes(*b"0123456789abcdef"));
    assert!(details.remotely_managed);

    let refused = reference!(results(
        "(result = (result = (error = (cause = \"EDUPCONN\", retryAfter = 5000000000, shouldRetry = true))))"
    ));
    let message = parse(&refused).await;
    let error = proto::registration_from(message.root().unwrap()).unwrap_err();
    assert!(error.is_duplicate());
    assert!(matches!(
        error,
        Error::Refused { retry_after, should_retry: true, .. }
            if retry_after == std::time::Duration::from_secs(5)
    ));
}

#[tokio::test]
async fn we_read_the_tools_stream_requests() {
    // Many entries, with texts shorter and longer than a word.
    let entries: Vec<String> = (0..60)
        .map(|i| format!("(key = \"HttpHeader:X-{i}\", val = \"{}\")", "v".repeat(i)))
        .collect();
    let text = format!(
        "(dest = \"https://example.com/a?b=c\", type = http, metadata = [{}])",
        entries.join(", ")
    );

    let bytes = reference!(encode("quic_metadata_protocol", "ConnectRequest", &text));
    let request = parse_request(&parse(&bytes).await).unwrap();

    assert_eq!(request.dest, "https://example.com/a?b=c");
    assert_eq!(request.kind, StreamKind::Http);
    assert_eq!(request.metadata.len(), 60);
    assert_eq!(request.metadata[59].0, "HttpHeader:X-59");
    assert_eq!(request.metadata[59].1, "v".repeat(59));

    for (text, kind) in [
        ("websocket", StreamKind::WebSocket),
        ("tcp", StreamKind::Tcp),
    ] {
        let bytes = reference!(encode(
            "quic_metadata_protocol",
            "ConnectRequest",
            &format!("(dest = \"host:22\", type = {text})")
        ));
        let request = parse_request(&parse(&bytes).await).unwrap();

        assert_eq!(request.kind, kind);
        assert!(request.metadata.is_empty());
    }
}
