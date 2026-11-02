//! The messages of the Cap'n Proto RPC protocol and of the tunnel
//! registration, with the field positions of the schemas in `tests/schema`.

use std::time::Duration;

use uuid::Uuid;

use crate::{
    ClientConfig, ConnectionDetails, Error,
    wire::{Builder, Message, Slot, Struct},
};

/// `RegistrationServer` interface id and method ordinals.
const REGISTRATION_INTERFACE: u64 = 0xf716_95ec_7fe8_5497;
pub(crate) const REGISTER_CONNECTION: u16 = 0;
pub(crate) const UNREGISTER_CONNECTION: u16 = 1;

/// Tags of the RPC `Message` union.
const ABORT: u16 = 1;
const CALL: u16 = 2;
const RETURN: u16 = 3;
const FINISH: u16 = 4;
const BOOTSTRAP: u16 = 8;

/// Tags of the `Return` union.
const RESULTS: u16 = 0;
const EXCEPTION: u16 = 1;

/// Tag of `CapDescriptor.senderHosted`.
const SENDER_HOSTED: u16 = 1;

/// Tags of the `ConnectionResponse` union.
const REFUSED: u16 = 0;
const REGISTERED: u16 = 1;

/// The first message of a connection: asks for the edge's main capability.
pub(crate) fn bootstrap(question: u32) -> Vec<u8> {
    let mut b = Builder::new();
    let message = b.init_root(1, 1);
    b.set_u16(message, 0, BOOTSTRAP);

    let body = b.init_struct(message.slot(0), 1, 1);
    b.set_u32(body, 0, question);

    b.finish()
}

/// Tells the edge it can drop the answer to `question`. With `release_caps`
/// false, the capabilities in that answer stay usable.
pub(crate) fn finish(question: u32, release_caps: bool) -> Vec<u8> {
    let mut b = Builder::new();
    let message = b.init_root(1, 1);
    b.set_u16(message, 0, FINISH);

    let body = b.init_struct(message.slot(0), 1, 0);
    b.set_u32(body, 0, question);

    // `releaseResultCaps` is true by default, so the stored bit is inverted.
    b.set_bool(body, 32, !release_caps);

    b.finish()
}

/// A call to `method` of the registration interface, on the exported
/// capability `cap`. `fill` writes the parameter struct into the given slot.
pub(crate) fn call(
    question: u32,
    cap: u32,
    method: u16,
    fill: impl FnOnce(&mut Builder, Slot),
) -> Vec<u8> {
    let mut b = Builder::new();
    let message = b.init_root(1, 1);
    b.set_u16(message, 0, CALL);

    let call = b.init_struct(message.slot(0), 3, 3);
    b.set_u32(call, 0, question);
    b.set_u16(call, 4, method);
    b.set_u64(call, 8, REGISTRATION_INTERFACE);

    // The target is the imported capability (union tag 0 at byte 4).
    let target = b.init_struct(call.slot(0), 1, 1);
    b.set_u16(target, 4, 0);
    b.set_u32(target, 0, cap);

    // The payload holds the parameters and no capabilities.
    let params = b.init_struct(call.slot(1), 0, 2);
    fill(&mut b, params.slot(0));

    b.finish()
}

/// The parameters of `registerConnection`.
pub(crate) fn register_params(b: &mut Builder, slot: Slot, config: &ClientConfig) {
    let params = b.init_struct(slot, 1, 3);
    b.set_u8(params, 0, config.conn_index);

    let auth = b.init_struct(params.slot(0), 0, 2);
    b.set_text(auth.slot(0), &config.token.account_tag);
    b.set_data(auth.slot(1), &config.token.secret);

    b.set_data(params.slot(1), config.token.tunnel_id.as_bytes());

    let options = b.init_struct(params.slot(2), 1, 2);
    let client = b.init_struct(options.slot(0), 0, 4);
    b.set_data(client.slot(0), config.client_id.as_bytes());
    b.set_text(client.slot(2), &config.version);
    b.set_text(client.slot(3), std::env::consts::ARCH);
}

/// The parameters of `unregisterConnection`: an empty struct.
pub(crate) fn unregister_params(b: &mut Builder, slot: Slot) {
    b.init_struct(slot, 0, 0);
}

/// What an incoming RPC message is, for a caller that waits for an answer.
pub(crate) enum Incoming {
    /// The answer to `question`.
    Return(u32),
    /// The peer ended the connection.
    Abort(String),
    /// Anything else. A client that serves no calls ignores it.
    Other,
}

pub(crate) fn classify(message: &Message) -> Result<Incoming, Error> {
    let root = message.root()?;

    match root.u16(0) {
        RETURN => {
            let body = root.structure(0)?.ok_or_else(|| missing("return"))?;

            Ok(Incoming::Return(body.u32(0)))
        }
        ABORT => {
            let body = root.structure(0)?.ok_or_else(|| missing("abort"))?;

            Ok(Incoming::Abort(body.text(0)?.to_owned()))
        }
        _ => Ok(Incoming::Other),
    }
}

fn missing(what: &str) -> Error {
    Error::Protocol(format!("{what} message has no body"))
}

/// The `Payload` of a successful `Return`.
fn results<'a>(message: &'a Message) -> Result<Struct<'a>, Error> {
    let body = message
        .root()?
        .structure(0)?
        .ok_or_else(|| missing("return"))?;

    match body.u16(6) {
        RESULTS => body.structure(0)?.ok_or_else(|| missing("results")),
        EXCEPTION => {
            let exception = body.structure(0)?.ok_or_else(|| missing("exception"))?;

            Err(Error::Rpc(exception.text(0)?.to_owned()))
        }
        other => Err(Error::Protocol(format!("unexpected return type {other}"))),
    }
}

/// The export id of the capability in the answer to a bootstrap request.
pub(crate) fn bootstrap_cap(message: &Message) -> Result<u32, Error> {
    let payload = results(message)?;
    let cap = payload
        .struct_list(1)?
        .get(0)
        .ok_or_else(|| Error::Protocol("bootstrap answer has no capability".into()))?;

    if cap.u16(0) != SENDER_HOSTED {
        return Err(Error::Protocol("bootstrap capability is not hosted".into()));
    }

    Ok(cap.u32(4))
}

/// The result struct of a call, from the `Payload` of its answer.
pub(crate) fn call_results<'a>(message: &'a Message) -> Result<Option<Struct<'a>>, Error> {
    results(message)?.structure(0)
}

/// Reads the outcome of `registerConnection` from the answer message.
pub(crate) fn registration(message: &Message) -> Result<ConnectionDetails, Error> {
    let results = call_results(message)?.ok_or_else(|| missing("registration results"))?;

    registration_from(results)
}

/// Reads the outcome of `registerConnection` from its result struct.
pub(crate) fn registration_from(results: Struct<'_>) -> Result<ConnectionDetails, Error> {
    let response = results
        .structure(0)?
        .ok_or_else(|| missing("connection response"))?;

    match response.u16(0) {
        REGISTERED => {
            let details = response
                .structure(0)?
                .ok_or_else(|| missing("connection details"))?;

            Ok(ConnectionDetails {
                uuid: Uuid::from_slice(details.data_bytes(0)?)
                    .map_err(|e| Error::Protocol(e.to_string()))?,
                location: details.text(1)?.to_owned(),
                remotely_managed: details.bool(0),
            })
        }
        REFUSED => {
            let refusal = response
                .structure(0)?
                .ok_or_else(|| missing("connection error"))?;

            Err(Error::Refused {
                cause: refusal.text(0)?.to_owned(),
                retry_after: Duration::from_nanos(refusal.i64(0).max(0) as u64),
                should_retry: refusal.bool(64),
            })
        }
        other => Err(Error::Protocol(format!("unknown response type {other}"))),
    }
}

#[cfg(test)]
pub(crate) mod fake_edge {
    //! Messages an edge would send, for tests.

    use super::*;

    pub(crate) fn return_message(answer: u32, fill: impl FnOnce(&mut Builder, Slot)) -> Vec<u8> {
        let mut b = Builder::new();
        let message = b.init_root(1, 1);
        b.set_u16(message, 0, RETURN);

        let body = b.init_struct(message.slot(0), 2, 1);
        b.set_u32(body, 0, answer);
        b.set_u16(body, 6, RESULTS);

        let payload = b.init_struct(body.slot(0), 0, 2);
        fill(&mut b, payload.slot(0));

        b.finish()
    }

    /// The answer to a bootstrap request, exporting capability `export`.
    pub(crate) fn bootstrap_answer(answer: u32, export: u32) -> Vec<u8> {
        let mut b = Builder::new();
        let message = b.init_root(1, 1);
        b.set_u16(message, 0, RETURN);

        let body = b.init_struct(message.slot(0), 2, 1);
        b.set_u32(body, 0, answer);
        b.set_u16(body, 6, RESULTS);

        let payload = b.init_struct(body.slot(0), 0, 2);
        let caps = b.init_struct_list(payload.slot(1), 1, 1, 1);
        b.set_u16(caps.get(0), 0, SENDER_HOSTED);
        b.set_u32(caps.get(0), 4, export);

        b.finish()
    }

    /// Writes the results of a successful `registerConnection` into `slot`.
    pub(crate) fn registered_fill(location: &str, uuid: Uuid) -> impl FnOnce(&mut Builder, Slot) {
        let location = location.to_owned();

        move |b, slot| {
            let results = b.init_struct(slot, 0, 1);
            let response = b.init_struct(results.slot(0), 1, 1);
            b.set_u16(response, 0, REGISTERED);

            let details = b.init_struct(response.slot(0), 1, 2);
            b.set_bool(details, 0, true);
            b.set_data(details.slot(0), uuid.as_bytes());
            b.set_text(details.slot(1), &location);
        }
    }

    pub(crate) fn registered(answer: u32, location: &str, uuid: Uuid) -> Vec<u8> {
        return_message(answer, registered_fill(location, uuid))
    }

    /// Writes the results of a refused `registerConnection` into `slot`.
    pub(crate) fn refused_fill(cause: &str, retry: bool) -> impl FnOnce(&mut Builder, Slot) {
        let cause = cause.to_owned();

        move |b, slot| {
            let results = b.init_struct(slot, 0, 1);
            let response = b.init_struct(results.slot(0), 1, 1);
            b.set_u16(response, 0, REFUSED);

            let refusal = b.init_struct(response.slot(0), 2, 1);
            b.set_bool(refusal, 64, retry);
            b.set_text(refusal.slot(0), &cause);
        }
    }

    pub(crate) fn refused(answer: u32, cause: &str, retry: bool) -> Vec<u8> {
        return_message(answer, refused_fill(cause, retry))
    }

    pub(crate) fn empty_answer(answer: u32) -> Vec<u8> {
        return_message(answer, |b, slot| {
            b.init_struct(slot, 0, 0);
        })
    }
}
