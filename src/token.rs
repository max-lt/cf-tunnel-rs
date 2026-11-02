use std::{fmt, str::FromStr};

use base64::{Engine, engine::general_purpose::STANDARD};
use serde::Deserialize;
use uuid::Uuid;

use crate::Error;

/// Tunnel token: standard base64 of `{"a": account tag, "s": secret, "t": tunnel id}`.
#[derive(Clone)]
pub struct Token {
    pub(crate) account_tag: String,
    pub(crate) secret: Vec<u8>,
    pub(crate) tunnel_id: Uuid,
}

#[derive(Deserialize)]
struct Wire {
    a: String,
    s: String,
    t: Uuid,
}

impl Token {
    pub fn tunnel_id(&self) -> Uuid {
        self.tunnel_id
    }
}

impl FromStr for Token {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Error> {
        let json = STANDARD
            .decode(s.trim())
            .map_err(|e| Error::Token(e.to_string()))?;
        let wire: Wire = serde_json::from_slice(&json).map_err(|e| Error::Token(e.to_string()))?;
        let secret = STANDARD
            .decode(&wire.s)
            .map_err(|e| Error::Token(e.to_string()))?;
        Ok(Token {
            account_tag: wire.a,
            secret,
            tunnel_id: wire.t,
        })
    }
}

impl fmt::Debug for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Token")
            .field("tunnel_id", &self.tunnel_id)
            .finish_non_exhaustive()
    }
}
