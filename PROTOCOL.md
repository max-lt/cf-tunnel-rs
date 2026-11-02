# Cloudflare Tunnel edge protocol (QUIC)

Source: `cloudflare/cloudflared`, commit `872b241f94871665c1c157adefcf90c11f0ead96`
(2026-10-30). Paths below are relative to that repo.

Cloudflare does not publish this protocol as stable. Sections marked
"Not read" are not yet extracted.

## 1. Roles

The client (this crate) dials the edge. The edge opens streams back to the
client to deliver requests. The client never listens on a public port.

## 2. Transport

- QUIC over UDP, with datagrams enabled.
- TLS server name: `quic.cftunnel.com`.
- ALPN: `argotunnel`.
- Probe connections use server name `probe.cftunnel.com`, same ALPN.
- The server certificate is checked against the system roots plus the
  Cloudflare Origin CA roots (`tlsconfig/cloudflare_ca.go`).
- An HTTP/2 transport exists (server name `h2.cftunnel.com`). It is out of scope.

Key exchange (`crypto/curves.go`). The default mode offers these groups in
order: `X25519MLKEM768`, `P256Kyber768Draft00` (`0xfe32`), `secp256r1`. The
classical group is a fallback, so a post-quantum exchange is not required.
Strict mode drops `secp256r1`. Only the `--post-quantum` flag enables strict
mode.

QUIC parameters (`quic/constants.go`, `connection/quic_config.go`):

| Parameter | Value |
| --- | --- |
| Handshake idle timeout | 5 s |
| Max idle timeout | 5 s |
| Keep-alive period | 1 s |
| Max incoming bidi and uni streams | 2^60 |
| Initial packet size, IPv4 | 1232 |
| Initial packet size, IPv6 | 1252 |

Each connection index uses its own UDP socket. The client reuses the same
local port for an index across reconnects (`connection/quic.go`).

## 3. Edge discovery

1. Look up SRV `_v2-origintunneld._tcp.argotunnel.com`.
2. If the lookup fails, retry over DNS-over-TLS to `1.1.1.1:853`, server name
   `cloudflare-dns.com`, timeout 15 s.
3. Resolve each SRV target to A and AAAA records.

Source: `edgediscovery/allregions/discovery.go`.

## 4. Credentials

A tunnel token string is standard base64 of a JSON object
(`connection/connection.go`). The JSON keys are:

| Key | Type | Meaning |
| --- | --- | --- |
| `a` | string | Account tag |
| `s` | bytes | Tunnel secret |
| `t` | UUID | Tunnel ID |

The registration call sends `TunnelAuth { accountTag, tunnelSecret }` and the
tunnel ID.

## 5. Connection setup

The client opens N connections. Each has a `connIndex` (u8) and its own QUIC
connection to one edge address.

1. Dial QUIC.
2. Open the first bidirectional stream. The edge treats it as the control
   stream (`connection/quic_connection.go`, `Serve`).
3. Run Cap'n Proto RPC on the control stream. The client is the RPC client.
   It writes no signature before the first RPC message.
4. Call `registerConnection`.
5. Accept streams and datagrams from the edge until shutdown.

### 5.1 Registration RPC

Schema: `tunnelrpc/proto/tunnelrpc.capnp`. Interface `RegistrationServer`
(`@0xf71695ec7fe85497`):

| Ordinal | Method | Params | Result |
| --- | --- | --- | --- |
| 0 | `registerConnection` | `auth: TunnelAuth`, `tunnelId: Data` (16 bytes), `connIndex: UInt8`, `options: ConnectionOptions` | `ConnectionResponse` |
| 1 | `unregisterConnection` | none | none |
| 2 | `updateLocalConfiguration` | `config: Data` | none |

`ConnectionResponse` is a union: `ConnectionError` or `ConnectionDetails`.

- `ConnectionError`: `cause: Text`, `retryAfter: Int64` (nanoseconds),
  `shouldRetry: Bool`. The cause `EDUPCONN` means a duplicate connection for
  this `connIndex`.
- `ConnectionDetails`: `uuid: Data`, `locationName: Text` (colo code),
  `tunnelIsRemotelyManaged: Bool`.

`ConnectionOptions`:

- `client.clientId`: 16-byte UUID, random per process.
- `client.features`: list of strings, see section 5.2.
- `client.version`, `client.arch`: text.
- `originLocalIp`: bytes.
- `replaceExisting`: false.
- `compressionQuality`: 0.
- `numPreviousAttempts`: count of failed attempts before this one.

After a successful registration, if `connIndex` is 0 and
`tunnelIsRemotelyManaged` is false, the client calls
`updateLocalConfiguration` with its ingress configuration as JSON
(`connection/control.go`).

On graceful shutdown the client calls `unregisterConnection`. It waits at most
the grace period for the reply.

### 5.2 Feature strings

From `features/features.go` and `features/selector.go`:

- `support_datagram_v3_2`: the client sends it to select datagram v3.
- `support_datagram_v2`: a user can pass it. The client sends no string by
  default, and the edge then uses datagram v2.
- `postquantum`: the client sends it only in strict mode.

The client removes deprecated strings (`serialized_headers`, `quick_reconnects`,
`allow_remote_config`, `support_quic_eof`, `management_logs`,
`support_datagram_v3`, `support_datagram_v3_1`) before it sends the list.

`cloudflared` picks the datagram version from a user flag first. Otherwise it
reads a percentage from the TXT record `cfd-features.argotunnel.com` (a JSON
object, refreshed on a timer). The same record has a `SkipPrechecks` switch.

### 5.3 Observed behavior

Tested with `examples/spike.rs` against `198.41.192.167:7844` (`region1`),
a remotely managed tunnel:

- The handshake succeeds with TLS 1.3, `ring`, ALPN `argotunnel` and the
  Cloudflare Origin CA roots only. No post-quantum group is needed.
- `capnp-rpc` 0.27 and the edge interoperate. `registerConnection` returns
  `ConnectionDetails` (`locationName` `mrs04`, `tunnelIsRemotelyManaged` true).
- The connection stays open for at least 12 s with a 1 s keep-alive.
- `unregisterConnection` gets no RPC reply. The edge closes the QUIC
  connection with application error code 0.

## 6. Streams opened by the edge

Every stream starts with a 6-byte signature
(`tunnelrpc/quic/protocol.go`):

| Kind | Signature |
| --- | --- |
| Data | `0A 36 CD 12 A1 3E` |
| RPC | `52 BB 82 5C DB 65` |

### 6.1 Data stream

Edge to client:

1. Signature (6 bytes).
2. Version: ASCII `01` (2 bytes).
3. One Cap'n Proto message `ConnectRequest` (standard framing, not packed).
4. Request body bytes until the edge closes its send side.

Client to edge:

1. Signature (6 bytes), version `01` (2 bytes).
2. One Cap'n Proto message `ConnectResponse`.
3. Response body bytes.

Schema: `tunnelrpc/proto/quic_metadata_protocol.capnp`.

- `ConnectRequest`: `dest: Text`, `type: ConnectionType`, `metadata: List(Metadata)`.
- `ConnectionType`: `http` 0, `websocket` 1, `tcp` 2.
- `ConnectResponse`: `error: Text`, `metadata: List(Metadata)`.
- `Metadata`: `key: Text`, `val: Text`.

Metadata keys (`connection/quic_connection.go`):

| Key | Direction | Meaning |
| --- | --- | --- |
| `HttpMethod` | request | HTTP method |
| `HttpHost` | request | Host header value |
| `HttpHeader:<Name>` | both | One header value per entry |
| `HttpStatus` | response | Status code |
| `FlowID` | request, tcp | Flow identifier |
| `FlowConnectRateLimited` | response | `true` when the flow limit is hit |

Rules:

- For HTTP and websocket, `dest` is the request URL.
- A metadata key that contains `HttpHeader` must split on `:` into exactly two
  parts. The client rejects any other key.
- HTTP responses send `ConnectResponse` once, with `HttpStatus` and headers.
  Body bytes follow on the same stream.
- A non-empty `error` means the request failed. The client sends it with
  `HttpStatus` 502 for HTTP.
- For tcp, the client sends an empty `ConnectResponse` to accept. Bytes flow in
  both directions after that.
- Trailers are not supported.
- A request with no `Content-Length`, no chunked encoding and a non-websocket
  type has no body. The client closes its read side.
- On a failure after the response is sent, the client resets the stream
  (`RST_STREAM`).

### 6.2 RPC stream

After the signature, the stream carries Cap'n Proto RPC. The client is the
RPC server. The bootstrap interface is `CloudflaredServer`, which extends
`SessionManager` and `ConfigurationManager`:

| Method | Params | Result |
| --- | --- | --- |
| `registerUdpSession` | `sessionId: Data`, `dstIp: Data`, `dstPort: UInt16`, `closeAfterIdleHint: Int64`, `traceContext: Text` | `RegisterUdpSessionResponse { err, spans }` |
| `unregisterUdpSession` | `sessionId: Data`, `message: Text` | none |
| `updateConfiguration` | `version: Int32`, `config: Data` | `UpdateConfigurationResponse { latestAppliedVersion, err }` |

The client closes the stream after a response timeout.

## 7. Datagrams

Two versions exist. Version 2 is the default. Section 5.2 shows how the client
selects version 3.

### 7.1 Version 3 (`support_datagram_v3_2`)

Source: `quic/v3/datagram.go`. All integers are big-endian. The first byte is
the type.

| Type | Value |
| --- | --- |
| UDP session registration | `0x00` |
| UDP session payload | `0x01` |
| ICMP | `0x02` |
| UDP session registration response | `0x03` |

Registration:

| Offset | Size | Field |
| --- | --- | --- |
| 0 | 1 | Type |
| 1 | 1 | Flags: bit 0 IPv6, bit 1 traced, bit 2 bundled payload |
| 2 | 2 | Destination port |
| 4 | 2 | Idle duration, seconds |
| 6 | 16 | Request ID |
| 22 | 4 or 16 | Destination IP |
| 26 or 38 | rest | Bundled payload |

Payload: type (1), request ID (16), payload (at most 1280 bytes).

Registration response: type (1), response type (1), request ID (16), error
length (2), error text. The response types are `0x00` ok, `0x01` destination
unreachable, `0x02` unable to bind socket, `0x03` too many active flows,
`0xff` error with message.

### 7.2 Version 2 (`support_datagram_v2`)

Source: `quic/datagramv2.go`, `quic/datagram.go`. The client appends metadata
to the end of the payload. Session registration uses the RPC in section 6.2.

The type is the last byte. Values: `0` UDP payload, `1` IP packet,
`2` IP packet with trace ID, `3` tracing spans (protobuf).

| Type | Layout |
| --- | --- |
| UDP payload | payload, session UUID (16 bytes), type (1 byte) |
| IP packet | packet, type (1 byte) |
| IP packet with trace | packet, trace ID, type (1 byte) |

Size limits: the maximum datagram frame is 1350 bytes on unix and 1220 bytes
on Windows. The maximum UDP payload is 1280 bytes on unix.

## 8. Not read

- Region selection (`edgediscovery/allregions/region.go`, `regions.go`).
- ICMP datagrams (`quic/v3/icmp.go`).
- The trace ID length in version 2 IP packets with trace.
- The JSON key names of the `cfd-features.argotunnel.com` record.
- Retry and backoff policy (`supervisor/`).
- Why `go.mod` replaces `quic-go` with a fork (`chungthuang/quic-go`).
- Whether `capnp-rpc` (Rust) interoperates with the Go RPC library.
