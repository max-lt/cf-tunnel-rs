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

A tunnel token is a JSON object (`connection/connection.go`):

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
  `shouldRetry: Bool`.
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

From `features/features.go`:

- `support_datagram_v2`
- `support_datagram_v3_2`
- `postquantum`

The client removes deprecated strings (`serialized_headers`, `quick_reconnects`,
`allow_remote_config`, `support_quic_eof`, `management_logs`,
`support_datagram_v3`, `support_datagram_v3_1`) before it sends the list.

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

Two versions exist. The client picks one with a feature string.

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

The type byte is the last byte. Values: `0` UDP payload, `1` IP packet,
`2` IP packet with trace ID, `3` tracing spans (protobuf).

## 8. Not read

- Region selection (`edgediscovery/allregions/region.go`, `regions.go`).
- The value of `DuplicateConnectionError`.
- The encoding of the token string (`TunnelToken.Encode`).
- ICMP datagrams (`quic/v3/icmp.go`).
- The exact byte order of the version 2 suffix.
- Retry and backoff policy (`supervisor/`).
- What the `postquantum` feature changes in the TLS handshake.
- Why `go.mod` replaces `quic-go` with a fork (`chungthuang/quic-go`).
- Whether `capnp-rpc` (Rust) interoperates with the Go RPC library.
