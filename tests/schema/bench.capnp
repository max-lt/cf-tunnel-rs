@0xd52ede7948b1c7a5;
# Mirrors of the implicit parameter and result structs of
# RegistrationServer.registerConnection, which the capnp tool cannot name.
# The field order is the same, so the layout is the same.

using Tunnel = import "tunnelrpc.capnp";

struct RegisterParams {
  auth @0 :Tunnel.TunnelAuth;
  tunnelId @1 :Data;
  connIndex @2 :UInt8;
  options @3 :Tunnel.ConnectionOptions;
}

struct RegisterResults {
  result @0 :Tunnel.ConnectionResponse;
}
