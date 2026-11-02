fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix("schema")
        .file("schema/tunnelrpc.capnp")
        .file("schema/quic_metadata_protocol.capnp")
        .run()
        .expect("compile capnp schemas");
    println!("cargo:rerun-if-changed=schema");
}
