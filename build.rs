fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix("schema")
        .file("schema/tunnelrpc.capnp")
        .run()
        .expect("compile tunnelrpc.capnp");
    println!("cargo:rerun-if-changed=schema");
}
