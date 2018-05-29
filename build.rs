extern crate protoc_grpcio;

fn main() {
    let proto_root = "proto";
    println!("cargo:rerun-if-changed={}", proto_root);

    protoc_grpcio::compile_grpc_protos(
        &["storage.proto"],
        &[proto_root],
        "storage-server/protocol",
    ).expect("Failed to compile gRPC definitions!");

    protoc_grpcio::compile_grpc_protos(&["storage.proto"], &[proto_root], "client/protocol")
        .expect("Failed to compile gRPC definitions!");
}
