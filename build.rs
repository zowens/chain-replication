extern crate protoc_grpcio;

fn main() {
    let proto_root = "proto";
    println!("cargo:rerun-if-changed={}", proto_root);

    let protos = ["storage.proto", "manage.proto"];
    protoc_grpcio::compile_grpc_protos(&protos, &[proto_root], "storage-server/protocol")
        .expect("Failed to compile gRPC definitions!");

    protoc_grpcio::compile_grpc_protos(&protos, &[proto_root], "client/protocol")
        .expect("Failed to compile gRPC definitions!");

    protoc_grpcio::compile_grpc_protos(&protos, &[proto_root], "management-server/protocol")
        .expect("Failed to compile gRPC definitions!");
}
