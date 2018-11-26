extern crate protoc_grpcio;

fn main() {
    let proto_root = "proto";

    let protos = ["storage.proto", "manage.proto"];
    for proto in &protos {
        println!("cargo:rerun-if-changed={}/{}", proto_root, proto);
    }

    protoc_grpcio::compile_grpc_protos(&protos, &[proto_root], "storage-server/protocol")
        .expect("Failed to compile gRPC definitions!");

    protoc_grpcio::compile_grpc_protos(&protos, &[proto_root], "client/protocol")
        .expect("Failed to compile gRPC definitions!");

    protoc_grpcio::compile_grpc_protos(&protos, &[proto_root], "management-server/protocol")
        .expect("Failed to compile gRPC definitions!");
}
