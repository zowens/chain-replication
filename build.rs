extern crate protoc_grpcio;
extern crate protobuf_codegen;

fn main() {
    let proto_root = "proto";

    let protos = ["storage.proto", "manage.proto"];
    for proto in &protos {
        println!("cargo:rerun-if-changed={}/{}", proto_root, proto);
    }

    let mut cust = protobuf_codegen::Customize::default();
    cust.carllerche_bytes_for_bytes = Some(true);

    protoc_grpcio::compile_grpc_protos(&protos, &[proto_root], "storage-server/protocol", Some(cust.clone()))
        .expect("Failed to compile gRPC definitions!");

    protoc_grpcio::compile_grpc_protos(&protos, &[proto_root], "client/protocol",  Some(cust.clone()))
        .expect("Failed to compile gRPC definitions!");

    protoc_grpcio::compile_grpc_protos(&protos, &[proto_root], "management-server/protocol",  Some(cust.clone()))
        .expect("Failed to compile gRPC definitions!");
}
