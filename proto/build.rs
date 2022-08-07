extern crate protobuf_codegen;
extern crate protoc_grpcio;

fn main() {
    let proto_root = ".";

    let protos = ["storage.proto", "manage.proto"];
    for proto in &protos {
        println!("cargo:rerun-if-changed={}/{}", proto_root, proto);
    }

    let cust = protobuf_codegen::Customize { carllerche_bytes_for_bytes: Some(true), ..Default::default() };

    protoc_grpcio::compile_grpc_protos(
        &protos,
        &[proto_root],
        "src",
        Some(cust.clone()),
    )
    .expect("Failed to compile gRPC definitions!");
}
