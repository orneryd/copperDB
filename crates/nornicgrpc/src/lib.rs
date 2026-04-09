//! gRPC server interface for magnetDB.
//!
//! Equivalent to Go's `pkg/nornicgrpc` in NornicDB.
//! Exposes a Protobuf/gRPC API as an alternative to the Bolt protocol.
//! Uses `tonic` (Rust gRPC) + `prost` (Protobuf codegen).
//!
//! ## Proto Definition
//! Proto files should be placed in `proto/` and compiled by `build.rs`.
//!
//! ## Note on Go equivalent
//! NornicDB uses `google.golang.org/grpc` + `google.golang.org/protobuf`.
//! Rust equivalent: `tonic` + `prost` (both are the de-facto standard).

use thiserror::Error;

#[derive(Debug, Error)]
pub enum GrpcError {
    #[error("gRPC transport error: {0}")]
    Transport(String),
    #[error("proto encoding error: {0}")]
    Encoding(String),
}

// TODO: Define .proto files in proto/ directory.
// TODO: Add build.rs to compile protos with tonic-build.
// TODO: Implement service handlers.
//
// Example build.rs:
// fn main() {
//     tonic_build::compile_protos("proto/magnetdb.proto").unwrap();
// }
