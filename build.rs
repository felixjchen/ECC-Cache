fn main() -> Result<(), Box<dyn std::error::Error>> {
  tonic_build::compile_protos("proto/raft.proto")?;
  tonic_build::compile_protos("proto/ecc.proto")?;

  Ok(())
}
