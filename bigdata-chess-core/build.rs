use {
    std::io::Result,
    prost_build::Config,
};

fn main() -> Result<()> {
    Config::new()
        .type_attribute("chess.ChessGame", "#[derive(derive_builder::Builder)]")
        .type_attribute("chess.Player", "#[derive(derive_builder::Builder)]")
        .field_attribute("chess.Player.title", "#[builder(default)]")
        .field_attribute("chess.ChessGame.rating_outcome_for_white", "#[builder(default)]")
        .field_attribute("chess.ChessGame.rating_outcome_for_black", "#[builder(default)]")
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(&["proto/chess.proto"], &["proto/"])?;
    Ok(())
}