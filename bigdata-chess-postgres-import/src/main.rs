mod step;

use crate::step::postgres_import_step;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // postgres_import_step().await;
    Ok(())
}