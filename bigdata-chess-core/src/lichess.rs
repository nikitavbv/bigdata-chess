pub struct Lichess {
}

impl Lichess {
    pub fn new() -> Self {
        Self {
        }
    }

    pub async fn files_from_lichess_database_list(&self) -> Vec<String> {
        reqwest::get("https://database.lichess.org/standard/list.txt")
            .await
            .unwrap()
            .text()
            .await
            .unwrap()
            .split("\n")
            .map(|v| v.to_owned())
            .collect()
    }
}