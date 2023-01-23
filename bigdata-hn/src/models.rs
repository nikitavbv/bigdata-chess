use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Comment {
    pub id: String,
    pub text: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CommentTokenized {
    pub id: String,
    pub text: String,
    pub tokens: Vec<String>,
}

impl Comment {
    pub fn tokenized(self, tokens: Vec<String>) -> CommentTokenized {
        CommentTokenized {
            id: self.id,
            text: self.text,
            tokens: tokens,
        }
    }
}