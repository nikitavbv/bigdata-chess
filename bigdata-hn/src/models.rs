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

#[derive(Serialize, Deserialize, Debug)]
pub struct CommentLemmatized {
    pub id: String,
    pub text: String,
    pub tokens: Vec<String>,
    pub lemmas: Vec<String>,
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

impl CommentTokenized {
    pub fn lemmatized(self, lemmas: Vec<String>) -> CommentLemmatized {
        CommentLemmatized {
            id: self.id,
            text: self.text, 
            tokens: self.tokens, 
            lemmas: lemmas,
        }
    }
}