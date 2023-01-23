use serde::{Serialize, Deserialize};

pub const HASHING_BUCKETS: u32 = 262144; // similar to Spark HashingTF

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

#[derive(Serialize, Deserialize, Debug)]
pub struct CommentHashed {
    pub id: String,
    pub text: String,
    pub tokens: Vec<String>,
    pub hashes: Vec<u32>,
}

impl Comment {
    pub fn tokenized(self, tokens: Vec<String>) -> CommentTokenized {
        CommentTokenized {
            id: self.id,
            text: self.text,
            tokens,
        }
    }
}

impl CommentTokenized {
    pub fn lemmatized(self, lemmas: Vec<String>) -> CommentLemmatized {
        CommentLemmatized {
            id: self.id,
            text: self.text, 
            tokens: self.tokens, 
            lemmas,
        }
    }

    pub fn hashed(self, hashes: Vec<u32>) -> CommentHashed {
        CommentHashed {
            id: self.id,
            text: self.text,
            tokens: self.tokens,
            hashes,
        }
    }
}