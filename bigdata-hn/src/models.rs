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

#[derive(Serialize, Deserialize, Debug)]
pub struct Sentiment {
    pub polarity: String,
    pub score: f64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CommentSentiment {
    pub id: String,
    pub text: String,
    pub sentiment: Sentiment,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CommentEmbeddings {
    pub id: String,
    pub text: String,
    pub embeddings: Vec<f32>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CommentTfIdf {
    pub id: String,
    pub text: String,
    pub tokens: Vec<String>,
    pub hashes: Vec<u32>,
    pub tfidf: Vec<f32>,
}

impl Comment {
    pub fn tokenized(self, tokens: Vec<String>) -> CommentTokenized {
        CommentTokenized {
            id: self.id,
            text: self.text,
            tokens,
        }
    }

    pub fn sentiment(self, sentiment: Sentiment) -> CommentSentiment {
        CommentSentiment {
            id: self.id,
            text: self.text,
            sentiment,
        }
    }

    pub fn embeddings(self, embeddings: Vec<f32>) -> CommentEmbeddings {
        CommentEmbeddings {
            id: self.id,
            text: self.text,
            embeddings,
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

impl CommentHashed {
    pub fn tfidf(self, tfidf: Vec<f32>) -> CommentTfIdf {
        CommentTfIdf {
            id: self.id,
            text: self.text,
            tokens: self.tokens,
            hashes: self.hashes,
            tfidf,
        }
    }
}