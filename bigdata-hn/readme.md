- [done] tokenization: https://docs.rs/tokenizers/latest/tokenizers/
  - read from `hn-comments`
  - write to `hn-comments-tokenized`
- lemmatization: https://docs.rs/nlprule/latest/nlprule/tokenizer/tag/struct.Tagger.html
  - read from `hn-comments-tokenized`
  - write to `hn-comments-lemmatized`
- sentiment: https://docs.rs/rust-bert/latest/rust_bert/
  - read from `hn-comments`
  - write to `hn-comments-sentiment`
- categories: cluster rust-bert sentence embeddings using kmeans
  - read from `hn-comments`
  - write comments and embeddings to json file.
  - read json file and cluster using https://docs.rs/linfa-clustering/latest/linfa_clustering/struct.KMeans.html
- hashing (for tfidf): https://docs.rs/murmur3/latest/murmur3/fn.murmur3_32.html
  - read from `hn-comments-tokenized`
  - write to `hn-comments-hashed`
- tfidf: count number of documents with term in them (i.e. term is counted once per document)
  - stats collector:
    - read from `hn-comments-hashed`
    - write stats to leveldb
  - metric calculator:
    - read from `hn-comments-hashed`
    - write metric to `hn-comments-tfidf`
- bag of words write words to csv and analyze using spark
  - read from `hn-comments-hashed`
  - write stats to leveldb
  - implement reading resulting file