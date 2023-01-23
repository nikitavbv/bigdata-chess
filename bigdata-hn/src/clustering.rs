use {
    std::{io::{BufReader, BufRead}, fs::File},
    tracing::info,
    kmeans::{KMeans, KMeansConfig},
    crate::models::CommentEmbeddings,
};

pub async fn run_clustering_step() {
    let reader = BufReader::new(File::open("embeddings.json").unwrap());

    let mut dataset = Vec::new();
    let mut texts = Vec::new();
    let mut samples = 0;
    let mut dim = 0;

    info!("reading dataset");
    for line in reader.lines() {
        let line = line.unwrap();
        let data: CommentEmbeddings = serde_json::from_str(&line).unwrap();

        dim = data.embeddings.len();
        texts.push(data.text);
        dataset.push(data.embeddings);
        samples += 1;
    }

    let dataset: Vec<f64> = dataset.into_iter().map(|v| v.into_iter().map(|x| x as f64)).flatten().collect();

    let kmeans = KMeans::new(dataset, samples, dim);
    let result = kmeans.kmeans_lloyd(16, 100, KMeans::init_kmeanplusplus, &KMeansConfig::default());

    let mut i = 0;
    for center in result.assignments {
        let text = texts.get(i).unwrap();
        let text = if text.len() <= 250 { text } else { &text[0..250] };
        info!("{}: {}", center, text);
        i+=1;
    }
}