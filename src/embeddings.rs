use anyhow::Result;
use embed_anything::embed_query;
use embed_anything::embeddings::embed::{Embedder, EmbedderBuilder};
use embed_anything::embeddings::local::text_embedding::ONNXModel;
use std::sync::Arc;

const BATCH_SIZE: usize = 32;

pub struct EmbeddingManager {
    embedder: Arc<Embedder>,
}

impl EmbeddingManager {
    pub fn new() -> Result<Self> {
        let embedder = EmbedderBuilder::new()
            .model_architecture("bert")
            .onnx_model_id(Some(ONNXModel::AllMiniLML6V2))
            .from_pretrained_onnx()?;

        Ok(Self {
            embedder: Arc::new(embedder),
        })
    }

    pub async fn embed_texts(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(vec![]);
        }

        let mut all_embeddings = Vec::with_capacity(texts.len());

        // Process in batches
        for chunk in texts.chunks(BATCH_SIZE) {
            let text_refs: Vec<&str> = chunk.iter().map(|s| s.as_str()).collect();
            let results = embed_query(&text_refs, &self.embedder, None).await?;

            for result in results {
                let embedding = result.embedding.to_dense()?;
                all_embeddings.push(embedding);
            }
        }

        Ok(all_embeddings)
    }

    pub async fn embed_query(&self, query: &str) -> Result<Vec<f32>> {
        let results = embed_query(&[query], &self.embedder, None).await?;
        let embedding = results
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("No embedding returned"))?
            .embedding
            .to_dense()?;
        Ok(embedding)
    }
}

pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() {
        return 0.0;
    }

    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();

    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }

    dot / (norm_a * norm_b)
}

// Helper to convert Vec<f32> to bytes for storage
pub fn embedding_to_bytes(embedding: &[f32]) -> Vec<u8> {
    use byteorder::{LittleEndian, WriteBytesExt};
    let mut bytes = Vec::with_capacity(embedding.len() * 4);
    for &val in embedding {
        bytes.write_f32::<LittleEndian>(val).unwrap();
    }
    bytes
}

// Helper to convert bytes back to Vec<f32>
pub fn bytes_to_embedding(bytes: &[u8]) -> Vec<f32> {
    use byteorder::{LittleEndian, ReadBytesExt};
    use std::io::Cursor;
    let mut cursor = Cursor::new(bytes);
    let mut embedding = Vec::with_capacity(bytes.len() / 4);
    while let Ok(val) = cursor.read_f32::<LittleEndian>() {
        embedding.push(val);
    }
    embedding
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cosine_similarity() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        assert!((cosine_similarity(&a, &b) - 1.0).abs() < 0.001);

        let c = vec![0.0, 1.0, 0.0];
        assert!(cosine_similarity(&a, &c).abs() < 0.001);
    }

    #[test]
    fn test_embedding_serialization() {
        let embedding = vec![1.0, 2.0, 3.0, -4.5];
        let bytes = embedding_to_bytes(&embedding);
        let restored = bytes_to_embedding(&bytes);
        assert_eq!(embedding, restored);
    }
}
