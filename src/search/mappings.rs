//! OpenSearch index mappings (spec §6.2). Per-user indices:
//! mail-{user}-msg (metadata + full body text, BM25) and
//! mail-{user}-chunk (chunk text + kNN vector, Lucene engine).

use serde_json::{Value, json};

pub fn msg_index_name(user_id: i64) -> String {
    format!("mail-{user_id}-msg")
}

pub fn chunk_index_name(user_id: i64) -> String {
    format!("mail-{user_id}-chunk")
}

pub fn msg_index_body() -> Value {
    json!({
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {
            "properties": {
                "message_id":      {"type": "keyword"},
                "thread_id":       {"type": "keyword"},
                "mailbox_ids":     {"type": "keyword"},
                "from":            {"type": "text", "fields": {"raw": {"type": "keyword"}}},
                "to":              {"type": "text", "fields": {"raw": {"type": "keyword"}}},
                "cc":              {"type": "text", "fields": {"raw": {"type": "keyword"}}},
                "subject":         {"type": "text"},
                "received_at":     {"type": "date"},
                "size":            {"type": "long"},
                "body_text":       {"type": "text"},
                "has_attachments": {"type": "boolean"},
                "sanitized":       {"type": "boolean"},
            }
        }
    })
}

pub fn chunk_index_body(dimension: usize) -> Value {
    json!({
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "index.knn": true,
        },
        "mappings": {
            "properties": {
                "message_id":  {"type": "keyword"},
                "chunk_index": {"type": "integer"},
                "chunk_text":  {"type": "text"},
                "embedding": {
                    "type": "knn_vector",
                    "dimension": dimension,
                    "method": {
                        "name": "hnsw",
                        "engine": "lucene",
                        "space_type": "cosinesimil",
                    }
                }
            }
        }
    })
}
