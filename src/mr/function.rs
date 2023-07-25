//! The hard-coded map reduce functions, may be changed to dynamic linking shared library in the future

/// Word Count application
pub mod wc {
    use crate::mr::worker::KeyValue;

    pub fn map(input: &str) -> Vec<KeyValue> {
        input
            .split_whitespace()
            .map(|x| KeyValue::new(x.to_string(), 1.to_string()) )
            .collect()
    }

    pub fn reduce(key: &str, value: Vec<&str>) -> String {
        value.len().to_string()
    }
}

// TODO: Add more functions for MapReduce applications here