//! The hard-coded map reduce functions, may be changed to dynamic linking shared library in the future

/// Word Count application
pub mod wc {
    pub fn map(input: &str) -> Vec<(String, i32)> {
        input
            .split_whitespace()
            .map(|x| (x.to_string(), 1))
            .collect()
    }

    pub fn reduce(key: &str, value: Vec<i32>) -> (String, i32) {
        (key.to_string(), value.len() as i32)
    }
}

// TODO: Add more functions for MapReduce applications here