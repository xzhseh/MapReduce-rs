//! The hard-coded map reduce functions, may be changed to dynamic linking shared library in the future

/// Word Count application
pub mod wc {
    use regex::Regex;

    use crate::mr::worker::KeyValue;

    pub fn map(input: &str) -> Vec<KeyValue> {
        let re = Regex::new(r"[^\w\s]").unwrap();
        let result = re.replace_all(input, "");
        result
            .split_whitespace()
            .map(|x| KeyValue::new(x.to_owned(), 1.to_string()))
            .collect()
    }

    pub fn reduce(_key: &str, value: Vec<&str>) -> String {
        value.len().to_string()
    }
}

// TODO: Add more functions for MapReduce applications here