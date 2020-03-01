#[no_mangle]
pub fn map(content: &str) -> Vec<(String, String)> {
    content
        .split(|c: char| !c.is_ascii_alphabetic())
        .map(|word| (word.to_string(), "1".to_string()))
        .collect()
}

#[no_mangle]
pub fn reduce(_key: &str, values: Vec<String>) -> String {
    values.len().to_string()
}
