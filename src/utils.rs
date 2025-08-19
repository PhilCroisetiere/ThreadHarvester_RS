use anyhow::Result;
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use reqwest::Client;

pub async fn fetch_image_b64(client: &Client, url: &str) -> Result<(Option<String>, Option<String>, Option<i64>)> {
    let resp = client.get(url).send().await?;
    let status = resp.status();
    if !status.is_success() {
        return Ok((None, None, None));
    }
    let mime = resp.headers().get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok()).map(|s| s.to_string());
    let bytes = resp.bytes().await?;
    let len = bytes.len() as i64;
    let b64 = B64.encode(&bytes);
    Ok((Some(b64), mime, Some(len)))
}
