use anyhow::Result;
use backoff::{ExponentialBackoff, backoff::Backoff};
use thirtyfour::prelude::WebDriver;
use crate::throttle::{gate, set_cooldown_secs, Limiter};

#[derive(Clone, Copy)]
pub struct PoliteKnobs {
    pub attempts: u32,
    pub initial_ms: u64,
    pub max_ms: u64,
    pub verbose: bool,
}

pub async fn polite_get(
    drv: &WebDriver,
    limiter: &Limiter,
    url: &str,
    knobs: PoliteKnobs,
) -> Result<bool> {
    let mut eb = ExponentialBackoff {
        current_interval: std::time::Duration::from_millis(knobs.initial_ms),
        initial_interval: std::time::Duration::from_millis(knobs.initial_ms),
        max_interval:     std::time::Duration::from_millis(knobs.max_ms),
        max_elapsed_time: Some(std::time::Duration::from_secs(15)),
        ..ExponentialBackoff::default()
    };
    for i in 0..knobs.attempts {
        gate(limiter).await;
        let _ = drv.goto(url).await;
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        let title = drv.title().await.unwrap_or_default().to_lowercase();
        let body  = drv.source().await.unwrap_or_default().to_lowercase();
        let is_429 = title.contains("429") || body.contains("too many requests");
        if !is_429 {
            if knobs.verbose && i > 0 {
                eprintln!("[RECOVERED] {url} after attempt {}", i+1);
            }
            return Ok(true);
        }
        let sleep = eb.next_backoff().unwrap_or(std::time::Duration::from_millis(1200));
        if knobs.verbose {
            eprintln!("[429] {url} → backoff {}ms (attempt {}/{})", sleep.as_millis(), i+1, knobs.attempts);
        }
        set_cooldown_secs(20 + (i as u64) * 10);
        tokio::time::sleep(sleep).await;
    }
    if knobs.verbose {
        eprintln!("[GAVE UP] {url}");
    }
    Ok(false)
}
