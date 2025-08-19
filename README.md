# ThreadHarvester_RS

A blazingly fast old.reddit crawler that snapshots posts, comments, and images into DuckDB.

Fast Reddit crawler (old.reddit) in Rust with:
- Multi-worker browsers (one WebDriver per worker)
- Global RPM limiter + shared 429 cooldown + exponential backoff
- Atomic JS extraction (titles/selftext/images/comments) → no stale elements
- DuckDB storage with snapshots and velocity/virality metrics
- Optional image base64
- Excel (XLSX) input of subreddits

## Requirements

- Linux (Ubuntu) recommended.
- **Chrome** and **chromedriver** (matching versions).
  - Ubuntu: `sudo apt install chromium-browser chromium-chromedriver` (or install Chrome + chromedriver).
  - Start chromedriver: `chromedriver --port=9515` (keep running).
- Rust toolchain: `curl https://sh.rustup.rs -sSf | sh`

## Build

```bash
cd ThreadHarvester_RS
cargo build --release
