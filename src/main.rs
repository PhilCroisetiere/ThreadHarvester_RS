use anyhow::Result;
use clap::Parser;
use indicatif::ProgressBar;
use std::time::Duration;

mod cli;
mod throttle;
mod driver;
mod nav;
mod extract;
mod db;
mod models;
mod crawler;
mod utils;

use crate::cli::Args;
use crate::crawler::run_crawl;
use crate::db::{compute_comment_metrics, compute_post_metrics, open_db, start_scan};
use crate::nav::PoliteKnobs;
use crate::throttle::make_limiter;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();


    let db_path = args.db.clone();


    let conn = open_db(&db_path)?;
    let scan_id = start_scan(&conn)?;
    drop(conn);


    let limiter = make_limiter(args.rpm);


    let knobs = PoliteKnobs {
        attempts: args.polite_attempts,
        // values in ms
        initial_ms: (args.polite_base * 1000.0) as u64,
        max_ms: 5000,
        verbose: args.verbose_429,
    };


    let pb = ProgressBar::new_spinner();
    pb.enable_steady_tick(Duration::from_millis(120));
    pb.set_message("Launching workers...");


    let saved = run_crawl(args, limiter, knobs, scan_id).await?;
    pb.finish_and_clear();


    let conn = open_db(&db_path)?;
    eprintln!("[METRICS] Computing post metrics...");
    compute_post_metrics(&conn, scan_id)?;
    eprintln!("[METRICS] Computing comment metrics...");
    compute_comment_metrics(&conn, scan_id)?;
    eprintln!("[SCAN {scan_id}] Saved {saved} posts");

    Ok(())
}
