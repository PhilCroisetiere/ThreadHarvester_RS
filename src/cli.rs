use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about = "Fast Reddit crawler (old.reddit + JS atomic extraction) with 429 safety")]
pub struct Args {

    #[arg(long)]
    pub excel: String,


    #[arg(long, default_value = "./reddit.duckdb")]
    pub db: String,


    #[arg(long, default_value = "old", value_parser = ["old"])]
    pub mode: String,


    #[arg(long, default_value_t = 20)]
    pub max_pages: usize,


    #[arg(long, default_value_t = true)]
    pub headless: bool,


    #[arg(long, default_value_t = 0.8)]
    pub delay: f64,


    #[arg(long)]
    pub chrome_user_data_dir: Option<String>,


    #[arg(long, default_value_t = 2)]
    pub workers: usize,


    #[arg(long, default_value_t = false)]
    pub use_uc: bool,


    #[arg(long)]
    pub proxies_file: Option<String>,


    #[arg(long, default_value_t = 24)]
    pub rpm: u32,


    #[arg(long, default_value_t = 3)]
    pub polite_attempts: u32,


    #[arg(long, default_value_t = 0.8)]
    pub polite_base: f64,


    #[arg(long, default_value_t = false)]
    pub verbose_429: bool,


    #[arg(long, default_value = "base64", value_parser = ["base64","none"])]
    pub images: String,


    #[arg(long, default_value_t = 500)]
    pub max_comments_per_post: usize,
}
