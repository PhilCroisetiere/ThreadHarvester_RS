use thirtyfour::prelude::*;
use thirtyfour::PageLoadStrategy;
use rand::{seq::SliceRandom, SeedableRng};
use rand::rngs::StdRng;

static UAS: &[&str] = &[
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
];
static LANGS: &[&str] = &["en-US,en;q=0.9", "en-GB,en;q=0.8", "en-CA,en;q=0.8"];
static SIZES: &[(u32, u32)] = &[(1366, 768), (1400, 900), (1600, 900), (1680, 1050)];

pub async fn make_driver(
    headless: bool,
    user_data_dir: Option<&str>,
    _profile_dir: Option<&str>, 
    proxy: Option<&str>,
    worker_id: usize,
    webdriver_url: &str,
) -> WebDriverResult<WebDriver> {
    let mut caps = DesiredCapabilities::chrome();


    let mut args: Vec<String> = vec![
        "--disable-gpu".into(),
        "--no-sandbox".into(),
        "--disable-dev-shm-usage".into(),
        "--disable-blink-features=AutomationControlled".into(),
        "--no-first-run".into(),
        "--no-default-browser-check".into(),
    ];
    if headless {
        args.push("--headless=new".into());
    }

    let mut rng = StdRng::seed_from_u64(1000 + worker_id as u64);
    let ua = *UAS.choose(&mut rng).unwrap();
    let lang = *LANGS.choose(&mut rng).unwrap();
    let (w, h) = *SIZES.choose(&mut rng).unwrap();

    args.push(format!("--user-agent={ua}"));
    args.push(format!("--lang={lang}"));
    args.push(format!("--window-size={w},{h}"));
    if let Some(dir) = user_data_dir {
        args.push(format!("--user-data-dir={dir}"));
    }
    if let Some(p) = proxy {
        args.push(format!("--proxy-server={p}"));
    }


    for a in &args {
        caps.add_arg(a)?;
    }
    caps.set_page_load_strategy(PageLoadStrategy::Eager)?;

    let driver = WebDriver::new(webdriver_url, caps).await?;
    driver
        .set_script_timeout(std::time::Duration::from_secs(30))
        .await?;
    Ok(driver)
}
