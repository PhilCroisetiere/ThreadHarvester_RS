use crate::cli::Args;
use crate::driver::make_driver;
use crate::nav::{polite_get, PoliteKnobs};
use crate::extract::{listing_old_top_day, post_old_page};
use crate::throttle::Limiter;
use crate::db::*;
use crate::models::*;
use crate::utils::fetch_image_b64;

use crossbeam_channel::unbounded;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle, ProgressDrawTarget};
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};
use rand::{seq::SliceRandom, SeedableRng};
use rand::rngs::StdRng;
use calamine::{open_workbook, Reader, Xlsx};
use tokio::task::JoinSet;

fn load_subreddits(xlsx_path: &str) -> Result<Vec<String>> {
    let mut wb: Xlsx<_> = open_workbook(xlsx_path)?;
    let first = wb
        .sheet_names()
        .get(0)
        .ok_or_else(|| anyhow!("empty workbook"))?
        .to_string();
    let range = wb.worksheet_range(&first).ok_or_else(|| anyhow!("no sheet"))??;

    let mut res = vec![];
    let mut header: Option<Vec<String>> = None;

    for (i, row) in range.rows().enumerate() {
        let vals: Vec<String> = row.iter().map(|c| c.to_string()).collect();
        if i == 0 {
            header = Some(vals.iter().map(|s| s.trim().to_lowercase()).collect());
            continue;
        }
        let sub = if let Some(h) = &header {
            if let Some(idx) = h.iter().position(|c| c == "subreddit") {
                vals.get(idx).map(|s| s.to_string())
            } else {
                vals.get(0).map(|s| s.to_string())
            }
        } else {
            vals.get(0).map(|s| s.to_string())
        };
        if let Some(mut s) = sub {
            s = s.trim_start_matches('/').trim_start_matches("r/").to_string();
            if !s.is_empty() {
                res.push(s);
            }
        }
    }
    Ok(res)
}

#[derive(Debug)]
enum Msg {
    BeginSubreddit(String),
    PostBundle {
        subreddit: String,
        post: PostRow,
        images: Vec<(String, Option<String>, Option<String>, Option<i64>)>, 
        comments: Vec<CommentRow>,
        snapshot: (String, i64, Option<i64>, Option<i64>, Option<i64>), 
    },
    Quit,
}

fn writer_thread(db_path: String, rx: crossbeam_channel::Receiver<Msg>) -> Result<()> {
    let conn = open_db(&db_path)?;
    let mut current_sub = String::new();
    while let Ok(msg) = rx.recv() {
        match msg {
            Msg::BeginSubreddit(s) => {
                current_sub = s;
                let _ = upsert_subreddit(&conn, &current_sub);
            }
            Msg::PostBundle { subreddit, post, images, comments, snapshot } => {
                let sub_id = upsert_subreddit(&conn, &subreddit)?;
                upsert_post(
                    &conn,
                    &post.id,
                    sub_id,
                    &post.url,
                    post.title.as_deref(),
                    post.author.as_deref(),
                    post.score,
                    post.created_utc,
                    post.selftext.as_deref(),
                    post.num_comments,
                )?;
                for (u, b64, mime, size) in images {
                    ensure_image(&conn, &post.id, &u, b64.as_deref(), mime.as_deref(), size)?;
                }
                for c in comments {
                    upsert_comment(
                        &conn,
                        &c.id,
                        &c.post_id,
                        c.parent_fullname.as_deref(),
                        c.author.as_deref(),
                        c.body.as_deref(),
                        c.score,
                        c.created_utc,
                    )?;
                    snapshot_comment(&conn, &c.id, snapshot.1, c.score, c.created_utc)?;
                }
                snapshot_post(&conn, &post.id, snapshot.1, snapshot.2, snapshot.3, snapshot.4)?;
            }
            Msg::Quit => break,
        }
    }
    Ok(())
}

fn session_gone<E: std::fmt::Display>(e: &E) -> bool {
    let s = e.to_string().to_lowercase();
    s.contains("invalid session id")
        || s.contains("session deleted")
        || s.contains("not connected to devtools")
}

#[inline]
fn ui_set(pb: &ProgressBar, last: &mut Instant, msg: String) {

    if last.elapsed() >= Duration::from_millis(250) {
        pb.set_message(msg);
        *last = Instant::now();
    }
}

pub async fn run_crawl(args: Args, limiter: Limiter, knobs: PoliteKnobs, scan_id: i64) -> Result<usize> {

    let subs = load_subreddits(&args.excel)?;
    if subs.is_empty() { return Err(anyhow!("No subreddits in {}", args.excel)); }


    let proxies: Vec<String> = if let Some(p) = &args.proxies_file {
        let t = std::fs::read_to_string(p).unwrap_or_default();
        t.lines().map(|l| l.trim())
            .filter(|s| !s.is_empty() && !s.starts_with('#'))
            .map(|s| s.to_string()).collect()
    } else { vec![] };


    let (tx, rx) = unbounded::<Msg>();
    let db_path = args.db.clone();
    let wt = std::thread::spawn(move || writer_thread(db_path, rx).expect("writer thread failed"));


    let mp = MultiProgress::with_draw_target(ProgressDrawTarget::stdout());
    let overall = mp.add(ProgressBar::new(subs.len() as u64));
    overall.set_style(
        ProgressStyle::with_template("{spinner:.green} {pos}/{len} subs done")?
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
    );
    overall.enable_steady_tick(Duration::from_millis(120));


    let mut order = subs.clone();
    let mut rng = StdRng::seed_from_u64(42);
    order.shuffle(&mut rng);

    let mut js = JoinSet::new();
    let per_worker = (order.len() + args.workers - 1) / args.workers;

    for w in 0..args.workers {
        let slice = order.clone().into_iter().skip(w * per_worker).take(per_worker).collect::<Vec<_>>();
        if slice.is_empty() { continue; }

        let wbar = mp.add(ProgressBar::new(0));
        wbar.set_style(
            ProgressStyle::with_template("w{prefix}: {wide_msg}")?
                .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
        );
        wbar.set_prefix(format!("{w}"));
        wbar.enable_steady_tick(Duration::from_millis(120));

        let txc           = tx.clone();
        let limiter_c     = limiter.clone();
        let knobs_c       = knobs;
        let headless      = args.headless;
        let delay         = args.delay;
        let max_pages     = args.max_pages;
        let images_mode   = args.images.clone();
        let max_comments  = args.max_comments_per_post;


        let user_data_dir_w = args.chrome_user_data_dir.as_ref().map(|base| {
            let p = std::path::Path::new(base).join(format!("worker-{}", w));
            let _ = std::fs::create_dir_all(&p);
            p.to_string_lossy().to_string()
        });

        
        let proxy = if proxies.is_empty() { None } else { Some(proxies[w % proxies.len()].clone()) };
        let overall_c = overall.clone();

        js.spawn(async move {
            let webdriver_url = std::env::var("WEBDRIVER_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:9515".to_string());

            let drv = match make_driver(
                headless,
                user_data_dir_w.as_deref(),
                None, 
                proxy.as_deref(),
                w,
                &webdriver_url
            ).await {
                Ok(drv) => drv,
                Err(e) => { eprintln!("[worker {w}] start driver error: {e}"); wbar.finish_with_message("failed to start"); return 0usize; }
            };

            let mut saved = 0usize;
            let mut last_ui = Instant::now();

            'sub_loop: for sub in slice {
                ui_set(&wbar, &mut last_ui, format!("r/{sub} — page 1/{max_pages}"));
                let _ = txc.send(Msg::BeginSubreddit(sub.clone()));

                let base = format!("https://old.reddit.com/r/{}/top/?t=day", sub);
                let mut next = Some(base);
                let mut pages = 0usize;

                while let Some(url) = next {
                    if pages >= max_pages { break; }
                    ui_set(&wbar, &mut last_ui, format!("r/{sub} — page {}/{}", pages + 1, max_pages));

                    if !polite_get(&drv, &limiter_c, &url, knobs_c).await.unwrap_or(false) { break; }

                    match listing_old_top_day(&drv).await {
                        Err(e) => {
                            if session_gone(&e) { eprintln!("[w{w}] session lost on listing: {e}"); break 'sub_loop; }
                            eprintln!("[{sub}] listing parse error: {e}");
                            break;
                        }
                        Ok(listing) => {
                            let total_on_page = listing.len().max(1);
                            for (idx, (post_id, href_opt, ts_opt)) in listing.into_iter().enumerate() {
                                ui_set(
                                    &wbar, &mut last_ui,
                                    format!("r/{sub} — page {}/{} • post {}/{}",
                                            pages + 1, max_pages, idx + 1, total_on_page)
                                );

                                let post_url = format!("https://old.reddit.com/comments/{}/", post_id);
                                if !polite_get(&drv, &limiter_c, &post_url, knobs_c).await.unwrap_or(false) {
                                    if drv.current_url().await.is_err() { break 'sub_loop; }
                                    continue;
                                }

                                match post_old_page(&drv).await {
                                    Ok(v) => {
                                        let title   = v.get("title").and_then(|x| x.as_str()).map(|s| s.to_string());
                                        let author  = v.get("author").and_then(|x| x.as_str()).map(|s| s.to_string());
                                        let score   = v.get("score").and_then(|x| x.as_i64());
                                        let created = v.get("created_utc").and_then(|x| x.as_i64()).or(ts_opt);
                                        let body    = v.get("selftext").and_then(|x| x.as_str()).map(|s| s.to_string());
                                        let ncom    = v.get("num_comments").and_then(|x| x.as_i64());

                                        let mut images_out = vec![];
                                        let imgs = v.get("images").and_then(|x| x.as_array()).cloned().unwrap_or_default();
                                        if images_mode == "base64" && !imgs.is_empty() {
                                            let client = reqwest::Client::builder().build().unwrap();
                                            for u in imgs.iter().filter_map(|x| x.as_str()) {
                                                if let Ok((b64, mime, size)) = fetch_image_b64(&client, u).await {
                                                    images_out.push((u.to_string(), b64, mime, size));
                                                } else {
                                                    images_out.push((u.to_string(), None, None, None));
                                                }
                                            }
                                        } else {
                                            for u in imgs.iter().filter_map(|x| x.as_str()) {
                                                images_out.push((u.to_string(), None, None, None));
                                            }
                                        }

                                        let mut comments_out = vec![];
                                        let comments = v.get("comments").and_then(|x| x.as_array()).cloned().unwrap_or_default();
                                        for c in comments.into_iter().take(max_comments) {
                                            let cid   = c.get("id").and_then(|x| x.as_str()).unwrap_or("").to_string();
                                            if cid.is_empty() { continue; }
                                            let cauth = c.get("author").and_then(|x| x.as_str()).map(|s| s.to_string());
                                            let cbody = c.get("body").and_then(|x| x.as_str()).map(|s| s.to_string());
                                            let csc   = c.get("score").and_then(|x| x.as_i64());
                                            let cts   = c.get("created_utc").and_then(|x| x.as_i64());
                                            let par   = c.get("parent_fullname").and_then(|x| x.as_str()).map(|s| s.to_string());
                                            comments_out.push(CommentRow {
                                                id: cid, post_id: post_id.clone(),
                                                parent_fullname: par, author: cauth, body: cbody,
                                                score: csc, created_utc: cts
                                            });
                                        }

                                        let row = PostRow {
                                            id: post_id.clone(),
                                            url: href_opt.unwrap_or(post_url.clone()),
                                            title, author, score, created_utc: created, selftext: body, num_comments: ncom,
                                        };
                                        let snap = (post_id.clone(), scan_id, score, ncom, created);
                                        let _ = txc.send(Msg::PostBundle {
                                            subreddit: sub.clone(),
                                            post: row,
                                            images: images_out,
                                            comments: comments_out,
                                            snapshot: snap,
                                        });

                                        saved += 1;
                                        tokio::time::sleep(std::time::Duration::from_millis(
                                            (delay_ms(delay) as f64 * (0.6 + rand::random::<f64>() * 0.8)) as u64
                                        )).await;
                                    }
                                    Err(e) => {
                                        if session_gone(&e) { eprintln!("[w{w}] session lost on post: {e}"); break 'sub_loop; }
                                        eprintln!("[{sub}] post {post_id} parse error: {e}");
                                    }
                                }
                            }
                        }
                    }


                    let mut next_href = drv.find_all(thirtyfour::By::Css("span.next-button > a")).await
                        .ok().and_then(|mut v| v.pop())
                        .and_then(|e| futures::executor::block_on(e.attr("href")).ok().flatten());

                    if next_href.is_none() {
                        next_href = drv.execute("return (document.querySelector('span.next-button > a')||{}).href;", vec![])
                            .await.ok()
                            .and_then(|ret| ret.convert::<Option<String>>().ok())
                            .flatten();
                    }

                    next = next_href;
                    pages += 1; 
                }

                overall_c.inc(1);
                ui_set(&wbar, &mut last_ui, format!("r/{sub} — done"));
            }

            let _ = drv.quit().await; 
            wbar.finish_and_clear();
            saved
        });
    }

    drop(tx);


    let mut total_saved = 0usize;
    while let Some(res) = js.join_next().await {
        if let Ok(n) = res { total_saved += n; }
    }

    overall.finish_and_clear();
    let _ = mp.clear();

    wt.join().ok();

    Ok(total_saved)
}

#[inline]
fn delay_ms(base: f64) -> u64 {
    (base * 1000.0) as u64
}
