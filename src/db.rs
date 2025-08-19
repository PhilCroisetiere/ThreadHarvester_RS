use duckdb::{params, Connection};
use anyhow::{Result, anyhow};
use std::time::{SystemTime, UNIX_EPOCH};

pub fn open_db(path: &str) -> Result<Connection> {
    let conn = Connection::open(path)?;
    conn.execute_batch(r#"
    PRAGMA threads=4;

    -- Keep schema simple: no PRIMARY KEY / IDENTITY to avoid constraint errors
    CREATE TABLE IF NOT EXISTS subreddits (
        id BIGINT,
        name VARCHAR
    );

    CREATE TABLE IF NOT EXISTS posts (
        id VARCHAR,
        subreddit_id BIGINT,
        url VARCHAR,
        title VARCHAR,
        author VARCHAR,
        score BIGINT,
        created_utc BIGINT,
        selftext VARCHAR,
        num_comments BIGINT
    );

    CREATE TABLE IF NOT EXISTS comments (
        id VARCHAR,
        post_id VARCHAR,
        parent_fullname VARCHAR,
        author VARCHAR,
        body VARCHAR,
        score BIGINT,
        created_utc BIGINT
    );

    CREATE TABLE IF NOT EXISTS images (
        post_id VARCHAR,
        url VARCHAR,
        data_base64 VARCHAR,
        mime VARCHAR,
        size_bytes BIGINT
    );

    CREATE TABLE IF NOT EXISTS scans (
        id BIGINT,
        scanned_at BIGINT
    );

    CREATE TABLE IF NOT EXISTS post_snapshots (
        post_id VARCHAR,
        scan_id BIGINT,
        score BIGINT,
        num_comments BIGINT,
        created_utc BIGINT
    );

    CREATE TABLE IF NOT EXISTS comment_snapshots (
        comment_id VARCHAR,
        scan_id BIGINT,
        score BIGINT,
        created_utc BIGINT
    );

    CREATE TABLE IF NOT EXISTS post_metrics (
        post_id VARCHAR,
        scan_id BIGINT,
        score BIGINT,
        num_comments BIGINT,
        prev_scan_id BIGINT,
        prev_score BIGINT,
        prev_num_comments BIGINT,
        dt_seconds BIGINT,
        score_delta BIGINT,
        comments_delta BIGINT,
        score_vph DOUBLE,
        comments_vph DOUBLE,
        virality_score DOUBLE
    );

    CREATE TABLE IF NOT EXISTS comment_metrics (
        comment_id VARCHAR,
        post_id VARCHAR,
        scan_id BIGINT,
        score BIGINT,
        prev_scan_id BIGINT,
        prev_score BIGINT,
        dt_seconds BIGINT,
        score_delta BIGINT,
        score_vph DOUBLE
    );

    -- Helpful indexes (unique optional, but plain indexes are safest)
    CREATE INDEX IF NOT EXISTS idx_subs_name ON subreddits(name);
    CREATE INDEX IF NOT EXISTS idx_subs_id   ON subreddits(id);
    CREATE INDEX IF NOT EXISTS idx_posts_id  ON posts(id);
    CREATE INDEX IF NOT EXISTS idx_scans_id  ON scans(id);
    CREATE INDEX IF NOT EXISTS idx_ps_post_scan ON post_snapshots(post_id, scan_id);
    CREATE INDEX IF NOT EXISTS idx_cs_comment_scan ON comment_snapshots(comment_id, scan_id);
    "#)?;
    Ok(conn)
}



fn now_secs() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64
}

pub fn start_scan(conn: &Connection) -> Result<i64> {

    let id = now_secs();
    conn.execute("INSERT INTO scans(id, scanned_at) VALUES (?, ?)", params![id, id])?;
    Ok(id)
}

pub fn upsert_subreddit(conn: &Connection, name: &str) -> Result<i64> {

    if let Ok(mut stmt) = conn.prepare("SELECT id FROM subreddits WHERE name = ? LIMIT 1") {
        let mut rows = stmt.query(params![name])?;
        if let Some(row) = rows.next()? {
            let id: i64 = row.get(0)?;
            return Ok(id);
        }
    }

    let mut rid_stmt = conn.prepare("SELECT COALESCE(MAX(id)+1, 1) FROM subreddits")?;
    let mut rid_rows = rid_stmt.query([])?;
    let new_id: i64 = rid_rows.next()?.ok_or_else(|| anyhow!("failed to alloc subreddit id"))?.get(0)?;

    conn.execute("INSERT INTO subreddits(id, name) VALUES (?, ?)", params![new_id, name])?;
    Ok(new_id)
}



pub fn upsert_post(
    conn: &Connection,
    id: &str, subreddit_id: i64, url: &str, title: Option<&str>, author: Option<&str>,
    score: Option<i64>, created_utc: Option<i64>, selftext: Option<&str>, num_comments: Option<i64>
) -> Result<()> {
    conn.execute("DELETE FROM posts WHERE id = ?", params![id])?;
    conn.execute(
        r#"INSERT INTO posts
           (id, subreddit_id, url, title, author, score, created_utc, selftext, num_comments)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
        params![id, subreddit_id, url, title, author, score, created_utc, selftext, num_comments]
    )?;
    Ok(())
}

pub fn upsert_comment(
    conn: &Connection,
    id: &str, post_id: &str, parent_fullname: Option<&str>, author: Option<&str>, body: Option<&str>,
    score: Option<i64>, created_utc: Option<i64>
) -> Result<()> {
    conn.execute("DELETE FROM comments WHERE id = ?", params![id])?;
    conn.execute(
        r#"INSERT INTO comments
           (id, post_id, parent_fullname, author, body, score, created_utc)
           VALUES (?, ?, ?, ?, ?, ?, ?)"#,
        params![id, post_id, parent_fullname, author, body, score, created_utc]
    )?;
    Ok(())
}

pub fn ensure_image(
    conn: &Connection, post_id: &str, url: &str, b64: Option<&str>, mime: Option<&str>, size: Option<i64>
) -> Result<()> {
    conn.execute("DELETE FROM images WHERE post_id = ? AND url = ?", params![post_id, url])?;
    conn.execute(
        r#"INSERT INTO images
           (post_id, url, data_base64, mime, size_bytes)
           VALUES (?, ?, ?, ?, ?)"#,
        params![post_id, url, b64, mime, size]
    )?;
    Ok(())
}

pub fn snapshot_post(
    conn: &Connection, post_id: &str, scan_id: i64, score: Option<i64>, num_comments: Option<i64>, created_utc: Option<i64>
) -> Result<()> {
    conn.execute("DELETE FROM post_snapshots WHERE post_id = ? AND scan_id = ?", params![post_id, scan_id])?;
    conn.execute(
        r#"INSERT INTO post_snapshots
           (post_id, scan_id, score, num_comments, created_utc)
           VALUES (?, ?, ?, ?, ?)"#,
        params![post_id, scan_id, score, num_comments, created_utc]
    )?;
    Ok(())
}

pub fn snapshot_comment(
    conn: &Connection, comment_id: &str, scan_id: i64, score: Option<i64>, created_utc: Option<i64>
) -> Result<()> {
    conn.execute("DELETE FROM comment_snapshots WHERE comment_id = ? AND scan_id = ?", params![comment_id, scan_id])?;
    conn.execute(
        r#"INSERT INTO comment_snapshots
           (comment_id, scan_id, score, created_utc)
           VALUES (?, ?, ?, ?)"#,
        params![comment_id, scan_id, score, created_utc]
    )?;
    Ok(())
}



pub fn compute_post_metrics(conn: &Connection, scan_id: i64) -> Result<()> {
    conn.execute_batch(&format!(r#"
    INSERT INTO post_metrics
    SELECT
        s.post_id,
        s.scan_id,
        s.score,
        s.num_comments,
        p.scan_id  AS prev_scan_id,
        p.score    AS prev_score,
        p.num_comments AS prev_num_comments,
        (s.created_utc - p.created_utc) AS dt_seconds,
        (s.score - p.score)             AS score_delta,
        (s.num_comments - p.num_comments) AS comments_delta,
        CASE WHEN (s.created_utc - p.created_utc) > 0
             THEN (s.score - p.score) * 3600.0 / (s.created_utc - p.created_utc) END AS score_vph,
        CASE WHEN (s.created_utc - p.created_utc) > 0
             THEN (s.num_comments - p.num_comments) * 3600.0 / (s.created_utc - p.created_utc) END AS comments_vph,
        COALESCE(
            (CASE WHEN (s.created_utc - p.created_utc) > 0
                  THEN (s.score - p.score) * 3600.0 / (s.created_utc - p.created_utc) ELSE 0 END) * 0.6 +
            (CASE WHEN (s.created_utc - p.created_utc) > 0
                  THEN (s.num_comments - p.num_comments) * 3600.0 / (s.created_utc - p.created_utc) ELSE 0 END) * 0.4,
            0
        ) AS virality_score
    FROM post_snapshots s
    LEFT JOIN LATERAL (
        SELECT * FROM post_snapshots ps
        WHERE ps.post_id = s.post_id AND ps.scan_id < s.scan_id
        ORDER BY ps.scan_id DESC LIMIT 1
    ) p ON true
    WHERE s.scan_id = {scan};
    "#, scan = scan_id))?;
    Ok(())
}

pub fn compute_comment_metrics(conn: &Connection, scan_id: i64) -> Result<()> {
    conn.execute_batch(&format!(r#"
    INSERT INTO comment_metrics
    SELECT
        s.comment_id,
        c.post_id,
        s.scan_id,
        s.score,
        p.scan_id  AS prev_scan_id,
        p.score    AS prev_score,
        (s.created_utc - p.created_utc) AS dt_seconds,
        (s.score - p.score)             AS score_delta,
        CASE WHEN (s.created_utc - p.created_utc) > 0
             THEN (s.score - p.score) * 3600.0 / (s.created_utc - p.created_utc) END AS score_vph
    FROM comment_snapshots s
    JOIN comments c ON c.id = s.comment_id
    LEFT JOIN LATERAL (
        SELECT * FROM comment_snapshots cs
        WHERE cs.comment_id = s.comment_id AND cs.scan_id < s.scan_id
        ORDER BY cs.scan_id DESC LIMIT 1
    ) p ON true
    WHERE s.scan_id = {scan};
    "#, scan = scan_id))?;
    Ok(())
}
