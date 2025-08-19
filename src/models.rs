use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct PostRow {
    pub id: String,
    pub url: String,
    pub title: Option<String>,
    pub author: Option<String>,
    pub score: Option<i64>,
    pub created_utc: Option<i64>,
    pub selftext: Option<String>,
    pub num_comments: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct CommentRow {
    pub id: String,
    pub post_id: String,
    pub parent_fullname: Option<String>,
    pub author: Option<String>,
    pub body: Option<String>,
    pub score: Option<i64>,
    pub created_utc: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct JsPost {
    pub id: String,
    pub parent_fullname: Option<String>,
    pub author: Option<String>,
    pub body: Option<String>,
    pub score: Option<i64>,
    pub created_utc: Option<i64>,
}
