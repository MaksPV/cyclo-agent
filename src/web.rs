use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Json};
use tokio::sync::RwLock;
use tower_cookies::{Cookie, CookieManagerLayer, Cookies};

use crate::auth::{hash_password, hash_token, new_session_token, valid_login, verify_password};
use crate::config::Config;
use crate::scheduler::{next_jobs, now_ms, SchedStatus};
use crate::store::Store;

#[derive(Clone)]
pub struct App {
    pub store: Store,
    pub cfg: Config,
    pub status: Arc<RwLock<SchedStatus>>,
    pub wake: Arc<tokio::sync::Notify>,
}

const COOKIE: &str = "cyclo_session";
const SESSION_TTL_SECS: i64 = 12 * 3600;

fn authed(cookies: &Cookies, app: &App) -> Option<i64> {
    let raw = cookies.get(COOKIE)?.value().to_owned();
    app.store.session_user(&hash_token(&raw), now_ms())
}

pub fn router(app: App) -> axum::Router {
    axum::Router::new()
        .route("/", axum::routing::get(index))
        .route("/api/status", axum::routing::get(status))
        .route("/api/setup", axum::routing::post(setup))
        .route("/api/login", axum::routing::post(login))
        .route("/api/logout", axum::routing::post(logout))
        .route("/api/runs", axum::routing::get(runs))
        .route("/api/next", axum::routing::get(next))
        .route(
            "/api/schedule",
            axum::routing::get(get_schedule).post(save_schedule),
        )
        .route("/api/validate", axum::routing::post(validate))
        .route("/api/reload", axum::routing::post(reload))
        .route("/healthz", axum::routing::get(|| async { "ok" }))
        .layer(CookieManagerLayer::new())
        .with_state(app)
}

async fn index() -> Html<&'static str> {
    Html(include_str!("../static/index.html"))
}

async fn status(State(app): State<App>, cookies: Cookies) -> impl IntoResponse {
    let st = app.status.read().await;
    let setup_required = !app.store.has_users();
    Json(serde_json::json!({
        "setup_required": setup_required,
        "authenticated": authed(&cookies, &app).is_some(),
        "schedule_valid": st.valid,
        "schedule_error": st.error,
        "schedule_name": st.name,
        "schedule_path": app.cfg.schedule.to_string_lossy(),
        "now": now_ms(),
    }))
}

#[derive(serde::Deserialize)]
struct SetupBody {
    login: String,
    password: String,
    confirm: String,
}

async fn setup(
    State(app): State<App>,
    cookies: Cookies,
    Json(b): Json<SetupBody>,
) -> impl IntoResponse {
    if app.store.has_users() {
        return (StatusCode::FORBIDDEN, "already set up".to_owned()).into_response();
    }
    if !valid_login(&b.login) {
        return (StatusCode::BAD_REQUEST, "login: 3-32 [a-z0-9_-]".to_owned()).into_response();
    }
    if b.password.len() < 8 {
        return (
            StatusCode::BAD_REQUEST,
            "password too short (min 8)".to_owned(),
        )
            .into_response();
    }
    if b.password != b.confirm {
        return (StatusCode::BAD_REQUEST, "passwords do not match".to_owned()).into_response();
    }
    let hash = match hash_password(&b.password) {
        Ok(h) => h,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    };
    let now = now_ms();
    if let Err(e) = app.store.create_user(&b.login, &hash, now) {
        return (StatusCode::INTERNAL_SERVER_ERROR, e).into_response();
    }
    let uid = app.store.user_hash(&b.login).map(|(id, _)| id).unwrap_or(1);
    let (raw, digest) = new_session_token();
    app.store
        .create_session(&digest, uid, now, SESSION_TTL_SECS);
    let mut c = Cookie::new(COOKIE, raw);
    c.set_http_only(true);
    c.set_path("/");
    cookies.add(c);
    StatusCode::CREATED.into_response()
}

#[derive(serde::Deserialize)]
struct LoginBody {
    login: String,
    password: String,
}

async fn login(
    State(app): State<App>,
    cookies: Cookies,
    Json(b): Json<LoginBody>,
) -> impl IntoResponse {
    if !app.store.has_users() {
        return (StatusCode::CONFLICT, "setup required".to_owned()).into_response();
    }
    let Some((uid, hash)) = app.store.user_hash(&b.login) else {
        return (StatusCode::UNAUTHORIZED, "bad login".to_owned()).into_response();
    };
    if !verify_password(&b.password, &hash) {
        return (StatusCode::UNAUTHORIZED, "bad login".to_owned()).into_response();
    }
    let (raw, digest) = new_session_token();
    app.store
        .create_session(&digest, uid, now_ms(), SESSION_TTL_SECS);
    let mut c = Cookie::new(COOKIE, raw);
    c.set_http_only(true);
    c.set_path("/");
    cookies.add(c);
    StatusCode::NO_CONTENT.into_response()
}

async fn logout(State(app): State<App>, cookies: Cookies) -> impl IntoResponse {
    if let Some(raw) = cookies.get(COOKIE).map(|c| c.value().to_owned()) {
        app.store.delete_session(&hash_token(&raw));
        cookies.remove(Cookie::new(COOKIE, ""));
    }
    StatusCode::NO_CONTENT.into_response()
}

#[derive(serde::Deserialize)]
struct RunsQuery {
    from: Option<i64>,
    to: Option<i64>,
    limit: Option<i64>,
}

async fn runs(
    State(app): State<App>,
    cookies: Cookies,
    Query(q): Query<RunsQuery>,
) -> impl IntoResponse {
    if authed(&cookies, &app).is_none() {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let now = now_ms();
    let to = q.to.unwrap_or(now);
    let from = q.from.unwrap_or(now - 24 * 3_600_000);
    Json(serde_json::to_value(app.store.list_runs(from, to, q.limit.unwrap_or(200))).unwrap())
        .into_response()
}

#[derive(serde::Deserialize)]
struct NextQuery {
    n: Option<usize>,
    within_secs: Option<i64>,
}

async fn next(
    State(app): State<App>,
    cookies: Cookies,
    Query(q): Query<NextQuery>,
) -> impl IntoResponse {
    if authed(&cookies, &app).is_none() {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let within = q
        .within_secs
        .unwrap_or(app.cfg.lookahead_secs as i64)
        .max(1)
        * 1000;
    match next_jobs(
        &app.cfg.schedule,
        now_ms(),
        within,
        q.n.unwrap_or(50).min(500),
    ) {
        Ok(v) => Json(serde_json::json!({"events": v})).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e).into_response(),
    }
}

async fn get_schedule(State(app): State<App>, cookies: Cookies) -> impl IntoResponse {
    if authed(&cookies, &app).is_none() {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match std::fs::read_to_string(&app.cfg.schedule) {
        Ok(t) => Json(serde_json::json!({"content": t})).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    }
}

#[derive(serde::Deserialize)]
struct ScheduleBody {
    content: String,
}

fn check_content(content: &str, schedule_path: &std::path::Path) -> Result<(), String> {
    let base = schedule_path
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| std::path::PathBuf::from("."));
    // Валидация без записи: check_source повторяет конвейер CLI 1:1.
    cyclorithm_core::pipeline::check_source(content, &base).map_err(|e| e.to_string())
}

async fn save_schedule(
    State(app): State<App>,
    cookies: Cookies,
    Json(b): Json<ScheduleBody>,
) -> impl IntoResponse {
    if authed(&cookies, &app).is_none() {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if let Err(e) = check_content(&b.content, &app.cfg.schedule) {
        return (StatusCode::BAD_REQUEST, e).into_response();
    }
    if let Some(dir) = app.cfg.schedule.parent() {
        if !dir.as_os_str().is_empty() {
            let _ = std::fs::create_dir_all(dir);
        }
    }
    match std::fs::write(&app.cfg.schedule, &b.content) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn validate(
    State(app): State<App>,
    cookies: Cookies,
    Json(b): Json<ScheduleBody>,
) -> impl IntoResponse {
    if authed(&cookies, &app).is_none() {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match check_content(&b.content, &app.cfg.schedule) {
        Ok(()) => Json(serde_json::json!({"ok": true})).into_response(),
        Err(e) => {
            let (code, message) = match e.split_once(": ") {
                Some((a, b)) => (a, b),
                None => ("syntax", e.as_str()),
            };
            Json(serde_json::json!({"ok": false, "code": code, "message": message})).into_response()
        }
    }
}

async fn reload(State(app): State<App>, cookies: Cookies) -> impl IntoResponse {
    if authed(&cookies, &app).is_none() {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    // Перезагрузка ленивая: будим планировщик, он перечитает файл сам.
    match std::fs::read_to_string(&app.cfg.schedule) {
        Ok(_) => {
            app.wake.notify_one();
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    }
}
