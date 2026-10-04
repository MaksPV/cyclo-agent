use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{ConnectInfo, Query, State};
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
    /// Делимый конфиг: планировщик читает retention-лимиты без рестарта.
    pub shared: Arc<tokio::sync::RwLock<Config>>,
    pub status: Arc<RwLock<SchedStatus>>,
    /// Неудачные логины по IP для rate limit.
    pub login_fails: Arc<std::sync::Mutex<HashMap<std::net::IpAddr, Vec<std::time::Instant>>>>,
}

const COOKIE: &str = "cyclo_session";
const SESSION_TTL_SECS: i64 = 12 * 3600;
/// Максимум тела расписания (POST schedule/validate): расписания больше не бывают.
const MAX_SCHEDULE_BYTES: usize = 1_000_000;
/// Окно /api/next: больше месяца за раз не разворачиваем (CPU-DoS).
const MAX_WITHIN_SECS: i64 = 30 * 86_400;
/// Логин: 5 неудач с IP за минуту — дальше 429.
const LOGIN_FAILS: usize = 5;
const LOGIN_WINDOW_SECS: u64 = 60;

fn authed(cookies: &Cookies, app: &App) -> Option<i64> {
    let raw = cookies.get(COOKIE)?.value().to_owned();
    app.store.session_user(&hash_token(&raw), now_ms())
}

fn session_cookie(raw: String) -> Cookie<'static> {
    Cookie::build((COOKIE, raw))
        .http_only(true)
        .path("/")
        .same_site(cookie::SameSite::Lax)
        .build()
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
        .route("/api/password", axum::routing::post(password))
        .route("/api/series", axum::routing::get(series))
        .route("/api/meta", axum::routing::get(meta))
        .route(
            "/api/config",
            axum::routing::get(get_config).post(save_config),
        )
        .route(
            "/api/files",
            axum::routing::get(get_file)
                .post(save_file)
                .delete(delete_file),
        )
        .route(
            "/api/dashboards",
            axum::routing::get(get_dashboards).post(save_dashboards),
        )
        .route("/healthz", axum::routing::get(|| async { "ok" }))
        .layer(CookieManagerLayer::new())
        .with_state(app)
}

async fn index() -> Html<&'static str> {
    Html(include_str!("../static/index.html"))
}

async fn status(State(app): State<App>, cookies: Cookies) -> impl IntoResponse {
    let setup_required = !app.store.has_users();
    let Some(uid) = authed(&cookies, &app) else {
        // Анониму — только то, что нужно стартовому экрану (без путей сервера).
        return Json(serde_json::json!({
            "setup_required": setup_required,
            "authenticated": false,
        }));
    };
    let st = app.status.read().await;
    Json(serde_json::json!({
        "setup_required": setup_required,
        "authenticated": true,
        "login": app.store.login_of(uid),
        "schedule_valid": st.valid,
        "schedule_error": st.error,
        "schedule_name": st.name,
        "schedule_path": app.cfg.schedule_path().to_string_lossy(),
        "directory": app.cfg.directory.to_string_lossy(),
        "schedule_file": app.cfg.schedule_file,
        "concurrency": app.cfg.concurrency,
        "lookahead_secs": app.cfg.lookahead_secs,
        "retention_days": app.cfg.retention_days,
        "retention_max_rows": app.cfg.retention_max_rows,
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
    cookies.add(session_cookie(raw));
    StatusCode::CREATED.into_response()
}

#[derive(serde::Deserialize)]
struct LoginBody {
    login: String,
    password: String,
}

fn login_allowed(app: &App, ip: std::net::IpAddr) -> bool {
    let mut map = app.login_fails.lock().expect("login fails");
    let now = std::time::Instant::now();
    let window = std::time::Duration::from_secs(LOGIN_WINDOW_SECS);
    let fails = map.entry(ip).or_default();
    fails.retain(|t| now.duration_since(*t) < window);
    fails.len() < LOGIN_FAILS
}

fn login_failed(app: &App, ip: std::net::IpAddr) {
    app.login_fails
        .lock()
        .expect("login fails")
        .entry(ip)
        .or_default()
        .push(std::time::Instant::now());
}

async fn login(
    State(app): State<App>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    cookies: Cookies,
    Json(b): Json<LoginBody>,
) -> impl IntoResponse {
    if !app.store.has_users() {
        return (StatusCode::CONFLICT, "setup required".to_owned()).into_response();
    }
    if !login_allowed(&app, addr.ip()) {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            "too many attempts, try later".to_owned(),
        )
            .into_response();
    }
    let uid = app
        .store
        .user_hash(&b.login)
        .filter(|(_, hash)| verify_password(&b.password, hash))
        .map(|(id, _)| id);
    let Some(uid) = uid else {
        login_failed(&app, addr.ip());
        return (StatusCode::UNAUTHORIZED, "bad login".to_owned()).into_response();
    };
    let (raw, digest) = new_session_token();
    app.store
        .create_session(&digest, uid, now_ms(), SESSION_TTL_SECS);
    cookies.add(session_cookie(raw));
    StatusCode::NO_CONTENT.into_response()
}

async fn logout(State(app): State<App>, cookies: Cookies) -> impl IntoResponse {
    if let Some(raw) = cookies.get(COOKIE).map(|c| c.value().to_owned()) {
        app.store.delete_session(&hash_token(&raw));
        cookies.remove(Cookie::build((COOKIE, "")).path("/").build());
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
    Json(
        serde_json::to_value(
            app.store
                .list_runs(from, to, q.limit.unwrap_or(200).clamp(1, 1000)),
        )
        .unwrap(),
    )
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
        .clamp(1, MAX_WITHIN_SECS)
        * 1000;
    match next_jobs(
        &app.cfg.schedule_path(),
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
    match std::fs::read_to_string(app.cfg.schedule_path()) {
        Ok(t) => Json(serde_json::json!({"content": t})).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    }
}

#[derive(serde::Deserialize)]
struct PasswordBody {
    current: String,
    new: String,
    confirm: String,
}

#[derive(serde::Deserialize)]
struct ScheduleBody {
    content: String,
}

#[derive(serde::Deserialize)]
struct SeriesQuery {
    tag: Option<String>,
    job: Option<String>,
    kind: Option<String>,
    metric: Option<String>,
    agg: Option<String>,
    from: Option<i64>,
    to: Option<i64>,
    bucket_secs: Option<i64>,
    /// Способ: raw — точки запусков, bucket — усреднение по шагу (дефолт).
    mode: Option<String>,
}

/// Число по пути метрики: встроенные (latency_ms, up, exit_code) и любой
/// числовой путь в result (`result.status_code`, ...). Bool — 0/1.
/// Нет пути — None (пустая серия, не ошибка: поле могли удалить).
fn metric_value(run: &crate::store::Run, metric: &str) -> Option<f64> {
    match metric {
        "latency_ms" => Some(run.latency_ms as f64),
        "up" => Some(if run.status == "ok" { 1.0 } else { 0.0 }),
        "exit_code" => run.exit_code.map(|v| v as f64),
        _ => {
            let path = metric.strip_prefix("result.")?;
            let v: serde_json::Value = serde_json::from_str(&run.result).ok()?;
            let mut cur = &v;
            for part in path.split('.') {
                cur = cur.get(part)?;
            }
            match cur {
                serde_json::Value::Number(n) => n.as_f64(),
                serde_json::Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
                _ => None,
            }
        }
    }
}

fn tags_of(run: &crate::store::Run) -> Vec<String> {
    serde_json::from_str(&run.tags).unwrap_or_default()
}

/// Настройки: чтение. restart_required — ключи, требующие рестарта демона.
async fn get_config(State(app): State<App>, cookies: Cookies) -> impl IntoResponse {
    if authed(&cookies, &app).is_none() {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let cfg = app.shared.read().await.clone();
    Json(serde_json::json!({
        "directory": cfg.directory.to_string_lossy(),
        "schedule_file": cfg.schedule_file,
        "retention_days": cfg.retention_days,
        "retention_max_rows": cfg.retention_max_rows,
        "concurrency": cfg.concurrency,
        "lookahead_secs": cfg.lookahead_secs,
        "config_source": cfg.config_source.map(|p| p.to_string_lossy().into_owned()),
        "restart_required": ["directory", "schedule_file", "concurrency"],
    }))
    .into_response()
}

#[derive(serde::Deserialize, Default)]
struct ConfigBody {
    directory: Option<String>,
    schedule_file: Option<String>,
    retention_days: Option<u64>,
    retention_max_rows: Option<u64>,
    concurrency: Option<usize>,
    lookahead_secs: Option<u64>,
}

/// Настройки: запись распознанных ключей в agent.toml + живое обновление.
/// retention-лимиты применяются без рестарта, остальное — после рестарта.
async fn save_config(
    State(app): State<App>,
    cookies: Cookies,
    Json(b): Json<ConfigBody>,
) -> impl IntoResponse {
    if authed(&cookies, &app).is_none() {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let mut cfg = app.shared.read().await.clone();
    if let Some(d) = b.directory {
        if d.is_empty() || d.len() > 1024 || d.contains('\0') {
            return (StatusCode::BAD_REQUEST, "bad directory".to_owned()).into_response();
        }
        cfg.directory = std::path::PathBuf::from(d);
    }
    if let Some(f) = b.schedule_file {
        if f.is_empty() || f.len() > 256 || f.contains(['/', '\\', '\0']) || !f.ends_with(".cyclo")
        {
            return (
                StatusCode::BAD_REQUEST,
                "schedule_file: имя .cyclo без путей".to_owned(),
            )
                .into_response();
        }
        cfg.schedule_file = f;
    }
    if let Some(v) = b.retention_days {
        cfg.retention_days = v.clamp(1, 3650);
    }
    if let Some(v) = b.retention_max_rows {
        cfg.retention_max_rows = v.clamp(1000, 100_000_000);
    }
    if let Some(v) = b.concurrency {
        cfg.concurrency = v.clamp(1, 64);
    }
    if let Some(v) = b.lookahead_secs {
        cfg.lookahead_secs = v.clamp(5, 3600);
    }
    // Пишем обратно в TOML: распознанные ключи, неизвестные сохраняем.
    let dest = cfg
        .config_source
        .clone()
        .unwrap_or_else(|| std::path::PathBuf::from("./agent.toml"));
    let mut doc: toml::Value = std::fs::read_to_string(&dest)
        .ok()
        .and_then(|t| t.parse().ok())
        .unwrap_or(toml::Value::Table(toml::map::Map::new()));
    if let Some(table) = doc.as_table_mut() {
        // Легаси-ключ schedule (полный путь) удаляем: живут directory+schedule_file,
        // иначе при рестарте он перетрёт directory.
        table.remove("schedule");
        table.insert(
            "directory".to_owned(),
            toml::Value::String(cfg.directory.to_string_lossy().into_owned()),
        );
        table.insert(
            "schedule_file".to_owned(),
            toml::Value::String(cfg.schedule_file.clone()),
        );
        table.insert(
            "retention_days".to_owned(),
            toml::Value::Integer(cfg.retention_days as i64),
        );
        table.insert(
            "retention_max_rows".to_owned(),
            toml::Value::Integer(cfg.retention_max_rows as i64),
        );
        table.insert(
            "concurrency".to_owned(),
            toml::Value::Integer(cfg.concurrency as i64),
        );
        table.insert(
            "lookahead_secs".to_owned(),
            toml::Value::Integer(cfg.lookahead_secs as i64),
        );
    }
    if let Some(dir) = dest.parent() {
        if !dir.as_os_str().is_empty() {
            let _ = std::fs::create_dir_all(dir);
        }
    }
    if let Err(e) = std::fs::write(&dest, toml::to_string(&doc).unwrap_or_default()) {
        return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
    }
    cfg.config_source = Some(dest);
    *app.shared.write().await = cfg;
    Json(serde_json::json!({"restart_required": ["directory", "schedule_file", "concurrency"]}))
        .into_response()
}

/// Корень файлов расписания: настроенная рабочая директория.
fn files_base(app: &App) -> std::path::PathBuf {
    app.cfg.directory.clone()
}

/// Относительный путь внутри базы без escapes: только .cyclo, без .. и абсолютных.
fn clean_rel(raw: &str) -> Result<std::path::PathBuf, String> {
    if raw.is_empty() || raw.len() > 512 {
        return Err("bad path".to_owned());
    }
    let p = std::path::PathBuf::from(raw);
    if p.is_absolute() {
        return Err("absolute path not allowed".to_owned());
    }
    if p.components()
        .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        return Err(".. not allowed".to_owned());
    }
    if p.extension().and_then(|e| e.to_str()) != Some("cyclo") {
        return Err("only .cyclo files".to_owned());
    }
    Ok(p)
}

#[derive(serde::Deserialize)]
struct FileQuery {
    path: Option<String>,
}

/// GET /api/files → {main, files[]} | ?path=rel → {path, content}.
async fn get_file(
    State(app): State<App>,
    cookies: Cookies,
    Query(q): Query<FileQuery>,
) -> impl IntoResponse {
    if authed(&cookies, &app).is_none() {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let base = files_base(&app);
    if let Some(rel) = q.path {
        let p = match clean_rel(&rel) {
            Ok(p) => base.join(p),
            Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
        };
        return match std::fs::read_to_string(&p) {
            Ok(t) => Json(serde_json::json!({"path": rel, "content": t})).into_response(),
            Err(e) => (StatusCode::NOT_FOUND, e.to_string()).into_response(),
        };
    }
    let mut out = vec![];
    let mut stack = vec![base.clone()];
    while let Some(dir) = stack.pop() {
        let rd = match std::fs::read_dir(&dir) {
            Ok(r) => r,
            Err(_) => continue,
        };
        for e in rd.flatten() {
            let path = e.path();
            if path.is_dir() {
                if out.len() < 500 {
                    stack.push(path);
                }
                continue;
            }
            if path.extension().and_then(|x| x.to_str()) != Some("cyclo") {
                continue;
            }
            if let Ok(rel) = path.strip_prefix(&base) {
                if out.len() < 500 {
                    out.push(rel.to_string_lossy().replace('\\', "/"));
                }
            }
        }
        if out.len() >= 500 {
            break;
        }
    }
    out.sort();
    Json(serde_json::json!({"main": app.cfg.schedule_file, "files": out})).into_response()
}

#[derive(serde::Deserialize)]
struct FileBody {
    path: String,
    content: String,
}

/// POST /api/files — сохранить (новый или существующий) файл в базе расписания.
async fn save_file(
    State(app): State<App>,
    cookies: Cookies,
    Json(b): Json<FileBody>,
) -> impl IntoResponse {
    if authed(&cookies, &app).is_none() {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if b.content.len() > MAX_SCHEDULE_BYTES {
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            "too large (max 1MB)".to_owned(),
        )
            .into_response();
    }
    let rel = match clean_rel(&b.path) {
        Ok(p) => p,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };
    let dest = files_base(&app).join(&rel);
    if let Some(dir) = dest.parent() {
        if !dir.as_os_str().is_empty() {
            let _ = std::fs::create_dir_all(dir);
        }
    }
    match std::fs::write(&dest, &b.content) {
        Ok(()) => Json(serde_json::json!({"path": rel.to_string_lossy()})).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// DELETE /api/files?path= — удалить файл (главный файл расписания — нельзя).
async fn delete_file(
    State(app): State<App>,
    cookies: Cookies,
    Query(q): Query<FileQuery>,
) -> impl IntoResponse {
    if authed(&cookies, &app).is_none() {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Some(rel) = q.path else {
        return (StatusCode::BAD_REQUEST, "need ?path=".to_owned()).into_response();
    };
    let p = match clean_rel(&rel) {
        Ok(p) => p,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };
    let dest = files_base(&app).join(&p);
    if same_file(&dest, &app.cfg.schedule_path()) {
        return (
            StatusCode::FORBIDDEN,
            "cannot delete main schedule".to_owned(),
        )
            .into_response();
    }
    match std::fs::remove_file(&dest) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    }
}

fn same_file(a: &std::path::Path, b: &std::path::Path) -> bool {
    match (a.canonicalize(), b.canonicalize()) {
        (Ok(x), Ok(y)) => x == y,
        _ => a == b,
    }
}

/// Что реально есть в БД: для выпадающих списков конструктора.
/// Сканируем свежие запуски (первые 20k): теги, виды, джобы, числовые метрики.
async fn meta(State(app): State<App>, cookies: Cookies) -> impl IntoResponse {
    if authed(&cookies, &app).is_none() {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let now = now_ms();
    let mut tags: std::collections::BTreeSet<String> = Default::default();
    let mut kinds: std::collections::BTreeSet<String> = Default::default();
    let mut jobs: std::collections::BTreeSet<String> = Default::default();
    let mut metrics: std::collections::BTreeSet<String> = Default::default();
    // Свежие first: series_points отдаёт DESC, берём первые 20k.
    for r in app
        .store
        .series_points(now - 30 * 86_400_000, now)
        .into_iter()
        .take(20_000)
    {
        kinds.insert(r.kind.clone());
        if jobs.len() < 1000 {
            jobs.insert(r.cmd.clone());
        }
        for t in tags_of(&r) {
            tags.insert(t);
        }
        metrics.insert("latency_ms".to_owned());
        metrics.insert("up".to_owned());
        if r.exit_code.is_some() {
            metrics.insert("exit_code".to_owned());
        }
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&r.result) {
            if let Some(m) = v.as_object() {
                for (k, x) in m {
                    if matches!(x, serde_json::Value::Number(_) | serde_json::Value::Bool(_)) {
                        metrics.insert(format!("result.{k}"));
                    }
                }
            }
        }
    }
    Json(serde_json::json!({
        "tags": tags.into_iter().collect::<Vec<_>>(),
        "kinds": kinds.into_iter().collect::<Vec<_>>(),
        "jobs": jobs.into_iter().take(1000).collect::<Vec<_>>(),
        "metrics": metrics.into_iter().collect::<Vec<_>>(),
    }))
    .into_response()
}

fn agg_value(agg: &str, mut xs: Vec<f64>) -> Option<f64> {
    if xs.is_empty() {
        return None;
    }
    match agg {
        "min" => xs.iter().cloned().reduce(f64::min),
        "max" => xs.iter().cloned().reduce(f64::max),
        "count" => Some(xs.len() as f64),
        "p50" | "p99" => {
            xs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let rank = if agg == "p50" { 0.5 } else { 0.99 };
            let i = ((rank * xs.len() as f64).ceil() as usize).saturating_sub(1);
            xs.get(i).copied()
        }
        _ => Some(xs.iter().sum::<f64>() / xs.len() as f64), // avg и неизвестное
    }
}

async fn series(
    State(app): State<App>,
    cookies: Cookies,
    Query(q): Query<SeriesQuery>,
) -> impl IntoResponse {
    if authed(&cookies, &app).is_none() {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let now = now_ms();
    let to = q.to.unwrap_or(now);
    let from = q.from.unwrap_or(to - 24 * 3_600_000);
    if to <= from || to - from > 90 * 86_400_000 {
        return (StatusCode::BAD_REQUEST, "bad range (max 90d)".to_owned()).into_response();
    }
    let metric = q.metric.unwrap_or_else(|| "latency_ms".to_owned());
    let agg = q.agg.unwrap_or_else(|| "avg".to_owned());
    // Способ: raw — каждый запуск своей точкой в точное время, bucket — усреднение
    // по шагу. Без параметра — bucket (старое поведение, ничего не ломается).
    let mode = q.mode.unwrap_or_else(|| "bucket".to_owned());
    if mode != "raw" && mode != "bucket" {
        return (StatusCode::BAD_REQUEST, "bad mode (raw|bucket)".to_owned()).into_response();
    }
    // Фильтр общий для обоих способов.
    let mut runs: Vec<(i64, f64)> = vec![];
    for r in app.store.series_points(from, to) {
        if let Some(tag) = &q.tag {
            if !tags_of(&r).iter().any(|t| t == tag) {
                continue;
            }
        }
        if let Some(job) = &q.job {
            if !r.cmd.contains(job.as_str()) {
                continue;
            }
        }
        if let Some(kind) = &q.kind {
            if r.kind != *kind {
                continue;
            }
        }
        if let Some(v) = metric_value(&r, &metric) {
            runs.push((r.scheduled_at, v));
        }
    }
    if mode == "raw" {
        // Честные точки: больше 5000 на экран не влезет — просим включить «по шагу».
        if runs.len() > 5000 {
            return (
                StatusCode::BAD_REQUEST,
                format!(
                    "too many points ({} > 5000): включи «по шагу» или уменьши окно",
                    runs.len()
                ),
            )
                .into_response();
        }
        runs.sort_by_key(|(t, _)| *t);
        let points: Vec<serde_json::Value> = runs
            .iter()
            .map(|(t, v)| serde_json::json!({"t": t, "v": v}))
            .collect();
        let count = points.len();
        return Json(serde_json::json!({
            "metric": metric, "mode": "raw", "count": count, "points": points,
        }))
        .into_response();
    }
    // Шаг: явный bucket_secs, иначе авто (окно/200). Бакетов не больше 2000 —
    // иначе укрупняем (фактический шаг виден в ответе и в подписи графика).
    let mut bucket_ms = q.bucket_secs.unwrap_or(0).max(0) * 1000;
    if bucket_ms <= 0 {
        bucket_ms = ((to - from) / 200).max(1000);
    }
    // Бакетов не больше 2000 — иначе укрупняем.
    let n = ((to - from) / bucket_ms) as usize + 1;
    if n > 2000 {
        bucket_ms = (to - from) / 2000 + 1;
    }
    let mut buckets: Vec<Vec<f64>> = vec![Vec::new(); ((to - from) / bucket_ms) as usize + 1];
    for (t, v) in runs {
        let i = ((t - from) / bucket_ms) as usize;
        if let Some(b) = buckets.get_mut(i) {
            b.push(v);
        }
    }
    let points: Vec<serde_json::Value> = buckets
        .into_iter()
        .enumerate()
        .map(|(i, xs)| {
            serde_json::json!({"t": from + i as i64 * bucket_ms, "v": agg_value(&agg, xs)})
        })
        .collect();
    Json(serde_json::json!({
        "metric": metric, "agg": agg, "mode": "bucket",
        "bucket_secs": bucket_ms / 1000, "points": points,
    }))
    .into_response()
}

fn default_dashboards() -> serde_json::Value {
    serde_json::json!({
        "version": 1,
        "charts": [
            {"title": "latency (все)", "metric": "latency_ms", "agg": "p50",
             "type": "line", "window_secs": 86400, "bucket_secs": 300, "mode": "raw"},
            {"title": "up (все)", "metric": "up", "agg": "avg",
             "type": "dots", "window_secs": 86400, "bucket_secs": 300, "mode": "raw"},
        ],
    })
}

/// Проверка формы дашбордов конструктора (версию и поля — строго, иначе 400).
fn check_dashboards(v: &serde_json::Value) -> Result<(), String> {
    let version = v.get("version").and_then(|x| x.as_u64()).unwrap_or(0);
    if version != 1 {
        return Err("need {\"version\": 1, ...}".to_owned());
    }
    let charts = v
        .get("charts")
        .and_then(|x| x.as_array())
        .ok_or("need charts[]")?;
    if charts.len() > 100 {
        return Err("too many charts (max 100)".to_owned());
    }
    for c in charts {
        let title = c.get("title").and_then(|x| x.as_str()).unwrap_or("");
        if title.is_empty() || title.len() > 200 {
            return Err("chart needs title ≤200".to_owned());
        }
        if c.get("metric")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .is_empty()
        {
            return Err("chart needs metric".to_owned());
        }
        match c.get("agg").and_then(|x| x.as_str()).unwrap_or("avg") {
            "avg" | "min" | "max" | "p50" | "p99" | "count" => {}
            other => return Err(format!("bad agg '{other}'")),
        }
        match c.get("type").and_then(|x| x.as_str()).unwrap_or("line") {
            "line" | "dots" | "bars" => {}
            other => return Err(format!("bad type '{other}'")),
        }
        let window = c.get("window_secs").and_then(|x| x.as_i64()).unwrap_or(0);
        if !(1..=30 * 86_400).contains(&window) {
            return Err("window_secs 1..2592000".to_owned());
        }
        // mode опционален (старые файлы без него — bucket, ничего не ломается).
        match c.get("mode").and_then(|x| x.as_str()).unwrap_or("bucket") {
            "raw" | "bucket" => {}
            other => return Err(format!("bad mode '{other}'")),
        }
        let bucket = c.get("bucket_secs").and_then(|x| x.as_i64()).unwrap_or(300);
        // bucket_secs опционален (нет — дефолт 300); при способе «запуски» не используется.
        if !(1..=86_400).contains(&bucket) {
            return Err("bucket_secs 1..86400".to_owned());
        }
    }
    Ok(())
}

async fn get_dashboards(State(app): State<App>, cookies: Cookies) -> impl IntoResponse {
    if authed(&cookies, &app).is_none() {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match std::fs::read_to_string(&app.cfg.dashboards) {
        Ok(t) => match serde_json::from_str::<serde_json::Value>(&t) {
            Ok(v) => Json(v).into_response(),
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        },
        Err(_) => Json(default_dashboards()).into_response(),
    }
}

#[derive(serde::Deserialize)]
struct DashboardsBody {
    content: String,
}

async fn save_dashboards(
    State(app): State<App>,
    cookies: Cookies,
    Json(b): Json<DashboardsBody>,
) -> impl IntoResponse {
    if authed(&cookies, &app).is_none() {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if b.content.len() > 256_000 {
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            "too large (max 256KB)".to_owned(),
        )
            .into_response();
    }
    let v: serde_json::Value = match serde_json::from_str(&b.content) {
        Ok(v) => v,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    if let Err(e) = check_dashboards(&v) {
        return (StatusCode::BAD_REQUEST, e).into_response();
    }
    if let Some(dir) = app.cfg.dashboards.parent() {
        if !dir.as_os_str().is_empty() {
            let _ = std::fs::create_dir_all(dir);
        }
    }
    match std::fs::write(
        &app.cfg.dashboards,
        serde_json::to_string_pretty(&v).unwrap(),
    ) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn password(
    State(app): State<App>,
    cookies: Cookies,
    Json(b): Json<PasswordBody>,
) -> impl IntoResponse {
    let Some(uid) = authed(&cookies, &app) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let raw = cookies
        .get(COOKIE)
        .map(|c| c.value().to_owned())
        .unwrap_or_default();
    let Some((_, hash)) = app
        .store
        .login_of(uid)
        .and_then(|l| app.store.user_hash(&l))
    else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    if !verify_password(&b.current, &hash) {
        return (StatusCode::FORBIDDEN, "bad current password".to_owned()).into_response();
    }
    if b.new.len() < 8 {
        return (
            StatusCode::BAD_REQUEST,
            "password too short (min 8)".to_owned(),
        )
            .into_response();
    }
    if b.new != b.confirm {
        return (StatusCode::BAD_REQUEST, "passwords do not match".to_owned()).into_response();
    }
    let hash = match hash_password(&b.new) {
        Ok(h) => h,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    };
    if !app.store.update_password(uid, &hash) {
        return (StatusCode::INTERNAL_SERVER_ERROR, "no such user".to_owned()).into_response();
    }
    // Чужие сессии отваливаются, текущая живёт.
    app.store.delete_other_sessions(uid, &hash_token(&raw));
    StatusCode::NO_CONTENT.into_response()
}

fn check_content(content: &str, schedule_path: &std::path::Path) -> Result<(), String> {
    if content.len() > MAX_SCHEDULE_BYTES {
        return Err("schedule too large (max 1MB)".to_owned());
    }
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
    if let Err(e) = check_content(&b.content, &app.cfg.schedule_path()) {
        let code = if e.starts_with("schedule too large") {
            StatusCode::PAYLOAD_TOO_LARGE
        } else {
            StatusCode::BAD_REQUEST
        };
        return (code, e).into_response();
    }
    if let Some(dir) = app.cfg.schedule_path().parent().map(|d| d.to_path_buf()) {
        if !dir.as_os_str().is_empty() {
            let _ = std::fs::create_dir_all(dir);
        }
    }
    match std::fs::write(app.cfg.schedule_path(), &b.content) {
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
    match check_content(&b.content, &app.cfg.schedule_path()) {
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
