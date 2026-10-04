use std::path::Path;
use std::sync::{Arc, Mutex};

use rusqlite::{params, Connection};

#[derive(Debug, Clone, serde::Serialize)]
pub struct Run {
    pub id: i64,
    pub job_key: String,
    pub scheduled_at: i64,
    pub started_at: i64,
    pub finished_at: i64,
    pub status: String,
    pub kind: String,
    pub tags: String,
    pub cmd: String,
    pub args: String,
    pub exit_code: Option<i64>,
    pub latency_ms: i64,
    pub out_tail: String,
    pub err_tail: String,
    pub result: String,
}

#[derive(Clone)]
pub struct Store {
    inner: Arc<Mutex<Connection>>,
}

impl Store {
    pub fn open(path: &Path) -> Result<Self, String> {
        if let Some(dir) = path.parent() {
            if !dir.as_os_str().is_empty() {
                let _ = std::fs::create_dir_all(dir);
            }
        }
        let conn = Connection::open(path).map_err(|e| e.to_string())?;
        // Ранняя разработка, миграций нет: старую таблицу runs без колонки
        // kind сносим целиком (users/sessions не трогаем).
        let needs_runs: bool = conn
            .prepare("SELECT kind FROM runs LIMIT 0")
            .map(|_| false)
            .unwrap_or(true);
        if needs_runs {
            conn.execute_batch(
                "DROP TABLE IF EXISTS runs;
                 CREATE TABLE runs(
                  id INTEGER PRIMARY KEY AUTOINCREMENT, job_key TEXT UNIQUE,
                  scheduled_at INTEGER, started_at INTEGER, finished_at INTEGER,
                  status TEXT, kind TEXT, tags TEXT, cmd TEXT, args TEXT,
                  exit_code INTEGER NULL, latency_ms INTEGER,
                  out_tail TEXT, err_tail TEXT, result TEXT);
                 CREATE INDEX IF NOT EXISTS idx_runs_time ON runs(scheduled_at);
                 CREATE INDEX IF NOT EXISTS idx_runs_status_time ON runs(status, scheduled_at);
                 CREATE INDEX IF NOT EXISTS idx_runs_kind_time ON runs(kind, scheduled_at);",
            )
            .map_err(|e| e.to_string())?;
        }
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS users(
              id INTEGER PRIMARY KEY AUTOINCREMENT, login TEXT UNIQUE,
              password_hash TEXT, created_at INTEGER);
             CREATE TABLE IF NOT EXISTS sessions(
              token_hash TEXT PRIMARY KEY, user_id INTEGER,
              created_at INTEGER, expires_at INTEGER);
             CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);",
        )
        .map_err(|e| e.to_string())?;
        Ok(Self {
            inner: Arc::new(Mutex::new(conn)),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.inner.lock().expect("db lock")
    }

    pub fn has_users(&self) -> bool {
        self.lock()
            .query_row("SELECT COUNT(*) FROM users", [], |r| r.get::<_, i64>(0))
            .unwrap_or(0)
            > 0
    }

    pub fn create_user(&self, login: &str, hash: &str, now: i64) -> Result<(), String> {
        self.lock()
            .execute(
                "INSERT INTO users(login, password_hash, created_at) VALUES(?,?,?)",
                params![login, hash, now],
            )
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    pub fn user_hash(&self, login: &str) -> Option<(i64, String)> {
        self.lock()
            .query_row(
                "SELECT id, password_hash FROM users WHERE login=?",
                params![login],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .ok()
    }

    pub fn create_session(&self, token_hash: &str, user_id: i64, now: i64, ttl_secs: i64) {
        let _ = self.lock().execute(
            "INSERT INTO sessions(token_hash, user_id, created_at, expires_at) VALUES(?,?,?,?)",
            params![token_hash, user_id, now, now + ttl_secs * 1000],
        );
    }

    pub fn session_user(&self, token_hash: &str, now: i64) -> Option<i64> {
        self.lock()
            .query_row(
                "SELECT user_id FROM sessions WHERE token_hash=? AND expires_at>?",
                params![token_hash, now],
                |r| r.get(0),
            )
            .ok()
    }

    pub fn delete_session(&self, token_hash: &str) {
        let _ = self.lock().execute(
            "DELETE FROM sessions WHERE token_hash=?",
            params![token_hash],
        );
    }

    pub fn update_password(&self, user_id: i64, hash: &str) -> bool {
        self.lock()
            .execute(
                "UPDATE users SET password_hash=? WHERE id=?",
                params![hash, user_id],
            )
            .map(|n| n == 1)
            .unwrap_or(false)
    }

    pub fn delete_other_sessions(&self, user_id: i64, keep_hash: &str) {
        let _ = self.lock().execute(
            "DELETE FROM sessions WHERE user_id=? AND token_hash<>?",
            params![user_id, keep_hash],
        );
    }

    pub fn login_of(&self, user_id: i64) -> Option<String> {
        self.lock()
            .query_row(
                "SELECT login FROM users WHERE id=?",
                params![user_id],
                |r| r.get(0),
            )
            .ok()
    }

    pub fn delete_expired_sessions(&self, now: i64) {
        let _ = self
            .lock()
            .execute("DELETE FROM sessions WHERE expires_at<?", params![now]);
    }

    pub fn clear_sessions(&self) {
        let _ = self.lock().execute("DELETE FROM sessions", []);
    }

    pub fn has_job_key(&self, key: &str) -> bool {
        self.lock()
            .query_row(
                "SELECT 1 FROM runs WHERE job_key=?",
                rusqlite::params![key],
                |_| Ok(()),
            )
            .is_ok()
    }

    pub fn insert_run(&self, r: &Run) -> bool {
        self.lock()
            .execute(
                "INSERT OR IGNORE INTO runs(job_key, scheduled_at, started_at, finished_at,
                 status, kind, tags, cmd, args, exit_code, latency_ms, out_tail, err_tail, result)
                 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                params![
                    r.job_key,
                    r.scheduled_at,
                    r.started_at,
                    r.finished_at,
                    r.status,
                    r.kind,
                    r.tags,
                    r.cmd,
                    r.args,
                    r.exit_code,
                    r.latency_ms,
                    r.out_tail,
                    r.err_tail,
                    r.result
                ],
            )
            .map(|n| n == 1)
            .unwrap_or(false)
    }

    pub fn list_runs(&self, from: i64, to: i64, limit: i64) -> Vec<Run> {
        let conn = self.lock();
        let mut st = match conn.prepare(
            "SELECT id, job_key, scheduled_at, started_at, finished_at, status,
             kind, tags, cmd, args, exit_code, latency_ms, out_tail, err_tail, result
             FROM runs WHERE scheduled_at>=? AND scheduled_at<? ORDER BY scheduled_at DESC LIMIT ?",
        ) {
            Ok(s) => s,
            Err(_) => return vec![],
        };
        st.query_map(params![from, to, limit.clamp(1, 100_000)], |r| {
            Ok(Run {
                id: r.get(0)?,
                job_key: r.get(1)?,
                scheduled_at: r.get(2)?,
                started_at: r.get(3)?,
                finished_at: r.get(4)?,
                status: r.get(5)?,
                kind: r.get(6)?,
                tags: r.get(7)?,
                cmd: r.get(8)?,
                args: r.get(9)?,
                exit_code: r.get(10)?,
                latency_ms: r.get(11)?,
                out_tail: r.get(12)?,
                err_tail: r.get(13)?,
                result: r.get(14)?,
            })
        })
        .map(|it| it.filter_map(|x| x.ok()).collect())
        .unwrap_or_default()
    }

    pub fn cleanup(&self, older_than_ms: i64) {
        let _ = self.lock().execute(
            "DELETE FROM runs WHERE scheduled_at<?",
            params![older_than_ms],
        );
    }

    /// Точки для серий: всё окно разом (лимит 100k), фильтры — в вызывателе.
    pub fn series_points(&self, from: i64, to: i64) -> Vec<Run> {
        self.list_runs(from, to, 100_000)
    }
}
