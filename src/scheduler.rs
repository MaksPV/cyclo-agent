use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::{Notify, RwLock, Semaphore};

use crate::config::Config;
use crate::jobs::to_job;
use crate::store::Store;

#[derive(Debug, Clone, Default)]
pub struct SchedStatus {
    pub valid: bool,
    pub error: String,
    pub name: String,
}

/// Опоздание, после которого событие не исполняем, а пишем `skipped`.
const LATE_MS: i64 = 60_000;
/// Как далеко ищем ближайшее событие.
const LOOKAHEAD_MS: i64 = 7 * 86_400_000;
/// Сколько ближайших тянем за раз. Пачка обязана покрывать все события,
/// due одновременно: ядро отдаёт топ-n по (time,k,seq) и не знает про уже
/// исполненные — меньшая пачка зациклилась бы на первых n навсегда.
const BATCH: usize = 50_000;
/// Пауза при пустом расписании / ошибке файла.
const IDLE_SECS: u64 = 60;

pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn read_schedule(path: &Path) -> Result<(String, PathBuf), String> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read schedule '{}': {e}", path.display()))?;
    let base = path
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));
    Ok((text, base))
}

fn file_mtime(path: &Path) -> Option<std::time::SystemTime> {
    std::fs::metadata(path).and_then(|m| m.modified()).ok()
}

/// Лёгкий вотчер: только stat файла, развёрток нет.
pub async fn watch_file(path: PathBuf, notify: Arc<Notify>) {
    let mut last = file_mtime(&path);
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        let cur = file_mtime(&path);
        if cur != last {
            last = cur;
            notify.notify_one();
        }
    }
}

pub async fn run_loop(
    cfg: Config,
    store: Store,
    status: Arc<RwLock<SchedStatus>>,
    sem: Arc<Semaphore>,
    wake: Arc<Notify>,
) {
    let mut last_cleanup = 0i64;
    // Пачка будущих событий, перенесённая через сон: после точного пробуждения
    // исполняем её без повторного запроса к ядру (экономим ~15мс) — свежесть
    // проверяем по mtime файла, дедуп — через UNIQUE(job_key).
    let mut pending: Vec<crate::jobs::Job> = vec![];
    let mut pending_mtime = file_mtime(&cfg.schedule);
    loop {
        let now = now_ms();
        let mtime = file_mtime(&cfg.schedule);
        // Окно с перекрытием 2с назад: сон срабатывает в T+мс, а `[now, …)`
        // событие ровно в T уже исключает.
        let from = now - 2_000;
        let due: Vec<crate::jobs::Job> = if mtime == pending_mtime && !pending.is_empty() {
            std::mem::take(&mut pending)
        } else {
            pending.clear();
            pending_mtime = mtime;
            match read_schedule(&cfg.schedule) {
                Err(e) => {
                    *status.write().await = SchedStatus {
                        valid: false,
                        error: e,
                        name: String::new(),
                    };
                    vec![]
                }
                Ok((text, base)) => {
                    match cyclorithm_core::pipeline::next_window(
                        &text,
                        &base,
                        from,
                        LOOKAHEAD_MS,
                        BATCH,
                        None,
                    ) {
                        Err(e) => {
                            *status.write().await = SchedStatus {
                                valid: false,
                                error: e.to_string(),
                                name: String::new(),
                            };
                            vec![]
                        }
                        Ok(win) => {
                            *status.write().await = SchedStatus {
                                valid: true,
                                error: String::new(),
                                name: win.schedule.clone(),
                            };
                            win.events
                                .iter()
                                .filter_map(|ev| {
                                    to_job(ev.time, &ev.point, &ev.action, &ev.action_attrs)
                                })
                                .collect()
                        }
                    }
                }
            }
        };
        let now = now_ms();
        // Исполняем всё, чьё время пришло (обычно 0–1 событие), будущее —
        // в pending через сон.
        let mut next_at: Option<i64> = None;
        for job in due {
            if job.time <= now {
                let key = crate::jobs::job_key(&job);
                if store.has_job_key(&key) {
                    continue; // уже исполнено/записано (перекрытие окна)
                }
                if now - job.time > LATE_MS {
                    let t = now_ms();
                    store.insert_run(&crate::store::Run {
                        id: 0,
                        job_key: key,
                        scheduled_at: job.time,
                        started_at: t,
                        finished_at: t,
                        status: "skipped".to_owned(),
                        cmd: job.cmd.clone(),
                        args: serde_json::to_string(&job.args).unwrap_or_default(),
                        exit_code: None,
                        latency_ms: 0,
                        out_tail: String::new(),
                        err_tail: "missed by more than 60s".to_owned(),
                    });
                } else {
                    let store2 = store.clone();
                    let sem2 = sem.clone();
                    tokio::spawn(async move {
                        let _p = sem2.acquire_owned().await;
                        let run = crate::executor::run_job(&job).await;
                        store2.insert_run(&run);
                    });
                }
            } else {
                if next_at.is_none_or(|t| job.time < t) {
                    next_at = Some(job.time);
                }
                pending.push(job);
            }
        }
        if now - last_cleanup > 3_600_000 {
            last_cleanup = now;
            let cutoff = now - cfg.retention_days as i64 * 86_400_000;
            store.cleanup(cutoff);
            store.delete_expired_sessions(now);
        }
        match next_at {
            // Есть будущее событие — спим до него (вотчер/SIGHUP/API разбудят раньше).
            // Точность: грубый сон до T-100мс, дальше дотягиваем короткими снами
            // и yield — пробуждение в пределах ±1мс от решётки.
            Some(t) => {
                let wait = (t - now_ms()).max(0) as u64;
                if wait > 150 {
                    tokio::select! {
                        () = tokio::time::sleep(std::time::Duration::from_millis(wait - 100)) => {}
                        () = wake.notified() => { continue; }
                    }
                }
                loop {
                    let left = t - now_ms();
                    if left <= 0 {
                        break;
                    }
                    if left > 5 {
                        tokio::time::sleep(std::time::Duration::from_millis(
                            (left - 2).min(20) as u64
                        ))
                        .await;
                    } else {
                        tokio::task::yield_now().await;
                    }
                }
            }
            // Нечего исполнять — короткая пауза (ошибка файла, пусто, всё due).
            None => {
                tokio::select! {
                    () = tokio::time::sleep(std::time::Duration::from_secs(IDLE_SECS)) => {}
                    () = wake.notified() => {}
                }
            }
        }
    }
}

/// Будущие запуски для /api/next: живьём из файла, без исполнения.
pub fn next_jobs(
    schedule: &Path,
    now: i64,
    within_ms: i64,
    n: usize,
) -> Result<Vec<serde_json::Value>, String> {
    let (text, base) = read_schedule(schedule)?;
    let win = cyclorithm_core::pipeline::next_window(&text, &base, now, within_ms, n, None)
        .map_err(|e| e.to_string())?;
    Ok(win
        .events
        .iter()
        .filter_map(|ev| {
            let j = to_job(ev.time, &ev.point, &ev.action, &ev.action_attrs)?;
            Some(serde_json::json!({
                "time": ev.time, "point": j.point, "action": j.action,
                "cmd": j.cmd, "args": j.args, "timeout_s": j.timeout_secs,
            }))
        })
        .collect())
}
