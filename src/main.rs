mod auth;
mod config;
mod executor;
mod jobs;
mod scheduler;
mod store;
mod web;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use tokio::sync::{Notify, RwLock, Semaphore};

#[derive(Parser)]
#[command(
    name = "cyclo-agent",
    about = "Daemon for Cyclorithm schedules (web UI optional)"
)]
struct Cli {
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    #[command(subcommand)]
    cmd: Cmd,
}

/// Разложить путь до файла на (directory, file).
fn split_schedule(p: PathBuf) -> (PathBuf, String) {
    let file = p
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("schedule.cyclo")
        .to_owned();
    let dir = p
        .parent()
        .filter(|d| !d.as_os_str().is_empty())
        .map(|d| d.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));
    (dir, file)
}

#[derive(Subcommand)]
enum Cmd {
    /// Запустить планировщик (без --web — без веб-морды).
    Run {
        /// Путь до файла расписания (исполняемый файл + база для use).
        #[arg(long)]
        schedule: Option<PathBuf>,
        /// Рабочая директория расписания (перекрывает каталог из --schedule).
        #[arg(long)]
        directory: Option<PathBuf>,
        #[arg(long)]
        db: Option<PathBuf>,
        /// Адрес веб-морды, например 127.0.0.1:8080. Без флага — только демон.
        #[arg(long)]
        web: Option<String>,
        /// Файл дашбордов конструктора (дефолт — рядом с БД).
        #[arg(long)]
        dashboards: Option<PathBuf>,
    },
    /// Сбросить сессии (таблица sessions чистится целиком).
    ResetAuth {
        #[arg(long)]
        db: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::ResetAuth { db } => {
            let mut cfg = config::Config::load(cli.config);
            if let Some(v) = db {
                cfg.db = v;
            }
            let store = store::Store::open(&cfg.db).expect("open db");
            store.clear_sessions();
            println!("sessions cleared; delete users row in agent.db to re-run setup");
        }
        Cmd::Run {
            schedule,
            directory,
            db,
            web,
            dashboards,
        } => {
            let mut cfg = config::Config::load(cli.config);
            if let Some(v) = schedule {
                let (dir, file) = split_schedule(v);
                // Голое имя без каталога — файл в directory (флаг/конфиг/текущая).
                if dir.as_os_str() == "." && directory.is_none() {
                    cfg.schedule_file = file;
                } else {
                    cfg.directory = dir;
                    cfg.schedule_file = file;
                }
            }
            if let Some(v) = directory {
                cfg.directory = v;
            }
            if let Some(v) = db {
                cfg.db = v;
                // БД сменили, а дашборды явно не заданы — кладём рядом с новой БД.
                if dashboards.is_none() && !cfg.dashboards_explicit {
                    if let Some(dir) = cfg.db.parent() {
                        cfg.dashboards = dir.join("dashboards.json");
                    }
                }
            }
            if let Some(v) = dashboards {
                cfg.dashboards = v;
                cfg.dashboards_explicit = true;
            }
            if web.is_some() {
                cfg.web = web;
            }
            run_daemon(cfg).await;
        }
    }
}

async fn run_daemon(cfg: config::Config) {
    let store = store::Store::open(&cfg.db).expect("open db");
    // Дефолтное расписание-пример, чтобы первый запуск был живым (один файл, без либ).
    let sched = cfg.schedule_path();
    if std::fs::read_to_string(&sched).is_err() {
        if let Some(dir) = sched.parent() {
            if !dir.as_os_str().is_empty() {
                let _ = std::fs::create_dir_all(dir);
            }
        }
        let _ = std::fs::write(&sched, include_str!("../examples/schedule.cyclo"));
    }
    let shared = Arc::new(RwLock::new(cfg.clone()));
    let status = Arc::new(RwLock::new(scheduler::SchedStatus::default()));
    let sem = Arc::new(Semaphore::new(cfg.concurrency));
    let wake = Arc::new(Notify::new());
    let sched_watch = sched.clone();
    tokio::spawn(scheduler::run_loop(
        shared.clone(),
        store.clone(),
        status.clone(),
        sem,
        wake.clone(),
    ));
    tokio::spawn(scheduler::watch_file(sched_watch, wake.clone()));
    // SIGHUP — мгновенное пробуждение планировщика (конфиг/расписание перечитаются сами).
    #[cfg(unix)]
    tokio::spawn({
        let wake = wake.clone();
        async move {
            let mut hup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
                .expect("sighup");
            loop {
                hup.recv().await;
                wake.notify_one();
            }
        }
    });
    match cfg.web.clone() {
        None => {
            tracing::info!("no web UI (run with --web ADDR to enable)");
            std::future::pending::<()>().await;
        }
        Some(addr) => {
            let app = web::App {
                store,
                cfg: cfg.clone(),
                shared: shared.clone(),
                status,
                login_fails: Arc::new(std::sync::Mutex::new(HashMap::new())),
            };
            let listener = tokio::net::TcpListener::bind(&addr).await.expect("bind");
            tracing::info!("web UI on {addr}");
            axum::serve(
                listener,
                web::router(app).into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .await
            .expect("serve");
        }
    }
}
