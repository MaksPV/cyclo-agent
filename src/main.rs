mod auth;
mod config;
mod executor;
mod jobs;
mod scheduler;
mod store;
mod web;

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use tokio::sync::{Notify, RwLock, Semaphore};

#[derive(Parser)]
#[command(
    name = "cyclo-agent",
    about = "Daemon + web UI for Cyclorithm schedules"
)]
struct Cli {
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long)]
    schedule: Option<PathBuf>,
    #[arg(long)]
    db: Option<PathBuf>,
    #[arg(long)]
    listen: Option<String>,
    #[command(subcommand)]
    cmd: Option<Cmd>,
}

#[derive(clap::Subcommand)]
enum Cmd {
    /// Reset auth: drop users + sessions (needs FS access to db).
    ResetAuth,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    let mut cfg = config::Config::load(cli.config);
    if let Some(v) = cli.schedule {
        cfg.schedule = v;
    }
    if let Some(v) = cli.db {
        cfg.db = v;
    }
    if let Some(v) = cli.listen {
        cfg.listen = v;
    }
    let store = store::Store::open(&cfg.db).expect("open db");
    if matches!(cli.cmd, Some(Cmd::ResetAuth)) {
        store.clear_sessions();
        // Удаляем пользователей тоже, чтобы /setup открылся заново.
        println!("sessions cleared; delete users row in agent.db to re-run setup");
        return;
    }
    // Дефолтное расписание-пример, чтобы первый запуск был живым.
    if std::fs::read_to_string(&cfg.schedule).is_err() {
        if let Some(dir) = cfg.schedule.parent() {
            if !dir.as_os_str().is_empty() {
                let _ = std::fs::create_dir_all(dir);
            }
        }
        let _ = std::fs::write(&cfg.schedule, include_str!("../examples/schedule.cyclo"));
    }
    let status = Arc::new(RwLock::new(scheduler::SchedStatus::default()));
    let sem = Arc::new(Semaphore::new(cfg.concurrency));
    let wake = Arc::new(Notify::new());
    let app = web::App {
        store: store.clone(),
        cfg: cfg.clone(),
        status: status.clone(),
        wake: wake.clone(),
    };
    tokio::spawn(scheduler::run_loop(
        cfg.clone(),
        store,
        status,
        sem,
        wake.clone(),
    ));
    tokio::spawn(scheduler::watch_file(cfg.schedule.clone(), wake.clone()));
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
    let listener = tokio::net::TcpListener::bind(&cfg.listen)
        .await
        .expect("bind");
    tracing::info!("listening on {}", cfg.listen);
    axum::serve(listener, web::router(app))
        .await
        .expect("serve");
}
