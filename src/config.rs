use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Config {
    /// Рабочая директория расписания (база для use и исполняемого файла).
    pub directory: PathBuf,
    /// Имя исполняемого файла в directory (дефолт schedule.cyclo).
    pub schedule_file: String,
    pub db: PathBuf,
    /// Адрес веб-морды; None — демон без веба.
    pub web: Option<String>,
    /// Файл дашбордов конструктора (дефолт — рядом с БД).
    pub dashboards: PathBuf,
    /// dashboards задан явно (файл/флаг), а не выведен из пути БД.
    pub dashboards_explicit: bool,
    pub poll_secs: u64,
    pub lookahead_secs: u64,
    pub concurrency: usize,
    pub retention_days: u64,
    pub retention_max_rows: u64,
    /// Откуда загружен конфиг (для записи настроек обратно).
    pub config_source: Option<PathBuf>,
}

impl Config {
    /// Полный путь исполняемого расписания.
    pub fn schedule_path(&self) -> PathBuf {
        self.directory.join(&self.schedule_file)
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            directory: PathBuf::from("/etc/cyclo-agent"),
            schedule_file: "schedule.cyclo".to_owned(),
            db: PathBuf::from("/var/lib/cyclo-agent/agent.db"),
            web: None,
            dashboards: PathBuf::from("/var/lib/cyclo-agent/dashboards.json"),
            dashboards_explicit: false,
            poll_secs: 10,
            lookahead_secs: 60,
            concurrency: 4,
            retention_days: 90,
            retention_max_rows: 1_000_000,
            config_source: None,
        }
    }
}

#[derive(Debug, Default, serde::Deserialize)]
struct FileCfg {
    /// Легаси: полный путь до файла (с сепараторами — режем на directory+file,
    /// голое имя — schedule_file).
    schedule: Option<PathBuf>,
    directory: Option<PathBuf>,
    schedule_file: Option<String>,
    db: Option<PathBuf>,
    web: Option<String>,
    dashboards: Option<PathBuf>,
    listen: Option<String>,
    poll_secs: Option<u64>,
    lookahead_secs: Option<u64>,
    concurrency: Option<usize>,
    retention_days: Option<u64>,
    retention_max_rows: Option<u64>,
}

impl Config {
    pub fn load(path: Option<PathBuf>) -> Self {
        Self::load_with_source(path).0
    }

    pub fn load_with_source(path: Option<PathBuf>) -> (Self, Option<PathBuf>) {
        let mut cfg = Config::default();
        let candidates = [
            path,
            Some(PathBuf::from("./agent.toml")),
            Some(PathBuf::from("/etc/cyclo-agent/agent.toml")),
        ];
        let mut source = None;
        for c in candidates.into_iter().flatten() {
            if let Ok(text) = std::fs::read_to_string(&c) {
                source = Some(c);
                if let Ok(f) = toml::from_str::<FileCfg>(&text) {
                    // Порядок: directory, затем schedule (файл поверх базы).
                    if let Some(v) = f.directory {
                        cfg.directory = v;
                    }
                    if let Some(v) = f.schedule {
                        if v.components().count() > 1 {
                            if let Some(parent) = v.parent() {
                                if !parent.as_os_str().is_empty() {
                                    cfg.directory = parent.to_path_buf();
                                }
                            }
                            if let Some(name) = v.file_name().and_then(|n| n.to_str()) {
                                cfg.schedule_file = name.to_owned();
                            }
                        } else if let Some(name) = v.to_str() {
                            cfg.schedule_file = name.to_owned();
                        }
                    }
                    if let Some(v) = f.schedule_file {
                        cfg.schedule_file = v;
                    }
                    if let Some(v) = f.db {
                        cfg.db = v;
                    }
                    if let Some(v) = f.web.or(f.listen) {
                        cfg.web = Some(v);
                    }
                    if let Some(v) = f.dashboards {
                        cfg.dashboards = v;
                        cfg.dashboards_explicit = true;
                    } else if let Some(dir) = cfg.db.parent() {
                        // Дефолт — рядом с БД (пересчёт после возможного --db).
                        cfg.dashboards = dir.join("dashboards.json");
                    }
                    if let Some(v) = f.poll_secs {
                        cfg.poll_secs = v.max(1);
                    }
                    if let Some(v) = f.lookahead_secs {
                        cfg.lookahead_secs = v.max(5);
                    }
                    if let Some(v) = f.concurrency {
                        cfg.concurrency = v.clamp(1, 64);
                    }
                    if let Some(v) = f.retention_days {
                        cfg.retention_days = v.max(1);
                    }
                    if let Some(v) = f.retention_max_rows {
                        cfg.retention_max_rows = v.max(1000);
                    }
                }
                break;
            }
        }
        cfg.config_source = source.clone();
        (cfg, source)
    }
}
