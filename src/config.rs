use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Config {
    pub schedule: PathBuf,
    pub db: PathBuf,
    /// Адрес веб-морды; None — демон без веба.
    pub web: Option<String>,
    pub poll_secs: u64,
    pub lookahead_secs: u64,
    pub concurrency: usize,
    pub retention_days: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            schedule: PathBuf::from("/etc/cyclo-agent/schedule.cyclo"),
            db: PathBuf::from("/var/lib/cyclo-agent/agent.db"),
            web: None,
            poll_secs: 10,
            lookahead_secs: 60,
            concurrency: 4,
            retention_days: 90,
        }
    }
}

#[derive(Debug, Default, serde::Deserialize)]
struct FileCfg {
    schedule: Option<PathBuf>,
    db: Option<PathBuf>,
    web: Option<String>,
    listen: Option<String>,
    poll_secs: Option<u64>,
    lookahead_secs: Option<u64>,
    concurrency: Option<usize>,
    retention_days: Option<u64>,
}

impl Config {
    pub fn load(path: Option<PathBuf>) -> Self {
        let mut cfg = Config::default();
        let candidates = [
            path,
            Some(PathBuf::from("./agent.toml")),
            Some(PathBuf::from("/etc/cyclo-agent/agent.toml")),
        ];
        for c in candidates.into_iter().flatten() {
            if let Ok(text) = std::fs::read_to_string(&c) {
                if let Ok(f) = toml::from_str::<FileCfg>(&text) {
                    if let Some(v) = f.schedule {
                        cfg.schedule = v;
                    }
                    if let Some(v) = f.db {
                        cfg.db = v;
                    }
                    if let Some(v) = f.web.or(f.listen) {
                        cfg.web = Some(v);
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
                }
                break;
            }
        }
        cfg
    }
}
