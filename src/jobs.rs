use cyclorithm_core::cond::Value;
use sha2::{Digest, Sha256};

/// Чем занимается задача: внешняя команда или встроенная проверка.
#[derive(Debug, Clone)]
pub enum Kind {
    Exec {
        cmd: String,
        args: Vec<String>,
    },
    Http {
        url: String,
        expect: Option<i64>,
        contains: Option<String>,
    },
    Tcp {
        host: String,
        port: u16,
    },
}

#[derive(Debug, Clone)]
pub struct Job {
    pub time: i64,
    pub point: String,
    pub action: String,
    pub kind: Kind,
    pub tags: Vec<String>,
    pub timeout_secs: u64,
}

fn str_field(attrs: &[(String, Value)], key: &str) -> Option<String> {
    attrs
        .iter()
        .find_map(|(k, v)| match (k.as_str() == key, v) {
            (true, Value::Str(s)) => Some(s.clone()),
            _ => None,
        })
}

fn num_field(attrs: &[(String, Value)], key: &str) -> Option<i64> {
    attrs
        .iter()
        .find_map(|(k, v)| match (k.as_str() == key, v) {
            (true, Value::Num(n)) => Some(*n),
            _ => None,
        })
}

/// Только события с `cmd` или `check` исполняются; остальные игнорируются.
pub fn to_job(time: i64, point: &str, action: &str, attrs: &[(String, Value)]) -> Option<Job> {
    let timeout_secs: u64 = num_field(attrs, "timeout_s")
        .map(|n| n.clamp(1, 86400) as u64)
        .unwrap_or(300);
    let tags: Vec<String> = attrs
        .iter()
        .find_map(|(k, v)| match (k.as_str(), v) {
            ("tags", Value::Array(xs)) => Some(
                xs.iter()
                    .filter_map(|x| match x {
                        Value::Str(s) => Some(s.clone()),
                        _ => None,
                    })
                    .collect(),
            ),
            _ => None,
        })
        .unwrap_or_default();
    let kind = match str_field(attrs, "check").as_deref() {
        Some("http") => Kind::Http {
            url: str_field(attrs, "url")?,
            expect: num_field(attrs, "expect"),
            contains: str_field(attrs, "contains"),
        },
        Some("tcp") => {
            let port = num_field(attrs, "port")?;
            if !(1..=65535).contains(&port) {
                return None;
            }
            Kind::Tcp {
                host: str_field(attrs, "host").unwrap_or_else(|| "127.0.0.1".to_owned()),
                port: port as u16,
            }
        }
        _ => {
            let cmd = str_field(attrs, "cmd")?;
            // Без явного check неизвестный check — не исполняем (опечатка видна отсутствием запусков).
            if str_field(attrs, "check").is_some() {
                return None;
            }
            let args: Vec<String> = attrs
                .iter()
                .find_map(|(k, v)| match (k.as_str(), v) {
                    ("args", Value::Array(xs)) => Some(
                        xs.iter()
                            .filter_map(|x| match x {
                                Value::Str(s) => Some(s.clone()),
                                _ => None,
                            })
                            .collect(),
                    ),
                    _ => None,
                })
                .unwrap_or_default();
            Kind::Exec { cmd, args }
        }
    };
    Some(Job {
        time,
        point: point.to_owned(),
        action: action.to_owned(),
        kind,
        tags,
        timeout_secs,
    })
}

/// Краткое имя kind для колонки БД и ключа.
pub fn kind_name(k: &Kind) -> &'static str {
    match k {
        Kind::Exec { .. } => "exec",
        Kind::Http { .. } => "http",
        Kind::Tcp { .. } => "tcp",
    }
}

/// Человекочитаемое «что запускали» для таблицы.
pub fn describe(j: &Job) -> String {
    match &j.kind {
        Kind::Exec { cmd, args } => {
            if args.is_empty() {
                cmd.clone()
            } else {
                format!("{} {}", cmd, args.join(" "))
            }
        }
        Kind::Http { url, .. } => url.clone(),
        Kind::Tcp { host, port } => format!("{host}:{port}"),
    }
}

pub fn job_key(j: &Job) -> String {
    let mut h = Sha256::new();
    h.update(j.time.to_string().as_bytes());
    h.update(b"|");
    h.update(j.point.as_bytes());
    h.update(b"|");
    h.update(j.action.as_bytes());
    h.update(b"|");
    match &j.kind {
        Kind::Exec { cmd, args } => {
            h.update(b"exec|");
            h.update(cmd.as_bytes());
            h.update(b"|");
            h.update(args.join(" ").as_bytes());
        }
        Kind::Http {
            url,
            expect,
            contains,
        } => {
            h.update(b"http|");
            h.update(url.as_bytes());
            h.update(format!("|{expect:?}|{contains:?}").as_bytes());
        }
        Kind::Tcp { host, port } => {
            h.update(format!("tcp|{host}|{port}").as_bytes());
        }
    }
    h.update(b"|");
    h.update(j.tags.join(",").as_bytes());
    hex::encode(h.finalize())
}

pub fn truncate(s: &str) -> String {
    const MAX: usize = 8192;
    if s.len() <= MAX {
        return s.to_owned();
    }
    format!("…[truncated]{}", &s[s.len() - MAX..])
}
