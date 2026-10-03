use cyclorithm_core::cond::Value;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone)]
pub struct Job {
    pub time: i64,
    pub point: String,
    pub action: String,
    pub cmd: String,
    pub args: Vec<String>,
    pub timeout_secs: u64,
}

/// Только события с action_attrs.cmd исполняются; остальные игнорируются.
pub fn to_job(time: i64, point: &str, action: &str, attrs: &[(String, Value)]) -> Option<Job> {
    let mut cmd: Option<String> = None;
    let mut args = vec![];
    let mut timeout_secs: u64 = 300;
    for (k, v) in attrs {
        match (k.as_str(), v) {
            ("cmd", Value::Str(s)) => cmd = Some(s.clone()),
            ("args", Value::Array(xs)) => {
                for x in xs {
                    if let Value::Str(s) = x {
                        args.push(s.clone());
                    }
                }
            }
            ("timeout_s", Value::Num(n)) => {
                timeout_secs = (*n).clamp(1, 86400) as u64;
            }
            _ => {}
        }
    }
    Some(Job {
        time,
        point: point.to_owned(),
        action: action.to_owned(),
        cmd: cmd?,
        args,
        timeout_secs,
    })
}

pub fn job_key(j: &Job) -> String {
    let mut h = Sha256::new();
    h.update(j.time.to_string().as_bytes());
    h.update(b"|");
    h.update(j.point.as_bytes());
    h.update(b"|");
    h.update(j.action.as_bytes());
    h.update(b"|");
    h.update(j.cmd.as_bytes());
    h.update(b"|");
    h.update(j.args.join(" ").as_bytes());
    hex::encode(h.finalize())
}

pub fn truncate(s: &str) -> String {
    const MAX: usize = 8192;
    if s.len() <= MAX {
        return s.to_owned();
    }
    format!("…[truncated]{}", &s[s.len() - MAX..])
}
