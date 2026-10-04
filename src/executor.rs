use crate::jobs::{describe, kind_name, truncate, Job, Kind};
use crate::store::Run;

pub async fn run_job(job: &Job) -> Run {
    let started = chrono::Utc::now().timestamp_millis();
    let (status, exit_code, latency_ms, out_tail, err_tail, result) = match &job.kind {
        Kind::Exec { cmd, args } => run_exec(cmd, args, job.timeout_secs).await,
        Kind::Http {
            url,
            expect,
            contains,
        } => run_http(url, *expect, contains.as_deref(), job.timeout_secs).await,
        Kind::Tcp { host, port } => run_tcp(host, *port, job.timeout_secs).await,
    };
    let finished = chrono::Utc::now().timestamp_millis();
    Run {
        id: 0,
        job_key: crate::jobs::job_key(job),
        scheduled_at: job.time,
        started_at: started,
        finished_at: finished,
        status,
        kind: kind_name(&job.kind).to_owned(),
        tags: serde_json::to_string(&job.tags).unwrap_or_default(),
        cmd: describe(job),
        args: String::new(),
        exit_code,
        latency_ms,
        out_tail,
        err_tail,
        result,
    }
}

async fn run_exec(
    cmd: &str,
    args: &[String],
    timeout_secs: u64,
) -> (String, Option<i64>, i64, String, String, String) {
    let mut c = tokio::process::Command::new(cmd);
    c.args(args);
    c.stdout(std::process::Stdio::piped());
    c.stderr(std::process::Stdio::piped());
    let t0 = std::time::Instant::now();
    let out = tokio::time::timeout(std::time::Duration::from_secs(timeout_secs), c.output()).await;
    let latency = t0.elapsed().as_millis() as i64;
    match out {
        Err(_) => (
            "timeout".to_owned(),
            None,
            latency,
            String::new(),
            "killed by timeout".to_owned(),
            "{}".to_owned(),
        ),
        Ok(Err(e)) => (
            "fail".to_owned(),
            None,
            latency,
            String::new(),
            e.to_string(),
            "{}".to_owned(),
        ),
        Ok(Ok(o)) => {
            let code = o.status.code().map(|c| c as i64);
            let st = if o.status.success() { "ok" } else { "fail" }.to_owned();
            (
                st,
                code,
                latency,
                truncate(&String::from_utf8_lossy(&o.stdout)),
                truncate(&String::from_utf8_lossy(&o.stderr)),
                "{}".to_owned(),
            )
        }
    }
}

async fn run_http(
    url: &str,
    expect: Option<i64>,
    contains: Option<&str>,
    timeout_secs: u64,
) -> (String, Option<i64>, i64, String, String, String) {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(timeout_secs))
        .build();
    let client = match client {
        Ok(c) => c,
        Err(e) => {
            return (
                "fail".to_owned(),
                None,
                0,
                String::new(),
                e.to_string(),
                "{}".to_owned(),
            );
        }
    };
    let t0 = std::time::Instant::now();
    let resp = client.get(url).send().await;
    let latency = t0.elapsed().as_millis() as i64;
    match resp {
        Err(e) => (
            if e.is_timeout() {
                "timeout".to_owned()
            } else {
                "fail".to_owned()
            },
            None,
            latency,
            String::new(),
            e.to_string(),
            "{}".to_owned(),
        ),
        Ok(r) => {
            let code = r.status().as_u16() as i64;
            let body = r.text().await.unwrap_or_default();
            let code_ok = expect.map_or((200..300).contains(&code), |e| code == e);
            let body_ok = contains.is_none_or(|s| body.contains(s));
            let matched = code_ok && body_ok;
            let result = serde_json::json!({
                "status_code": code,
                "bytes": body.len(),
                "matched": matched,
            })
            .to_string();
            (
                if matched {
                    "ok".to_owned()
                } else {
                    "fail".to_owned()
                },
                Some(code),
                latency,
                truncate(&body),
                if matched {
                    String::new()
                } else {
                    format!(
                        "expectation failed: expect={expect:?} contains={contains:?} got={code}"
                    )
                },
                result,
            )
        }
    }
}

async fn run_tcp(
    host: &str,
    port: u16,
    timeout_secs: u64,
) -> (String, Option<i64>, i64, String, String, String) {
    let t0 = std::time::Instant::now();
    let out = tokio::time::timeout(
        std::time::Duration::from_secs(timeout_secs),
        tokio::net::TcpStream::connect((host, port)),
    )
    .await;
    let latency = t0.elapsed().as_millis() as i64;
    match out {
        Err(_) => (
            "timeout".to_owned(),
            None,
            latency,
            String::new(),
            "connect timed out".to_owned(),
            "{}".to_owned(),
        ),
        Ok(Err(e)) => (
            "fail".to_owned(),
            None,
            latency,
            String::new(),
            e.to_string(),
            "{}".to_owned(),
        ),
        Ok(Ok(_)) => (
            "ok".to_owned(),
            None,
            latency,
            String::new(),
            String::new(),
            "{}".to_owned(),
        ),
    }
}
