use crate::jobs::{truncate, Job};
use crate::store::Run;

pub async fn run_job(job: &Job) -> Run {
    let started = chrono::Utc::now().timestamp_millis();
    let mut cmd = tokio::process::Command::new(&job.cmd);
    cmd.args(&job.args);
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());
    let t0 = std::time::Instant::now();
    let out = tokio::time::timeout(
        std::time::Duration::from_secs(job.timeout_secs),
        cmd.output(),
    )
    .await;
    let latency = t0.elapsed().as_millis() as i64;
    let finished = chrono::Utc::now().timestamp_millis();
    let (status, exit_code, out_s, err_s) = match out {
        Err(_) => (
            "timeout".to_owned(),
            None,
            String::new(),
            "killed by timeout".to_owned(),
        ),
        Ok(Err(e)) => ("fail".to_owned(), None, String::new(), e.to_string()),
        Ok(Ok(o)) => {
            let code = o.status.code().map(|c| c as i64);
            let st = if o.status.success() { "ok" } else { "fail" }.to_owned();
            (
                st,
                code,
                String::from_utf8_lossy(&o.stdout).into_owned(),
                String::from_utf8_lossy(&o.stderr).into_owned(),
            )
        }
    };
    Run {
        id: 0,
        job_key: crate::jobs::job_key(job),
        scheduled_at: job.time,
        started_at: started,
        finished_at: finished,
        status,
        cmd: job.cmd.clone(),
        args: serde_json::to_string(&job.args).unwrap_or_default(),
        exit_code,
        latency_ms: latency,
        out_tail: truncate(&out_s),
        err_tail: truncate(&err_s),
    }
}
