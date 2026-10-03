# cyclo-agent

Демон + веб-морда для расписаний на языке Cyclorithm. Раз в N минут
исполняет команды из `.cyclo`-файла (пинг, скрипты, бэкап), пишет историю
в SQLite, отдаёт дашборд с графиками и редактор расписания. Один бинарь,
без докера.

Ядро языка берётся из `../cycloritm/crates/cyclorithm-core` (path-зависимость).

## Быстрый старт

```sh
cargo build --release
./target/release/cyclo-agent --schedule ./examples/schedule.cyclo \
  --db ./agent.db --listen 127.0.0.1:8080
```

Открыть http://127.0.0.1:8080 — при первом запуске страница `/setup`
попросит придумать логин/пароль (хранится argon2-хэшем в БД).

Конфиг `agent.toml` (флаг `--config`, иначе `./agent.toml`,
иначе `/etc/cyclo-agent/agent.toml`) — см. `agent.toml.example`:

```toml
schedule = "/etc/cyclo-agent/schedule.cyclo"
db = "/var/lib/cyclo-agent/agent.db"
listen = "127.0.0.1:8080"
poll_secs = 10        # сейчас не используется (сон до события)
lookahead_secs = 60   # окно /api/next по умолчанию
concurrency = 4
retention_days = 90
```

## Формат задач

Агент исполняет только события с `action_attrs.cmd`:

```cyclo
schedule "Agent" {
  point JOB { actions = [fire]; }
  cycle RUN(job) duration = 1m {
    0m: JOB.fire() {"cmd": job.cmd, "args": job.args, "timeout_s": job.timeout_s};
  }
  root_cycle start_time = "2026-01-01T00:00:00", duration = 24h {
    0h: fill RUN({"cmd": "/opt/jobs/ping.sh", "args": [], "timeout_s": 60});
    3h: RUN({"cmd": "/usr/local/bin/backup.sh", "args": ["--incremental"], "timeout_s": 1800});
  }
}
```

Планировщик спит до ближайшего события (`next`, точность ~мс),
исполняет due-пачкой, дедуп — через `UNIQUE(job_key)`. Опоздание >60с
пишется как `skipped` и не исполняется. Hot reload: правка файла (вотчер
5с), `POST /api/reload`, SIGHUP; битый файл — старый план продолжает
работать, ошибка видна в `/api/status`.

## API (всё, кроме `/healthz` и `/api/setup`, — по сессии)

- `GET /` — дашборд (вшит в бинарь): runs, canvas-график latency,
  ближайшие, редактор с «Проверить»/«Сохранить» и шпаргалкой
- `POST /api/setup {login,password,confirm}` — только при пустых users
- `POST /api/login`, `POST /api/logout`
- `GET /api/status`, `GET /healthz`
- `GET /api/runs?from&to&limit`, `GET /api/next?n&within_secs`
- `GET /api/schedule`, `POST /api/schedule {content}` (валидация перед записью)
- `POST /api/validate {content}` → `{ok | code,message}`
- `POST /api/reload` — разбудить планировщик

## Раскладка на сервере

```
/usr/local/bin/cyclo-agent
/etc/cyclo-agent/agent.toml, schedule.cyclo   # 640 root:cyclo-agent
/var/lib/cyclo-agent/agent.db                 # владелец cyclo-agent
```

systemd: `User=cyclo-agent Restart=always`, наружу — через reverse-proxy.
Сброс доступа: `cyclo-agent reset-auth` (гасит сессии) + удалить строку
`users` в БД для повторного setup.

## Ограничения

- Только exec (без `sh -c`), всё от юзера демона, `timeout_s` убивает зависшее.
- Плотные расписания (тысячи повторов на экземпляр корня, субминутные
  тики на весь день) упираются в производительность развёртки ядра:
  см. ветку `perf/lazy-here` в `cycloritm`. Минутные и реже — миллисекунды.
