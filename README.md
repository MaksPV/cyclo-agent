# cyclo-agent

Демон + веб-морда для расписаний на языке Cyclorithm. Раз в N минут
исполняет команды из `.cyclo`-файла (пинг, скрипты, бэкап), пишет историю
в SQLite, отдаёт дашборд с графиками и редактор расписания. Один бинарь,
без докера.

Ядро языка берётся из `../cycloritm/crates/cyclorithm-core` (path-зависимость).

## Быстрый старт

```sh
cargo build --release
# Только демон (без порта):
./target/release/cyclo-agent run --schedule ./examples/schedule.cyclo --db ./agent.db
# Демон + веб-морда:
./target/release/cyclo-agent run --schedule ./examples/schedule.cyclo \
  --db ./agent.db --web 127.0.0.1:8080
```

Открыть http://127.0.0.1:8080 — при первом запуске страница `/setup`
попросит придумать логин/пароль (хранится argon2-хэшем в БД).

Конфиг `agent.toml` (флаг `--config`, иначе `./agent.toml`,
иначе `/etc/cyclo-agent/agent.toml`) — см. `agent.toml.example`:

```toml
schedule = "/etc/cyclo-agent/schedule.cyclo"
db = "/var/lib/cyclo-agent/agent.db"
# web = "127.0.0.1:8080"  # без ключа — демон без веба
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

Планировщик спит до ближайшего события (точность 0 мс по `started_at`),
исполняет due-пачкой (до 50 000 одновременных), дедуп — через `UNIQUE(job_key)`
плюс проверка перед спавном. Опоздание >60с пишется как `skipped` и не
исполняется. Hot reload: правка файла (вотчер 5с), SIGHUP; битый файл —
старый план продолжает работать, ошибка видна в `/api/status`.

## API (всё, кроме `/`, `/healthz` и `/api/setup`, — по сессии)

- `GET /` — дашборд (вшит в бинарь): вкладки События (runs, график latency,
  ближайшие), Редактор («Проверить»/«Сохранить», шпаргалка), Настройки
  (конфиг, сессия, смена пароля); светлая/тёмная тема с переключателем
- `POST /api/setup {login,password,confirm}` — только при пустых users
- `POST /api/login`, `POST /api/logout` (брутфорс: 5 попыток/мин с IP → 429)
- `POST /api/password {current,new,confirm}` — смена пароля, чужие сессии закрываются
- `GET /api/status` (анониму — только setup_required/authenticated), `GET /healthz`
- `GET /api/runs?from&to&limit` (limit ≤ 1000), `GET /api/next?n≤500&within_secs≤30д`
- `GET /api/schedule`, `POST /api/schedule {content}` (валидация перед записью, ≤1МБ)
- `POST /api/validate {content}` → `{ok | code,message}` (≤1МБ)

Сессии: HttpOnly + SameSite=Lax, TTL 12ч.

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
- Простой idle: ~12 МБ RAM, ~0% CPU между событиями (сон до ближайшего).
