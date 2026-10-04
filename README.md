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
schedule = "/etc/cyclo-agent/schedule.cyclo"  # путь до исполняемого файла
db = "/var/lib/cyclo-agent/agent.db"
# web = "127.0.0.1:8080"  # без ключа — демон без веба
poll_secs = 10        # сейчас не используется (сон до события)
lookahead_secs = 60   # окно /api/next по умолчанию
concurrency = 4
retention_days = 90
retention_max_rows = 1000000
```

`schedule` — путь до файла (исполняемый + база для `use`); раскладывается
на `directory` + `schedule_file` (имя `.cyclo` без путей). Хранение режется
и по дням, и по числу строк. Настройки правятся и из морды
(`GET/POST /api/config`; директория, файл, параллельность — после рестарта
демона, лимиты хранения — без).

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

## Виды задач

Исполняется только событие с `cmd` или `check` в `action_attrs`:

```cyclo
cycle EXEC(task) duration = 1m {
  0m: JOB.fire() {"cmd": task.cmd, "args": task.args, "tags": task.tags, "timeout_s": task.timeout_s};
}
cycle HTTPCHECK(task) duration = 1m {
  0m: JOB.fire() {"check": "http", "url": task.url, "expect": task.expect, "contains": task.contains, "tags": task.tags, "timeout_s": task.timeout_s};
}
cycle TCPCHECK(task) duration = 1m {
  0m: JOB.fire() {"check": "tcp", "host": task.host, "port": task.port, "tags": task.tags, "timeout_s": task.timeout_s};
}
```

- `cmd` — внешняя команда без shell (`args` — массив строк).
  Циклы лучше разделять по видам: блок вычисляет все поля сразу,
  отсутствующее поле в мапе — `unknown-field` в момент развёртки.
- `check: "http"` — GET; `ok`, если код == `expect` (без `expect` — любой
  2xx) и тело содержит `contains` (без него — не проверяется).
  В `result`: `status_code`, `bytes`, `matched`; тело — в `out_tail`.
- `check: "tcp"` — connect; `ok`, если порт открылся за `timeout_s`.
- `tags` — массив строк (`["web", "prod"]`), хранятся с запуском.
- Неизвестный `check` — событие игнорируется (опечатка видна отсутствием запусков).

## API (всё, кроме `/`, `/healthz` и `/api/setup`, — по сессии)

- `GET /` — дашборд (вшит в бинарь): вкладки События (runs, график latency,
  ближайшие), Редактор («Проверить»/«Сохранить», шпаргалка), Настройки
  (конфиг, сессия, смена пароля); светлая/тёмная тема с переключателем
- `POST /api/setup {login,password,confirm}` — только при пустых users
- `POST /api/login`, `POST /api/logout` (брутфорс: 5 попыток/мин с IP → 429)
- `POST /api/password {current,new,confirm}` — смена пароля, чужие сессии закрываются
- `GET /api/status` (анониму — только setup_required/authenticated), `GET /healthz`
- `GET /api/runs?from&to&limit&offset&status` (limit ≤ 1000, статус из
  ok|fail|timeout|skipped, иначе 400) — `{runs, total}` для пагинации,
  `GET /api/next?n≤500&within_secs≤30д`
- `GET /api/schedule`, `POST /api/schedule {content}` (валидация перед записью, ≤1МБ)
- `POST /api/validate {content}` → `{ok | code,message}` (≤1МБ)
- `GET /api/files` → `{main, files[]}` (.cyclo рядом с расписанием),
  `GET /api/files?path=` → `{path, content}`, `POST /api/files {path, content}`
  (≤1МБ, только `.cyclo` без `..`), `DELETE /api/files?path=` (главный нельзя).
  Проверка — всегда по главному файлу (use резолвится с диска).
- `GET /api/series?tag&job&kind&metric&agg&from&to&bucket_secs&mode` — точки `{t, v|null}`.
  Способ `mode=bucket` (дефолт): окно режется на кусочки по шагу
  (явный `bucket_secs` или авто окно/200, бакетов ≤2000 — иначе шаг укрупняется,
  фактический виден в ответе), в каждом агрегация; метрика — `latency_ms`,
  `up` (1/0 из статуса), `exit_code` или числовой путь в `result`
  (`result.status_code`, ...); агрегация `avg|min|max|p50|p99|count`,
  окно ≤90д. Способ `mode=raw`: каждый запуск своей точкой в точное время
  (агрегации нет); точек больше 5000 — 400 с просьбой включить «по шагу».
- `GET /api/dashboards` — дефолт, если файла нет; `POST /api/dashboards {content}`
  (≤256КБ, `{version:1, charts:[{title,metric,agg,type:line|dots|bars,window_secs,bucket_secs?,mode?:raw|bucket,tag?|job?|kind?}]}`;
  без `mode` — `bucket`, старые файлы читаются как раньше)

Сессии: HttpOnly + SameSite=Lax, TTL 12ч.

## От расписания до графика

1. Задача с тегом в `.cyclo`: `{"cmd": ..., "tags": ["web"], ...}`
   или `{"check": "http", ..., "tags": ["web"], ...}`.
2. Запуски копятся в `runs` (вид, теги, `result` JSON).
3. Вкладка «Дашборд» → конструктор: списки тегов/видов/джоб/метрик
   подтягиваются из живых данных (`GET /api/meta`) — вбивать вслепую
   не надо; новая метрика появится в списке после первых запусков.
4. Строка таблицы = график (источник, метрика-путь, агрегация, вид,
   окно, бакет); правки inline, «Сохранить дашборд» пишет `dashboards.json`.
5. Примеры: `examples/schedule.cyclo` (старт), `examples/demo.cyclo`
   (плотное с тегами), `examples/checks.cyclo` (шаблон http/tcp).

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
