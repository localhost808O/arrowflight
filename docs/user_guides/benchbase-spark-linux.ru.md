# BenchBase Spark: сравнительные тесты на Linux

Guide запускает одинаковый набор TPC-H queries двумя путями:

- `Flight` - Spark читает общий HDFS dataset через Arrow Flight SQL;
- `Direct` - Spark читает тот же HDFS dataset через Parquet DataSource.

Benchmark поднимает NameNode и три составные storage/compute ноды. На каждой ноде
в одном container работают `HDFS DataNode`, `Flight server` и `Spark worker`.
HDFS использует `replication=1`; каждый generated Parquet shard загружается с
colocated DataNode и занимает один HDFS block. Это даёт одинаковые физические
файлы и согласованные hostnames для Spark/Flight data locality.

## Требования

- Linux x86_64;
- Docker Engine;
- Docker Compose v2 (`docker compose`);
- Bash, Python 3, `awk`, `sed` и `tee`;
- минимум 8 GB RAM, рекомендовано 12-16 GB.

Проверка окружения:

```bash
docker --version
docker compose version
docker info
python3 --version
```

Если `docker info` требует root, запускай команды через `sudo` или добавь пользователя в группу `docker` и заново войди в shell.

## Полное сравнение

Из корня репозитория:

```bash
chmod +x run-benchmark-spark.sh

BENCHMARK_SCALE_FACTOR=0.01 \
BENCHBASE_TIME_SECONDS=60 \
BENCHBASE_WARMUP_SECONDS=30 \
BENCHBASE_TERMINALS=1 \
./run-benchmark-spark.sh \
  tpch compare q1,q6,q14
```

`compare` выполняет полный цикл:

1. удаляет старые benchmark containers и volumes;
2. запускает HDFS NameNode и четыре `DataNode + Flight + Spark worker` ноды;
3. генерирует один TPC-H dataset и загружает его в `hdfs://hdfs-namenode:8020/bench`;
4. публикует `tpch_flight` и `tpch_direct` поверх тех же HDFS Parquet-файлов;
5. запускает заданное число парных наблюдений, автоматически чередуя порядок
   Flight/Direct;
6. строит общий HTML report и обновляет локальную папку `pages/`.

В generated report и reference results попадут только `q1`, `q6`, `q14`. BenchBase может вывести в консоль остальные transaction types с нулевыми counters; они не выполняются и не добавляются в per-query report.

Все 22 TPC-H query по одному разу для каждого пути внутри каждого парного
наблюдения:

```bash
bash benchmarks/benchbase-spark/run-benchbase-spark.sh tpch compare all
```

Для результата, который должен попасть в основную curated-матрицу, запускайте
отдельно `q1`, `q6` и `q14`, используйте не менее трёх пар и обязательно
запишите ресурсы хоста:

```bash
BENCHBASE_PAIRED_OBSERVATIONS=3 \
BENCHBASE_HOST_RESOURCES="8 vCPU, 32 GiB RAM, Spark workers=2" \
BENCHMARK_SCALE_FACTOR=1 \
bash benchmarks/benchbase-spark/run-benchbase-spark.sh tpch compare q1
```

Основная страница принимает только Q1/Q6/Q14, SF 0.1/1 и топологии с 1/3/8
Flight-нодами. Запуски `all`, legacy, smoke и diagnostic публикуются отдельно
на `exploratory.html`. На графиках latency меньшая колонка означает более
быстрое выполнение.

Selector `all` активирует Q1-Q22 для Flight и Direct. Общий compare report и
главная страница GitHub Pages показывают grouped bar chart средней measured
latency по каждому query (`q01`-`q22`). Значения берутся из BenchBase
`*.raw.csv`.

Если нужны повторные samples каждого query, задай длительность измерения каждой
query:

```bash
BENCHBASE_TIME_SECONDS=180 \
BENCHBASE_WARMUP_SECONDS=20 \
bash benchmarks/benchbase-spark/run-benchbase-spark.sh tpch compare all
```

В timed-режиме `BENCHBASE_TIME_SECONDS` применяется отдельно к каждой выбранной
query. Скрипт создаёт последовательные фазы Q1-Q22, в каждой из которых активна
только одна query. `BENCHBASE_WARMUP_SECONDS` применяется один раз перед первой
query каждого engine run. Для значений выше один engine run занимает примерно
`20 + 22 * 180 = 3980` секунд. Полный `compare` по умолчанию выполняет два
engine runs (одну пару), то есть около 7960 секунд плюс подготовка данных и
завершение текущих queries. Для publishable-запуска с тремя парами потребуется
около 23880 секунд.

Результат:

```text
benchmarks/benchbase-spark/results/tpch-compare-q1,q6,q14-<timestamp>/compare.report.html
```

Внутри сохраняются все наблюдения и отдельные reports:

```text
observations/observation-001/flight/*.report.html
observations/observation-001/direct/*.report.html
observations/observation-002/...
observations/observation-003/...
```

## Основные параметры

```bash
BENCHMARK_SCALE_FACTOR=0.1       # TPC-H scale factor
BENCHBASE_TIME_SECONDS=120      # measured-секунды для каждой выбранной query
BENCHBASE_WARMUP_SECONDS=30     # прогрев каждого engine run
BENCHBASE_TERMINALS=2           # параллельные BenchBase workers
BENCHBASE_RATE=unlimited        # лимит requests/sec
BENCHBASE_QUERY_TIMEOUT_SECONDS=120   # timeout BenchBase query через JDBC
BENCHBASE_CAPTURE_TIMEOUT_SECONDS=120 # timeout повторного query для HTML-проверки
BENCHBASE_PAIRED_OBSERVATIONS=3       # opt-in: publishable-запуск из трёх пар
BENCHBASE_CLUSTER_NODES=3             # 1-10 Flight/Spark worker нод
BENCHBASE_COMPARE_ORDER=flight-first  # порядок первой пары; следующие чередуются
BENCHBASE_CACHE_POLICY=warm-cache     # один stack без eviction/reset между runs
BENCHBASE_UPDATE_PAGES=false    # не обновлять локальную pages/
BENCHMARK_OBSERVABILITY=true    # автоматически запустить Grafana/Prometheus
HDFS_BLOCK_SIZE_BYTES=1073741824 # shard обязан помещаться в один HDFS block
FLIGHT_BATCH_SIZE=65536         # Arrow/gRPC batch; 131072 доступен для A/B
FLIGHT_DUCKDB_THREADS=2         # optional A/B override для DuckDB SET threads
FLIGHT_TIMING_LOG_LEVEL=DEBUG   # только TIMING-события при общем INFO
```

Benchmark Spark запускается с `spark.sql.ansi.enabled=true`. Это необходимо для
Spark DataSource V2: без ANSI Spark 3.5 не передаёт decimal-выражения Q1
(`l_extendedprice * (1 - l_discount)`) в aggregation pushdown, и Flight вынужден
передавать миллионы исходных строк обратно в Spark вместо нескольких partial
aggregate rows.

Для профилирования не включай общий `FLIGHT_LOG_LEVEL=DEBUG`: он синхронно пишет
все клиентские и серверные DEBUG-события и искажает Flight-only часть сравнения.
Используй `FLIGHT_LOG_LEVEL=INFO FLIGHT_TIMING_LOG_LEVEL=DEBUG`, чтобы оставить
структурированные `TIMING`-строки без подробного лога каждого batch. Значение
`FLIGHT_BATCH_SIZE=131072` уменьшает число batch, но его нужно сравнивать на
целевом HDFS workload: на локальном synthetic scan устойчивого ускорения
относительно `65536` не было.

Два пути внутри пары выполняются последовательно. В одной команде порядок
автоматически чередуется: `Flight → Direct`, затем `Direct → Flight` и так
далее. `BENCHBASE_COMPARE_ORDER=direct-first` меняет только порядок первой пары.
Поддерживаемая cache policy `warm-cache` готовит stack один раз и не сбрасывает
JVM, Spark, HDFS или OS caches между runs; эффект порядка распределяется за счёт
чередования. Warmup применяется отдельно в начале каждого engine run.

Machine-readable `benchmark-result.json` по
[schema v2](../../benchmarks/benchbase-spark/schema/benchmark-result-v2.schema.json)
хранит значения каждого наблюдения,
timestamps, порядок, warmup, cache policy, failure metadata и ссылки на все raw
CSV. Агрегаты используют медиану каждого успешного наблюдения как один
равновесный sample и показывают median, min/max spread, p25/p75, IQR и p95.
Если один engine падает, harness всё равно запускает вторую половину пары и
следующие пары, но такой результат не попадает в публикуемый dashboard.
Английский
[reproducibility and interpretation report](../benchmarks/tpch-flight-vs-direct-v2.md)
содержит publication contract, точные команды и ограничения интерпретации.

После измерения каждый выбранный query повторно выполняется через `beeline`, чтобы
сохранить фактический результат в HTML report. Если этот запуск превышает
`BENCHBASE_CAPTURE_TIMEOUT_SECONDS`, script удаляет неполный CSV, отмечает result как
`not captured` и продолжает следующий benchmark path.

Пример более тяжёлого запуска:

```bash
BENCHMARK_SCALE_FACTOR=1 \
BENCHBASE_TIME_SECONDS=300 \
BENCHBASE_TERMINALS=4 \
BENCHBASE_UPDATE_PAGES=false \
./run-benchmark-spark.sh \
  tpch compare q1,q6,q14
```

## Одиночный тест

Запуск Flight с нуля, один проход Q6:

```bash
./run-benchmark-spark.sh tpch fresh q6
```

Несколько выбранных queries, по одному проходу каждого:

```bash
./run-benchmark-spark.sh \
  tpch fresh q1,q6,q14
```

## Диагностика и остановка

## Grafana и Prometheus

Benchmark script по умолчанию сам запускает observability stack. Его можно
отключить через `BENCHMARK_OBSERVABILITY=false`. Ручной запуск:

```bash
docker compose --profile observability up -d \
  prometheus grafana node-exporter cadvisor
```

Открой `http://<linux-server>:3000/d/arrowflight-benchmark`. Dashboard
`ArrowFlight Benchmark` создаётся автоматически. Prometheus доступен на порту
`9090`; Grafana разрешает anonymous read-only доступ, поэтому закрой порты
`3000` и `9090` firewall-ом от публичного интернета.

Если сервер удалённый и наружу открыт только SSH-порт, используй SSH-туннель.
На локальном компьютере открой отдельный терминал:

```bash
ssh -L 13000:127.0.0.1:3000 \
  -p 30105 ssamokhin@84.237.52.100
```

Не закрывай этот терминал. В локальном браузере открой:

```text
http://localhost:13000/d/arrowflight-benchmark
```

Для другого сервера замени SSH user, IP и port. Публично открывать Grafana port
`3000` при таком подключении не требуется.

Dashboard показывает:

- загрузку каждого Linux CPU core, host RAM, число процессов и потоков;
- CPU, working-set RAM и network I/O каждого Docker container;
- heap, non-heap и потоки каждого Flight JVM;
- active queries, failures, query rate и p95/mean server execution time;
- logical Parquet throughput, physical disk throughput и HDFS network I/O.

`Parquet query/read duration` измеряет полный server path: получение файлов,
scan, filter/aggregation и отправку результата. Это полезное benchmark-время, но
не чистый kernel I/O wait. Чистый физический поток чтения смотри в
`Linux disk I/O`; planned размер Parquet input — в `Parquet logical throughput`.

Проверка targets:

```bash
curl -fsS http://localhost:9090/-/ready
curl -fsS http://localhost:9404/metrics  # если порт Flight node опубликован вручную
docker compose --profile observability ps
```

Остановка monitoring без удаления истории:

```bash
docker compose --profile observability stop \
  grafana prometheus node-exporter cadvisor
```

История хранится в volumes `grafana-data` и `prometheus-data` (по умолчанию 15 дней).

Логи всех benchmark services:

```bash
./run-benchmark-spark.sh tpch logs
```

Проверка состояния:

```bash
docker compose -f docker-compose.yml --profile benchbase ps
```

Остановка с удалением benchmark volumes:

```bash
./run-benchmark-spark.sh tpch down
```

`compare`, `fresh`, `prepare` и `down` удаляют benchmark volumes. Не используй их, если текущий generated dataset нужно сохранить.
