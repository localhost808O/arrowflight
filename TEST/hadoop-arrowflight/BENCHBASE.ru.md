# BenchBase для legacy ArrowFlight

Эта папка содержит старый ArrowFlight на commit
`c6f0ef9d81c522c2d5a949d95be938966359854a`. Benchmark wrapper собирает из него
отдельный image `arrowflight-legacy-test:latest` и использует общую проверенную
BenchBase-обвязку из основного репозитория.

Старые server, client, pushdown и execution-классы не заменяются текущими.
Добавлен только `LegacyFlightSessionCatalog`: он позволяет Spark Thrift открыть
сохранённые таблицы старого DataSource V2. Сам bridge не читает данные и не
участвует в Flight/Parquet execution.

## Запуск

Сначала из корня основного репозитория запусти current:

```bash
SPARK_SQL_ANSI_ENABLED=true \
FLIGHT_LOG_LEVEL=INFO \
FLIGHT_TIMING_LOG_LEVEL=DEBUG \
FLIGHT_BATCH_SIZE=65536 \
FLIGHT_DUCKDB_THREADS=2 \
BENCHMARK_SCALE_FACTOR=1 \
BENCHBASE_CLUSTER_NODES=3 \
BENCHBASE_PAIRED_OBSERVATIONS=3 \
BENCHBASE_QUERY_REPETITIONS=1 \
BENCHBASE_TIME_SECONDS=60 \
BENCHBASE_WARMUP_SECONDS=20 \
BENCHBASE_TERMINALS=1 \
BENCHBASE_RATE=unlimited \
BENCHBASE_QUERY_TIMEOUT_SECONDS=600 \
BENCHBASE_CAPTURE_TIMEOUT_SECONDS=600 \
BENCHBASE_COMPARE_ORDER=flight-first \
BENCHBASE_CACHE_POLICY=warm-cache \
BENCHBASE_HOST_RESOURCES="16 logical CPU, 16 GiB RAM, Spark/Flight nodes=3" \
BENCHBASE_UPDATE_PAGES=false \
BENCHMARK_OBSERVABILITY=false \
HDFS_BLOCK_SIZE_BYTES=1073741824 \
DIRECT_PARQUET_PARTITIONS=3 \
timeout --foreground --signal=TERM --kill-after=30s 10770s \
bash benchmarks/benchbase-spark/run-benchbase-spark.sh tpch compare q1,q4
```

Затем перейди в legacy-проект и выполни ту же команду:

```bash
cd TEST/hadoop-arrowflight

SPARK_SQL_ANSI_ENABLED=true \
FLIGHT_LOG_LEVEL=INFO \
FLIGHT_TIMING_LOG_LEVEL=DEBUG \
FLIGHT_BATCH_SIZE=65536 \
FLIGHT_DUCKDB_THREADS=2 \
BENCHMARK_SCALE_FACTOR=1 \
BENCHBASE_CLUSTER_NODES=3 \
BENCHBASE_PAIRED_OBSERVATIONS=3 \
BENCHBASE_QUERY_REPETITIONS=1 \
BENCHBASE_TIME_SECONDS=60 \
BENCHBASE_WARMUP_SECONDS=20 \
BENCHBASE_TERMINALS=1 \
BENCHBASE_RATE=unlimited \
BENCHBASE_QUERY_TIMEOUT_SECONDS=600 \
BENCHBASE_CAPTURE_TIMEOUT_SECONDS=600 \
BENCHBASE_COMPARE_ORDER=flight-first \
BENCHBASE_CACHE_POLICY=warm-cache \
BENCHBASE_HOST_RESOURCES="16 logical CPU, 16 GiB RAM, Spark/Flight nodes=3" \
BENCHBASE_UPDATE_PAGES=false \
BENCHMARK_OBSERVABILITY=false \
HDFS_BLOCK_SIZE_BYTES=1073741824 \
DIRECT_PARQUET_PARTITIONS=3 \
timeout --foreground --signal=TERM --kill-after=30s 10770s \
bash benchmarks/benchbase-spark/run-benchbase-spark.sh tpch compare q1,q4
```

Результаты не смешиваются:

```text
../../benchmarks/benchbase-spark/results/tpch-compare-q1,q4-.../
benchmarks/benchbase-spark/results/tpch-legacy-compare-q1,q4-.../
```

## Итоговый old/new отчёт

Из `TEST/hadoop-arrowflight` передай две завершённые папки в comparator:

```bash
python3 ../../benchmarks/benchbase-spark/compare-implementations.py \
  --legacy benchmarks/benchbase-spark/results/tpch-legacy-compare-q1,q4-LEGACY_TIMESTAMP \
  --current ../../benchmarks/benchbase-spark/results/tpch-compare-q1,q4-CURRENT_TIMESTAMP \
  --out benchmarks/benchbase-spark/results/legacy-vs-current-q1,q4
```

Он создаст:

```text
implementation-comparison.md
implementation-comparison.json
```

Comparator не строит отчёт, если отличаются workload, scale factor, warmup,
measurement, число пар, topology, JVM/HDFS/Spark-настройки, BenchBase image,
generator image, хэши входов harness, SQL digests или layout Parquet shards.
Также обязательны корректные ответы обоих путей и не менее трёх полных пар
Flight/Direct.

Основная межверсионная метрика:

```text
(legacy Flight / legacy Direct) / (current Flight / current Direct)
```

Значение больше `1.0` означает, что current быстрее после нормализации через
Direct-контроль. В отчёте остаётся и raw Flight speedup. Если Direct между
двумя запусками расходится более чем на 25%, comparator явно пишет warning.

## Важное ограничение старого кода

Legacy фиксирует Arrow scan batch в `65536`, DuckDB `threads=1` и не имеет
структурированного `TIMING` logger. Поэтому wrapper предупреждает, что
`FLIGHT_DUCKDB_THREADS=2` и `FLIGHT_TIMING_LOG_LEVEL=DEBUG` старым execution
кодом не поддерживаются, а в machine-readable context записывает эффективные
значения `1` и `unsupported-by-legacy`.

Это честное сравнение двух реально существующих реализаций, но не изоляция
только одного алгоритма при одинаковом числе DuckDB threads. Если нужен именно
такой эксперимент, current тоже следует запускать с
`FLIGHT_DUCKDB_THREADS=1`.
