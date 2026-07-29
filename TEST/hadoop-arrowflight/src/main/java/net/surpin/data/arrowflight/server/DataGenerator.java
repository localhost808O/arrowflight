package net.surpin.data.arrowflight.server;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * Генератор данных для тестирования Arrow Flight SQL.
 * Генерирует большие объемы данных без shuffle операций.
 *
 * Использование:
 *   spark-submit --class server.net.surpin.data.arrowflight.DataGenerator <jar> <num_rows> <output_path> <num_partitions>
 *   Пример: spark-submit --class server.net.surpin.data.arrowflight.DataGenerator target/hadoop-arrow-flight.jar 10000000000 hdfs:///data/vsurpin/test_db/test_1tb_table 2000
 */
public class DataGenerator {

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: DataGenerator <num_rows> <output_path> <num_partitions>");
            System.err.println("Example: DataGenerator 10000000000 hdfs:///data/vsurpin/test_db/test_1tb_table 2000");
            System.exit(1);
        }

        long numRows = Long.parseLong(args[0]);
        String outputPath = args[1];
        int numPartitions = Integer.parseInt(args[2]);

        SparkConf conf = new SparkConf()
                .setAppName("DataGenerator")
                .setMaster("local[*]")
                .set("spark.sql.committer-protocol-class", "org.apache.spark.internal.io.FileCommitProtocol")
                .set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
                .set("spark.sql.legacy.replaceDatabricksSparkAvro.allowUnsupportedTypes", "true")
                .set("spark.sql.adaptive.enabled", "true")
                .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .set("spark.sql.adaptive.skewJoin.enabled", "true")
                .set("spark.sql.adaptive.localShuffleReader.enabled", "true");

        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        try {
            // Генерируем данные БЕЗ SHUFFLE:
            // 1. spark.range() создает DataFrame с уникальными ID без shuffle (range partitioning)
            // 2. Данные распределяются по partitionам без data shuffle
            Dataset<Row> df = spark.range(0, numRows, 1, numPartitions)
                    .select(
                            // id: уникальные значения от 0 до numRows-1
                            functions.col("id").cast("integer").alias("id"),
                            // bool_col: true, если id чётное
                            functions.col("id").mod(2).equalTo(0).alias("bool_col"),
                            // tinyint_col, smallint_col, int_col: остаток от деления id на 10
                            functions.col("id").mod(10).cast("tinyint").alias("tinyint_col"),
                            functions.col("id").mod(10).cast("smallint").alias("smallint_col"),
                            functions.col("id").mod(10).cast("integer").alias("int_col"),
                            // bigint_col: остаток от деления на 10, умноженный на 10
                            functions.col("id").mod(10).multiply(10).cast("bigint").alias("bigint_col"),
                            // float_col: случайное float значение от 0 до 1
                            functions.rand(47).cast("float").alias("float_col"),
                            // double_col: случайное double значение от 0 до 1
                            functions.rand(48).cast("double").alias("double_col"),
                            // date_string_col: случайная дата в формате "yyyy-MM-dd"
                            functions.concat(
                                    functions.lit("20"),
                                    functions.format_string("%02d", functions.floor(functions.rand(49).multiply(24)).plus(1).cast("integer")),
                                    functions.lit("-"),
                                    functions.format_string("%02d", functions.floor(functions.rand(50).multiply(12)).plus(1).cast("integer")),
                                    functions.lit("-"),
                                    functions.format_string("%02d", functions.floor(functions.rand(51).multiply(28)).plus(1).cast("integer"))
                            ).alias("date_string_col"),
                            // string_col: случайная строка в формате "str_<num>"
                            functions.concat(
                                    functions.lit("str_"),
                                    functions.lpad(functions.rand(52).multiply(1000000).cast("integer").cast("string"), 6, "0")
                            ).alias("string_col"),
                            // timestamp_col: случайный timestamp как bigint (в миллисекундах)
                            functions.floor(functions.rand(53).multiply(1000000000000L)).cast("bigint").alias("timestamp_col"),
                            // year: случайный год от 2000 до 2024
                            functions.floor(functions.rand(54).multiply(25)).plus(2000).cast("integer").alias("year"),
                            // month: случайный месяц от 1 до 12
                            functions.floor(functions.rand(55).multiply(12)).plus(1).cast("integer").alias("month")
                    );

            // Сохраняем в Parquet (перезаписываем если существует)
            df.write()
                    .mode("overwrite")
                    .format("parquet")
                    .save(outputPath);

            // Считаем количество строк (только для подтверждения)
            long count = df.count();
            System.out.println("===========================================");
            System.out.println("Генерация завершена!");
            System.out.println("Rows generated: " + count);
            System.out.println("Output path: " + outputPath);
            System.out.println("Partitions: " + df.rdd().getNumPartitions());
            System.out.println("===========================================");

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            spark.stop();
        }
    }
}
