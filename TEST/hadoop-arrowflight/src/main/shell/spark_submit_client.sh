SPARK_DRIVER_EXTRAJAVAOPTIONS="--add-opens=java.base/java.nio=ALL-UNNAMED --add-exports java.base/sun.nio.ch=ALL-UNNAMED --add-exports java.base/sun.security.action=ALL-UNNAMED" \
SPARK_EXECUTOR_EXTRAJAVAOPTIONS="--add-opens=java.base/java.nio=ALL-UNNAMED --add-exports java.base/sun.nio.ch=ALL-UNNAMED --add-exports java.base/sun.security.action=ALL-UNNAMED" \
SPARK_MAJOR_VERSION=3 \
spark-submit \
    --master yarn \
    --deploy-mode client \
    --num-executors 4 \
    --executor-cores 2 \
    --executor-memory 4g \
    --driver-memory 2g \
    --conf "spark.executorEnv.JAVA_HOME=/usr/lib/jvm/java-17/" \
    --conf "spark.yarn.appMasterEnv.JAVA_HOME=/usr/lib/jvm/java-17/" \
    --conf "spark.driver.userClassPathFirst=true" \
    --conf "spark.executor.userClassPathFirst=true" \
    --class net.surpin.data.arrowflight.SparkArrowClient \
    ./hadoop-arrow-flight-1.0-SNAPSHOT.jar \
    --server [SERVER_URL]