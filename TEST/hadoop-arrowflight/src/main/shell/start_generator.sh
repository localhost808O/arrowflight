spark-submit \
    --class net.surpin.data.arrowflight.server.DataGenerator \
    --master yarn \
    --deploy-mode client \
    --num-executors 320 \
    --executor-cores 8 \
    --executor-memory 16g \
    --driver-memory 16g \
    "hadoop-arrow-flight-1.0-SNAPSHOT.jar" \
    10000000000 /data/test_db/test_schema/test_300g_table 1000 \
    > generate_data.log 2>&1 &

