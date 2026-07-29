java \
--add-modules java.se \
--add-exports java.base/jdk.internal.ref=ALL-UNNAMED \
--add-opens java.base/java.lang=ALL-UNNAMED \
--add-opens java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens java.management/sun.management=ALL-UNNAMED \
--add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED \
-Dio.netty.tryReflectionSetAccessible=true \
net.surpin.data.arrowflight.HadoopArrowFlightServer \
--data-dir hdfs:///data/test_db/ \
--hosts [HOST_LIST] \
--localhost [LOCAL_HOST_DNS_NAME]