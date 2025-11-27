#!/bin/bash

SPARK_WORKLOAD=$1

echo "SPARK_WORKLOAD: $SPARK_WORKLOAD"
echo "Setting up Spark configuration..."

if [ "$SPARK_WORKLOAD" == "master" ];
then
  /bin/bash /opt/bitnami/spark/sbin/start-master.sh -p 7077

elif [[ $SPARK_WORKLOAD =~ "worker" ]];
# if $SPARK_WORKLOAD contains substring "worker". try 
# try "worker-1", "worker-2" etc.
then
  /bin/bash /opt/bitnami/spark/sbin/start-worker.sh spark://spark-master:7077

elif [ "$SPARK_WORKLOAD" == "history" ]
then
  /bin/bash /opt/bitnami/spark/sbin/start-history-server.sh

elif [ "$SPARK_WORKLOAD" == "thrift" ]
then
  sleep 10

  /bin/bash /opt/bitnami/spark/sbin/start-thriftserver.sh \
  --jars /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar,/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/bitnami/spark/jars/delta-spark_2.12-3.3.1.jar,/opt/bitnami/spark/jars/delta-storage-3.3.1.jar \
  --hiveconf hive.metastore.uris=thrift://hive-metastore:9083

fi

echo "Spark configuration complete."

tail -f /dev/null