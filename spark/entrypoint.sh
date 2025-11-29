#!/bin/bash

SPARK_WORKLOAD=$1

echo "SPARK_WORKLOAD: $SPARK_WORKLOAD"
echo "Setting up Spark configuration..."

if [ "$SPARK_WORKLOAD" == "master" ];
then
  /bin/bash /opt/bitnami/spark/sbin/start-master.sh -p 7077

elif [[ $SPARK_WORKLOAD =~ "worker" ]];
then
  /bin/bash /opt/bitnami/spark/sbin/start-worker.sh spark://spark-master:7077
fi

echo "Spark configuration complete."

tail -f /dev/null