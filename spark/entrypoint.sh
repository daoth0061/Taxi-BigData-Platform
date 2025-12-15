#!/bin/bash

SPARK_WORKLOAD=$1

echo "SPARK_WORKLOAD: $SPARK_WORKLOAD"

echo "Downloading Hadoop AWS and AWS SDK JARs..."

cd /opt/spark/jars
wget -q "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
wget -q "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"
wget -q "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.3.1/delta-spark_2.12-3.3.1.jar"
wget -q "https://repo1.maven.org/maven2/io/delta/delta-storage/3.3.1/delta-storage-3.3.1.jar"
wget -q "https://jdbc.postgresql.org/download/postgresql-42.6.0.jar"
wget -q "https://repo1.maven.org/maven2/io/delta/delta-hive_2.12/3.3.1/delta-hive_2.12-3.3.1.jar"

echo "Setting up Spark configuration..."

if [ "$SPARK_WORKLOAD" == "master" ]; then

  /bin/bash /opt/spark/sbin/start-master.sh -p 7077 

elif [[ $SPARK_WORKLOAD =~ "worker" ]]; then

  /bin/bash /opt/spark/sbin/start-worker.sh spark://spark-master:7077

  pip install great_expectations==0.18.20

elif [ "$SPARK_WORKLOAD" == "history" ]; then

  /bin/bash /opt/spark/sbin/start-history-server.sh

elif [ "$SPARK_WORKLOAD" == "thrift" ]; then

  sleep 10
  /bin/bash /opt/spark/sbin/start-thriftserver.sh \
    --jars /opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/spark/jars/delta-spark_2.12-3.3.1.jar,/opt/spark/jars/delta-storage-3.3.1.jar,/opt/spark/jars/postgresql-42.6.0.jar,/opt/spark/jars/delta-hive_2.12-3.3.1.jar \
    --hiveconf hive.metastore.uris=thrift://hive-metastore:9083

fi

echo "Spark configuration complete."

tail -f /dev/null
