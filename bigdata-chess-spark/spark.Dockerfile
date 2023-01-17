FROM gcr.io/spark-operator/spark:v3.1.1

# see https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/master/spark-docker/Dockerfile

ADD https://search.maven.org/remotecontent?filepath=org/apache/spark/spark-sql-kafka-0-10_2.11/2.4.8/spark-sql-kafka-0-10_2.11-2.4.8.jar $SPARK_HOME/jars
