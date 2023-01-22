All you need is to install zeppelin using this helm chart:

https://artifacthub.io/packages/helm/duyet/zeppelin

No need to install spark separately. Zeppelin is able to automatically create pods using spark-submit (that is a feature of spark-submit integration with k8s)

Add this to be able to read from s3:
org.apache.hadoop:hadoop-aws:3.1.1