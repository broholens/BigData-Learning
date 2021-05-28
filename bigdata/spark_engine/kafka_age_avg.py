# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.0.2 kafka_age_avg.py
# org.apache.spark.sql.sources.DataSourceRegister: Provider org.apache.spark.sql.kafka010.KafkaSourceProvider could not be instantiated
# https://www.waitingforcode.com/apache-spark-structured-streaming/analyzing-structured-streaming-kafka-integration-kafka-source/read
# https://stackoverflow.com/questions/60230124/pyspark-sql-utils-analysisexception-failed-to-find-data-source-kafka
# https://alphaoragroup.com/2021/05/27/py4j-protocol-py4jjavaerror-an-error-occurred-while-calling-o65-load-java-lang-nosuchmethoderror/
# https://stackoverflow.com/questions/54546513/py4jjavaerror-an-error-occurred-while-calling
# https://stackoverflow.com/questions/67496998/pyspark-kafka-org-apache-spark-sql-kafka010-kafkasourceprovider-could-not-be
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 /opt/BigData-Learning/bigdata/spark_engine/kafka_age_avg.py
# https://docs.databricks.com/delta/delta-streaming.html
"""
hadoop start cmd
cd /home/z/Downloads/hadoop-3.3.0
bin/hdfs namenode -format
sbin/start-dfs.sh
sbin/start-yarn.sh

hadoop stop cmd
sbin/stop-dfs.sh
sbin/stop-yarn.sh
"""

from pyspark import SparkConf
from pyspark.sql import SparkSession, functions
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

if __name__ == '__main__':

    conf = SparkConf()
    conf.setSparkHome('/home/z/spark/bin')

    spark = SparkSession \
        .builder \
        .appName("kafka_age_avg") \
        .config(conf=conf) \
        .getOrCreate()

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "student") \
        .load() \
        .selectExpr("CAST(value AS STRING)")

    student_schema = StructType([
        StructField('age', IntegerType()),
        StructField('gender', StringType()),
        StructField('name', StringType())
    ])

    df = df.select(
        functions.from_json(functions.col('value').cast('string'), student_schema).alias('parsed_value')
    ).select('parsed_value.age', 'parsed_value.gender', 'parsed_value.name')

    query = df \
        .writeStream.outputMode("update").format("console") \
        .start()

    query.awaitTermination()

