package org.readingsending
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Reading extends App {
  val spark = SparkSession.builder.appName("KafkaToJson").master("local[*]").getOrCreate()

  // Define the Kafka parameters
  val kafkaParams = Map(
    "kafka.bootstrap.servers" -> "ip-172-31-3-80.eu-west-2.compute.internal:9092",
    "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "group.id" -> "group1",
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> "false"
  )

  // Define the Kafka topic to subscribe to
  val kafkaTopic = "MahmoodTopic"

  // Define the schema for the JSON messages
  val schema = StructType(Seq(
    StructField("amount", StringType, nullable = true),
    StructField("id", StringType, nullable = true),
    StructField("isFlaggedFraud", StringType, nullable = true),
    StructField("isFraud", StringType, nullable = true),
    StructField("nameDest", StringType, nullable = true),
    StructField("nameOrig", StringType, nullable = true),
    StructField("newbalanceDest", StringType, nullable = true),
    StructField("newbalanceOrig", StringType, nullable = true),
    StructField("oldbalanceDest", StringType, nullable = true),
    StructField("oldbalanceOrg", StringType, nullable = true),
    StructField("step", StringType, nullable = true),
    StructField("type", StringType, nullable = true)
  ))

  // Read the JSON messages from Kafka as a DataFrame
  val df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "ip-172-31-13-101.eu-west-2.compute.internal:9092").option("subscribe", kafkaTopic).option("startingOffsets", "earliest").load().select(from_json(col("Value").cast("string"), schema).alias("data")).selectExpr("data.*")


  // Write the DataFrame as CSV files to HDFS
  df.writeStream.format("csv").option("checkpointLocation", "/tmp/jenkins/Mahmoud/Kafka/").option("path", "/tmp/jenkins/Mahmoud/Kafka/FraudApib").start().awaitTermination()

}
