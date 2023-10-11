package org.readingsending

import org.apache.spark.sql.SparkSession
import requests._

object Sending extends App {
  val spark = SparkSession.builder().appName("My Spark Application").master("local[*]").getOrCreate()
  while (true) {
    import spark.implicits._
    val apiUrl = "http://3.9.191.104:5000/api/data"
    val response = get(apiUrl, headers = headers)
    val total = response.text()
    val dfFromText = spark.read.json(Seq(total).toDS)

    // select the columns you want to include in the message

    val messageDF = dfFromText.select($"amount", $"id", $"isFlaggedFraud", $"isFraud", $"nameDest",
      $"nameOrig", $"newbalanceDest", $"newbalanceOrig", $"oldbalanceDest", $"oldbalanceOrg", $"step", $"type")

    val kafkaServer: String = "ip-172-31-13-101.eu-west-2.compute.internal:9092"
    val topicSampleName: String = "MahmoodTopic"

    messageDF.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value").selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").write.format("kafka").option("kafka.bootstrap.servers", kafkaServer).option("topic", topicSampleName).save()
    println("message is loaded to kafka topic")
    Thread.sleep(10000) // wait for 10 seconds before making the next call
  }
}
