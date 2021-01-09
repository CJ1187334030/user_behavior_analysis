package com.atguigu.user_behavior_analysis.utils

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object MykafkaCon {


  val properties = new Properties()
  properties.setProperty("bootstrap.servers", "192.168.30.131:9092")
  properties.setProperty("group.id", "consumer-group")
  properties.setProperty("key.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer")
  properties.setProperty("value.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer")
  properties.setProperty("auto.offset.reset", "latest")

  def getKafkaCon(topic:String) ={

    val kafkaCon = new FlinkKafkaConsumer[String](topic,new SimpleStringSchema(),properties)

    kafkaCon

  }

}
