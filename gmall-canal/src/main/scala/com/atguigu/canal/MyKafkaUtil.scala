package com.atguigu.canal

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
 * Author atguigu
 * Date 2020/7/17 14:02
 */
object MyKafkaUtil {
    
    val pops = new Properties()
    pops.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    pops.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    pops.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    
    val producer = new KafkaProducer[String, String](pops)
    
    def sendToKafka(topic: String, content: String) = {
        producer.send(new ProducerRecord[String, String](topic, content))
    }
}
