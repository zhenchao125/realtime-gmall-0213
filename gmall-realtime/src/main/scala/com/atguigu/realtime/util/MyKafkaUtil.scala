package com.atguigu.realtime.util

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

/**
 * Author atguigu
 * Date 2020/7/14 16:39
 */
object MyKafkaUtil {
    
    val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> ConfigUtil.getProperty("config.properties", "kafka.servers"),
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> ConfigUtil.getProperty("config.properties", "group.id"),
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (true: java.lang.Boolean)
        
    )
    
    def getKafkaStream(ssc: StreamingContext, topic: String) = {
        println(kafkaParams)
        KafkaUtils
            .createDirectStream[String, String](
                ssc,
                PreferConsistent, // 标配
                Subscribe[String, String](Set(topic), kafkaParams)
            )
            .map(_.value())
    }
    
}
