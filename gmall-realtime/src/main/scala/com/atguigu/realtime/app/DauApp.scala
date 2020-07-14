package com.atguigu.realtime.app

import com.atguigu.common.Constant
import com.atguigu.realtime.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author atguigu
 * Date 2020/7/14 16:34
 */
object DauApp {
    def main(args: Array[String]): Unit = {
        // 1. 创建一个StreamingContext
        val conf = new SparkConf().setMaster("local[2]").setAppName("DauApp")
        val ssc = new StreamingContext(conf, Seconds(3))
        // 2. 获取一个流
        val sourceStream: DStream[String] = MyKafkaUtil.getKafkaStream(ssc, Constant.STARTUP_TOPIC)
        sourceStream.print(1000)
        
        // 3. 去重
        
        // 4. 数据写入到hbse中
        
        // 6. 开启流
        ssc.start()
        // 7. 阻止main退出
        ssc.awaitTermination()
    }
}
