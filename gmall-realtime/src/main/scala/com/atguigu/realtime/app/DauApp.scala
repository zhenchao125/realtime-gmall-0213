package com.atguigu.realtime.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.common.Constant
import com.atguigu.realtime.bean.StartupLog
import com.atguigu.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

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
        // 2.1 把每个json字符串的数据,父封装到一个样例类对象中
        val startupLogStream = sourceStream.map(json => JSON.parseObject(json, classOf[StartupLog]))
        // 3. 去重  过滤掉一件启动的那些设备的记录  从redis去读取已经启动过的设备的id
        val filteredStartupLogStream = startupLogStream.transform(rdd => {
            // 3.1 先去读redis的数据   mid都存在一个 set集合中, "mids:" + ...
            val client: Jedis = RedisUtil.getClient
            val mids = client.smembers(Constant.STARTUP_TOPIC + ":" + new SimpleDateFormat("yyyy-MM-dd").format(new Date()))
            client.close() // 归还连接池
            // 3.2 把集合做一个广播变量
            val midsBD = ssc.sparkContext.broadcast(mids)
            // 返回那些没有启动过的设备的启动记录
            rdd
                .filter(startupLog => !midsBD.value.contains(startupLog.mid))
                // 如果一个设备在他第一次启动的批次中有多次启动记录, 则无法过滤
                .map(log => (log.mid, log))
                .groupByKey()
                .map {
                    case (_, logs) =>
                        logs.toList.sortBy(_.ts).head
                }
            
        })
        
        // 3.3 把第一次启动的记录写入到redis中
        filteredStartupLogStream.foreachRDD(rdd => {
            // rdd的数据写入到redis, 只需要写mid就行了
            // 一个分区一个分区的写
            rdd.foreachPartition((logs: Iterator[StartupLog]) => {
                val client: Jedis = RedisUtil.getClient
                logs.foreach(log => {
                    client.sadd(Constant.STARTUP_TOPIC + ":" + log.logDate, log.mid)
                })
                client.close()
            })
            // 4. 数据写入到hbse中  当天启动的设备的第一条启动记录
            import org.apache.phoenix.spark._
            //
            rdd.saveToPhoenix(
                "GMALL_DAU",
                Seq("MID", "UID", "APPID", "AREA", "OS", "CHANNEL", "LOGTYPE", "VERSION", "TS", "LOGDATE", "LOGHOUR"),
                zkUrl = Some("hadoop102,hadoop103,hadoop104:2181"))
        })
        filteredStartupLogStream.print(10000)
        
        // 6. 开启流
        ssc.start()
        // 7. 阻止main退出
        ssc.awaitTermination()
    }
}
