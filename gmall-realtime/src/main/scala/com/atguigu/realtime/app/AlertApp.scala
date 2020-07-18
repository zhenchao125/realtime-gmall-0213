package com.atguigu.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.common.Constant
import com.atguigu.realtime.bean.{AlertInfo, EventLog}
import com.atguigu.realtime.util.MyKafkaUtil
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks._

/**
 * Author atguigu
 * Date 2020/7/18 8:52
 */
object AlertApp extends BaseApp {
    override def run(ssc: StreamingContext): Unit = {
        val eventLogStream = MyKafkaUtil.getKafkaStream(ssc, Constant.EVENT_TOPIC)
            .map(log => JSON.parseObject(log, classOf[EventLog]))
            .window(Minutes(5), Seconds(6)) // 给流添加窗口
        
        // 1. 安装设备id进行分组
        val eventLogGroupedStream = eventLogStream
            .map(event => (event.mid, event))
            .groupByKey
        // 2. 产生预警信息
        val alertInfoStream = eventLogGroupedStream.map {
            case (mid, eventLogIt) =>
                // eventLogIt 表示当前mid上5分钟内所有的事件
                val uidSet = new java.util.HashSet[String]()
                // 存储5分钟内在当前设备上所有的事件
                val eventList = new util.ArrayList[String]()
                // 存储优惠券对应的那些商品id
                val itemSet = new util.HashSet[String]()
                // 表示是否点击过商品
                var isClickItem = false
                breakable {
                    eventLogIt.foreach(log => {
                        // 把事件id添加到eventList
                        eventList.add(log.eventId)
                        // 只关注领取优惠券的用户
                        log.eventId match {
                            case "coupon" =>
                                uidSet.add(log.uid)  // 领取优惠券的用户
                                itemSet.add(log.itemId) // 优惠券对应的商品
                            
                            case "clickItem" =>
                                // 一旦出现浏览商品, 则不会再产生预警信息
                                isClickItem = true
                                break
                        }
                    })
                }
                
                // (是否预警, Alert(....))
                (!isClickItem && uidSet.size() >= 3, AlertInfo(mid, uidSet, itemSet, eventList, System.currentTimeMillis()))
        }
        
        // 3. 把数据写入到es中
        alertInfoStream
            .filter(_._1)
            .foreachRDD(rdd => {
                // ..../
                
            })
        
        
    }
}


/*
----
同一设备  ->   group by mid_id
5分钟内的数据, 每6秒统计一次 -> 窗口 窗口的长度: 5分钟  窗口的步长:6s

三次及以上用不同账号登录  -> 统计每个设备的登录的用户数
领取优惠劵  -> 统计领取优惠券的行为

并且在登录到领劵过程中没有浏览商品 -> 事件中没有浏览商品行为

----

同一设备，每分钟只记录一次预警。  -> 不在spark-streaming 完成, 让es来完成


// 1. reduceByKeyAndWindow
   2. 直接在流上使用window, 将来所有的操作都是基于这个窗口

*/