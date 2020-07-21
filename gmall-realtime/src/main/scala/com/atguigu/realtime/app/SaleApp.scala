package com.atguigu.realtime.app

import java.util
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.atguigu.common.Constant
import com.atguigu.realtime.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.realtime.util.{EsUtil, MyKafkaUtil, RedisUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis

import scala.collection.mutable

/**
 * Author atguigu
 * Date 2020/7/21 9:26
 */
object SaleApp extends BaseApp {
    
    override def run(ssc: StreamingContext): Unit = {
        val (orderInfoStream, orderDetailStream) = getOrderInfoAndOrderDetailStream(ssc)
        // 双流join
        var saleDetailStream: DStream[SaleDetail] = fullJoin(orderInfoStream, orderDetailStream)
        // 把user信息join进去: 根据user_id去mysql中反向查询到user的数据
        saleDetailStream = joinUser(saleDetailStream, ssc)
        // 把信息吸入到es
        saleDetailStream.foreachRDD(rdd => {
            rdd.foreachPartition(it => {
                EsUtil.insertBulk("gmall_sale_detail", it.map(detail => (detail.order_id + "_" + detail.order_detail_id, detail)))
            })
        })
        
        saleDetailStream.print(100)
    }
    
    def joinUser(saleDetailStream: DStream[SaleDetail], ssc: StreamingContext) = {
        val url = "jdbc:mysql://hadoop102:3306/gmall0213?useSSL=false"
        val tableName = "user_info"
        val props = new Properties()
        
        def readUserInfo() = {
            val spark: SparkSession = SparkSession.builder()
                .config(ssc.sparkContext.getConf)
                .getOrCreate()
            import spark.implicits._
            spark.read.jdbc(url, tableName, props)
                .as[UserInfo]
                .rdd
        }
        
        
        props.setProperty("user", "root")
        props.setProperty("password", "aaaaaa")
        
        // 用spark-sql从mysql读数据
        // 1. 先得到Spark-session
        
        saleDetailStream.transform((rdd: RDD[SaleDetail]) => {
            // 1. 拿到用户信息
            val userInfoRDD = readUserInfo().map(user => (user.id, user))
            // 2. saleDetail编程kv, 然后和User join 补齐user信息
            val detailRDD: RDD[(String, SaleDetail)] = rdd.map(detail => (detail.user_id, detail))
            detailRDD
                .join(userInfoRDD)
                .map {
                    case (userId, (saleDetail, userInfo)) =>
                        saleDetail.mergeUserInfo(userInfo)
                }
        })
    }
    
    
    def fullJoin(orderInfoStream: DStream[OrderInfo], orderDetailStream: DStream[OrderDetail]) = {
        // 把oderInfo的信息缓存到redis中
        def cacheOderInfoToRedis(orderInfo: OrderInfo, client: Jedis) = {
            // orderInfo信息在30分钟后自动删除
            client.setex("orderInfo:" + orderInfo.id, 60 * 30, Serialization.write(orderInfo)(DefaultFormats))
        }
        
        def cacheOderDetailToRedis(orderDetail: OrderDetail, client: Jedis) = {
            client.setex("orderDetail:" + orderDetail.order_id + ":" + orderDetail.id, 60 * 30, Serialization.write(orderDetail)(DefaultFormats))
        }
        
        // select ... a join b  on ..=..
        // join: 流必须是kv形式, k就是他们的连接条件
        val orderIdAndOrderInfoStream: DStream[(String, OrderInfo)] = orderInfoStream
            .map(info => (info.id, info))
        val orderIdAndOrderDetailStream: DStream[(String, OrderDetail)] = orderDetailStream
            .map(detail => (detail.order_id, detail))
        
        //val value: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] =
        orderIdAndOrderInfoStream
            .fullOuterJoin(orderIdAndOrderDetailStream)
            .mapPartitions((it: Iterator[(String, (Option[OrderInfo], Option[OrderDetail]))]) => {
                // 连接redis
                val client: Jedis = RedisUtil.getClient
                // 读写
                val result = it.flatMap {
                    case (orderId, (Some(orderInfo), Some(orderDetail))) =>
                        println("some some....")
                        // 1. 先把orderInfo的数据写缓存, 因为他对应的OrderDetail的信息有可能延迟
                        cacheOderInfoToRedis(orderInfo, client)
                        // 2. 把orderInfo和orderDetail的数据合并到一起
                        val saleDetail = SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
                        // 3. 去OrderDetail对应的缓存中, 找到这个orderId都应的所有orderDetail信息
                        /*
                        orderDetail缓存
                        key                                                     value(string)
                        "orderDetail:" + order_id + : +  order_detail_id        把orderDetail信息转成json存储
                        
                        orderDetail:1:1
                        orderDetail:1:2
                        orderDetail:1:3  ...
                         */
                        import scala.collection.JavaConversions._
                        val keys: util.Set[String] = client.keys("orderDetail:" + orderId + ":*")
                        val orderDetails = keys.map(key => {
                            val orderDetail: OrderDetail = JSON.parseObject(client.get(key), classOf[OrderDetail])
                            client.del(key) // OrderDetail缓存的数据, 一旦join后, 必须删除, 否则会出现重复数据
                            SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
                        })
                        orderDetails += saleDetail
                        orderDetails // 每个case的返回值必须是集合
                    case (orderId, (Some(orderInfo), None)) =>
                        println("some none....")
                        cacheOderInfoToRedis(orderInfo, client)
                        import scala.collection.JavaConversions._
                        val keys: util.Set[String] = client.keys("orderDetail:" + orderId + ":*")
                        keys.map(key => {
                            val orderDetail: OrderDetail = JSON.parseObject(client.get(key), classOf[OrderDetail])
                            client.del(key) // OrderDetail缓存的数据, 一旦join后, 必须删除, 否则会出现重复数据
                            SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
                        })
                    case (orderId, (None, Some(orderDetail))) =>
                        println("none some....")
                        //1.先orderInfo的缓存找对应的orderInfo
                        val orderInfoString = client.get("orderInfo:" + orderId)
                        if (orderInfoString == null) { // orderInfo滞后
                            // 缓存orderDetail
                            cacheOderDetailToRedis(orderDetail, client)
                            mutable.Set[SaleDetail]()
                        } else {
                            println(orderInfoString)
                            val orderInfo = JSON.parseObject(orderInfoString, classOf[OrderInfo])
                            // join在一起,. 不需要缓存orderDetail
                            val saleDetail = SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
                            mutable.Set[SaleDetail](saleDetail)
                        }
                }
                // 关闭redis
                client.close()
                result
            })
        
        
    }
    
    
    /**
     * 获取OrderDetail和orderInfo的两个流
     *
     * @param ssc
     * @return
     */
    private def getOrderInfoAndOrderDetailStream(ssc: StreamingContext): (DStream[OrderInfo], DStream[OrderDetail]) = {
        val orderInfoStream = MyKafkaUtil
            .getKafkaStream(ssc, Constant.TOPIC_ORDER_INFO)
            .map(info => JSON.parseObject(info, classOf[OrderInfo]))
        
        val orderDetailStream: DStream[OrderDetail] = MyKafkaUtil
            .getKafkaStream(ssc, Constant.TOPIC_ORDER_DETAIL)
            .map(detail => JSON.parseObject(detail, classOf[OrderDetail]))
        (orderInfoStream, orderDetailStream)
    }
}

/*
orderInfo缓存
key                                     value(string)
"orderInfo:" + orderId                把orderInfo信息转成json存储



orderDetail缓存
key                                                                                     value(string)
"orderDetail:" + order_id + : +  order_detail_id                                        把orderDetail信息转成json存储





 */