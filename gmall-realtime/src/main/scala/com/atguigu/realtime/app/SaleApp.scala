package com.atguigu.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.common.Constant
import com.atguigu.realtime.bean.{OrderDetail, OrderInfo}
import com.atguigu.realtime.util.MyKafkaUtil
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
 * Author atguigu
 * Date 2020/7/21 9:26
 */
object SaleApp extends BaseApp {
    
    
    override def run(ssc: StreamingContext): Unit = {
        val (orderInfoStream, orderDetailStream) = getOrderInfoAndOrderDetailStream(ssc)
        
        fullJoin(orderInfoStream, orderDetailStream)
        
        
    }
    
    def fullJoin(orderInfoStream: DStream[OrderInfo], orderDetailStream: DStream[OrderDetail]) = {
        // select ... a join b  on ..=..
        // join: 流必须是kv形式, k就是他们的连接条件
        val orderIdAndOrderInfoStream = orderInfoStream
            .map(info => (info.id, info))
        val orderIdAndOrderDetailStream = orderDetailStream
            .map(detail => (detail.order_id, detail))
        
        //val value: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] =
        orderIdAndOrderInfoStream
            .fullOuterJoin(orderIdAndOrderDetailStream)
            .map {
                case (orderId, (Some(orderInfo), Some(orderDetail))) =>
                
                case (orderId, (Some(orderInfo), None)) =>
                
                case (orderId, (None, Some(orderDetail))) =>
                
            }
        
        
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
