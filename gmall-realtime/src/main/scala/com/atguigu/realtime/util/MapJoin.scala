package com.atguigu.realtime.util

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/7/24 8:48
 */
object MapJoin {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("MapJoin").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        
        val rdd1 = sc.parallelize(List(("hello", 1), ("hello", 1), ("world", 1), ("atguigu", 1)))
        val rdd2 = sc.parallelize(List(("hello", 2), ("hello", 1), ("world", 2), ("world", 2), ("ABC", 2)))
        
        //        val result: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
        // map join: 其中小的rdd的数据拉到驱动, 把结果广播出去
        val bd: Broadcast[Array[(String, Int)]] = sc.broadcast(rdd2.collect())
        // 对比较大的rdd做map
        val result = rdd1.flatMap{
            case (k, v1) =>
                // ("hello", 1)
                val arr2: Array[(String, Int)] = bd.value
                arr2.filter(_._1 == k).map {
                    case (k, v2) =>
                        (k, (v1, v2))
                }
            
        }
        
        result.collect().foreach(println)
        
        sc.stop()
        
    }
}

/*
(hello,(1,2))
(hello,(1,1))
(hello,(1,2))
(hello,(1,1))
(world,(1,2))
(world,(1,2))

----

(hello,(1,2))
(hello,(1,1))
(hello,(1,2))
(hello,(1,1))
(world,(1,2))
(world,(1,2))
 */