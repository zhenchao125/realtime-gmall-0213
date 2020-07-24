package com.atguigu.realtime.util

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
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
        
        //                val result = rdd1.rightOuterJoin(rdd2)
        // map join: 其中小的rdd的数据拉到驱动, 把结果广播出去
        val bd: Broadcast[Array[(String, Int)]] = sc.broadcast(rdd2.collect())
        // 对比较大的rdd做map
        /* val result = rdd1.flatMap{
             // ("hello", 1)
             case (k, v1) =>
                 val arr2: Array[(String, Int)] = bd.value
                 arr2.filter(_._1 == k).map {
                     case (k, v2) =>
                         (k, (v1, v2))
                 }
             
         }*/
        
        // leftjoin
        /*val result: RDD[(String, (Int, Option[Int]))] = rdd1.flatMap{
            // ("hello", 1)
            case (k, v1) =>
                val arr2: Array[(String, Int)] = bd.value
                if(!arr2.map(_._1).contains(k)){
                    Array((k, (v1, None)))
                }else{
                    arr2.filter(_._1 == k).map {
                        case (k, v2) =>
                            (k, (v1, Option(v2)))
                    }
                }
        }*/
        
        // 有问题
        val result: RDD[(String, (Option[Int], Int))] = rdd1.flatMap {
            // ("hello", 1)
            case (k1, v1) =>
                val arr: Array[(String, Int)] = bd.value
                arr.foreach(k2 => {
                    if(k1 != k2 &&  !arr.map(_._1).contains(k1)
                })
                
                null
                
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

----

(hello,(1,Some(2)))
(hello,(1,Some(1)))
(hello,(1,Some(2)))
(hello,(1,Some(1)))
(atguigu,(1,None))
(world,(1,Some(2)))
(world,(1,Some(2)))

----

(ABC,(None,2))
(hello,(Some(1),2))
(hello,(Some(1),1))
(hello,(Some(1),2))
(hello,(Some(1),1))
(world,(Some(1),2))
(world,(Some(1),2))

 */