package com.atguigu.realtime.util

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/7/22 8:41
 */
object OptionDemo {
    def main(args: Array[String]): Unit = {
        val opt: Option[C] = foo()
        /*if (opt.isDefined) {
            println(opt.get)
        } else {
            println(opt)
        }*/
        
       /* opt match {
            case Some(c) =>
                println(c)
            case None =>
                println("none")
            
        }
        // s rdd  => option
        val conf = new SparkConf()
        val sc = new SparkContext(conf)
        val rdd1: RDD[(Int, Int)] = sc.parallelize(1 to 10).map(x => (x, 1))
        val rdd2: RDD[(Int, Int)] = sc.parallelize(1 to 10).map(x => (x, 2))
        val rdd3= rdd1.fullOuterJoin(rdd2)
        rdd3.map{
            case (k, (Some(a),Some(b)) ) =>
            
        }*/
    
        val e: Either[String, Long] = foo1()
        /*if(e.isRight){
            println(e.right.get)
        }else{
            println(e.left.get)
        }*/
        e match {
            case Left(s) =>
                println(s)
            case Right(v) =>
                println(v)
        }
        
    }
    
    def foo1(): Either[String, Long] = {
        Right(100)
//        Left("你算的不对")
    }
    
    def foo(): Option[C] = {
//        Option(C(10))
        // Some(C(20))
        //        None
        Option(null)
    }
    
    
}

case class C(age: Int)

/*
Option
    可以看成一个集合, 最多只能封装一个元素的集合
    
    语义: 表示这个值要么存在要么不存在
    
Either
    可以看成一个集合, 最多只能封装一个元素的集合
    
    语义: 表示这个值要么错误要么正确
    
    
 */