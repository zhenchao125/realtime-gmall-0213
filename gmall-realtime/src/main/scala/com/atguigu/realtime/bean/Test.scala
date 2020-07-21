package com.atguigu.realtime.bean

import com.alibaba.fastjson.JSON
import org.json4s.jackson.Serialization

import scala.beans.BeanProperty

/**
 * Author atguigu
 * Date 2020/7/21 11:56
 */
object Test {
    def main(args: Array[String]): Unit = {
        val a = A("abc", 20)
        // json4s  专门给sclaa准备的json工具
        import org.json4s.DefaultFormats
        val s = Serialization.write(a)(DefaultFormats)
        println(s)
    }
}
case class A(name: String, age: Int)
