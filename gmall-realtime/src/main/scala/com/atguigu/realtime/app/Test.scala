package com.atguigu.realtime.app

/**
 * Author atguigu
 * Date 2020/7/18 9:28
 */
object Test {
    def main(args: Array[String]): Unit = {
        
        val arr1 = Array(30, 50, 70, 60, 10, 20)
        
        arr1.foreach(e =>{
            try{
    
                if (e > 40) return
                println(e)
            }catch {
                case e => println(e)
            }
        })
        
        println("------------")
        
        
    }
}
