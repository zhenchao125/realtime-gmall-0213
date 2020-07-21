package com.atguigu.realtime.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * Author atguigu
 * Date 2020/7/15 9:08
 */
object RedisUtil {
    val conf = new JedisPoolConfig
    conf.setMaxTotal(100)
    conf.setMaxIdle(30)
    conf.setMinIdle(10)
    conf.setBlockWhenExhausted(true)
    conf.setMaxWaitMillis(10000)
    conf.setTestOnCreate(true)
    conf.setTestOnBorrow(true)
    conf.setTestOnReturn(true)
    val pool = new JedisPool(conf, "hadoop102", 8000)
    
    def getClient = {
//        pool.getResource
        new Jedis("hadoop102", 8000)
    }
    
    
    def main(args: Array[String]): Unit = {
        val client: Jedis = getClient
        client.set("k1", "redis")
        client.close()  // 归还给连接池
    }
}
