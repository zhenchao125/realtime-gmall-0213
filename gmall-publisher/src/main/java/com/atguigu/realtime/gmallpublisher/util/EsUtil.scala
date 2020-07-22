package com.atguigu.realtime.gmallpublisher.util

import io.searchbox.client.JestClientFactory
import io.searchbox.client.config.HttpClientConfig

/**
 * Author atguigu
 * Date 2020/7/20 13:58
 */
object EsUtil {
    val esUrl = "http://hadoop102:9200"
    // 创建es客户端
    val factory = new JestClientFactory
    val conf = new HttpClientConfig.Builder(esUrl)
        .connTimeout(1000 * 10)
        .readTimeout(1000 * 10)
        .maxTotalConnection(100)
        .multiThreaded(true)
        .build()
    factory.setHttpClientConfig(conf)
    
    def getESClient() = factory.getObject
    
    def getDSl(date: String, keyword: String, aggField: String, aggSize: Int, startPage: Int, sizePerPage: Int) = {
        s"""
          |{
          |  "query": {
          |    "bool": {
          |      "filter": {
          |        "term": {
          |          "dt": "${date}"
          |        }
          |      },
          |      "must": [
          |        {"match": {
          |          "sku_name": "${keyword}"
          |        }}
          |      ]
          |    }
          |  },
          |  "aggs": {
          |    "group_by_${aggField}": {
          |      "terms": {
          |        "field": "${aggField}",
          |        "size": ${aggSize}
          |      }
          |    }
          |  },
          |  "from": ${(startPage - 1) * sizePerPage} ,
          |  "size": ${sizePerPage}
          |}
          |""".stripMargin
    }
    
}


