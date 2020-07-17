package com.atguigu.canal

import java.net.{InetAddress, InetSocketAddress}
import java.util

import com.alibaba.fastjson.JSONObject
import com.alibaba.otter.canal.client.{CanalConnector, CanalConnectors}
import com.alibaba.otter.canal.protocol.CanalEntry.{EventType, RowChange}
import com.alibaba.otter.canal.protocol.{CanalEntry, Message}
import com.google.protobuf.ByteString

import scala.collection.JavaConversions._
/**
 * Author atguigu
 * Date 2020/7/17 11:16
 */
object CanalClient {
    
    def handleData(rowDatas: util.List[CanalEntry.RowData], tableName: String, eventType: CanalEntry.EventType) = {
        if(tableName == "order_info" && eventType == EventType.INSERT && rowDatas != null && !rowDatas.isEmpty){
            for(rowData <- rowDatas){
                // 变化后的列
                // mysql中的一行, 到kafka的时候是一条
                val obj = new JSONObject()
                val columnList: util.List[CanalEntry.Column] = rowData.getAfterColumnsList
                for( column <- columnList){
                    // id: 100  total_amount: 1000.2
                    val key: String = column.getName
                    val value: String = column.getValue
                    obj.put(key, value)
                }
                println(obj.toJSONString)
            }
        }
    }
    
    def main(args: Array[String]): Unit = {
        // 1. 连接canal
        val address = new InetSocketAddress("hadoop102", 11111)
        val connector: CanalConnector = CanalConnectors.newSingleConnector(address, "example", "", "")
        connector.connect()  // 连接canal服务器
        // 2. 拉取数据
        // 2.1 订阅想读的数据
        connector.subscribe("gmall0213.*")
        // 2.2 拉取
        while(true){
            // 100表示最多拉取由于100条sql导致变化的数据.
            // 所有的数据封装到一个Message中
            val message: Message = connector.get(100)
            // 3. 解析数据
            val entries: util.List[CanalEntry.Entry] = message.getEntries
            
            if(entries != null && !entries.isEmpty){
                for(entry <- entries){
                    // entry的类型必须是ROWDATA
                    if(entry != null && entry.hasEntryType && entry.getEntryType == CanalEntry.EntryType.ROWDATA){
                        val value: ByteString = entry.getStoreValue
                        val rowChange: RowChange = RowChange.parseFrom(value)
                        // 所有行变化的数据
                        val rowDatas: util.List[CanalEntry.RowData] = rowChange.getRowDatasList
                        
                        handleData(rowDatas, entry.getHeader.getTableName, rowChange.getEventType)
                        
                    }
                }
                
                
                
            }else{  // 没有拉取到数据
                System.out.println("没有拉取到数据, 3s后继续拉取....");
                Thread.sleep(3000)  // 休眠3秒后继续拉取数据
            }
            

        }
        
        
       
        
        // 4. 解析后的数据, 组成json字符串, 写入到kafka
        
        
    }
}
