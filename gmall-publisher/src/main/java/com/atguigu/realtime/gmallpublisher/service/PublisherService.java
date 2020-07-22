package com.atguigu.realtime.gmallpublisher.service;

import java.io.IOException;
import java.util.Map;

public interface PublisherService {
    // 获取总的日活
    Long getDau(String date);

    /*
        数据层
        // List(Map("loghour": "10", count: 100), Map,.....)
        List<Map<String, Object>> getHourDau(String date);

        //  Map("10"->100, "11"->200. "12"->100)
     */
    Map<String, Long> getHourDau(String date);


    // 获取指定日期的销售总额
    Double getTotalAmount(String date);

    // 获取小时的销售额
    Map<String, Double> getHourAmount(String date);

    // 从es读数据, 返回需要的数据Controller
    /*
        "total": 200,
        "agg": Map("M"->100, "F" -> 100),
        "detail": List(Map(一样记录), Map(...))
     */
    Map<String, Object> getSaleDetailAndAgg(String date,
                               String keyword,
                               int startPage,
                               int sizePerPage,
                               String aggField,
                               int aggCount) throws IOException;


}
