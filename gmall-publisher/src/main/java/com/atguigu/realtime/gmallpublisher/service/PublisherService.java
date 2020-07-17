package com.atguigu.realtime.gmallpublisher.service;

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
}
