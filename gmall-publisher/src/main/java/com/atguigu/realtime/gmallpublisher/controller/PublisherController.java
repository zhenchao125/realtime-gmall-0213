package com.atguigu.realtime.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.realtime.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author lzc
 * @Date 2020/7/15 14:54
 */
@RestController
public class PublisherController {

    @Autowired
    PublisherService service;

    @GetMapping("/realtime-total")
    public String realtimeTotal(String date) {
        Long dau = service.getDau(date);
        // json字符串先用java的数据结构表示, 最后使用json序列化工具直接转成json字符串
        List<Map<String, String>> result = new ArrayList<>();

        Map<String, String> map1 = new HashMap<>();
        map1.put("id", "dau");
        map1.put("name", "新增日活");
        map1.put("value", dau.toString());
        result.add(map1);

        Map<String, String> map2 = new HashMap<>();
        map2.put("id", "new_mid");
        map2.put("name", "新增设备");
        map2.put("value", "223");
        result.add(map2);

        // {"id":"order_amount","name":"新增交易额","value":1000.2 }
        Map<String, String> map3 = new HashMap<>();
        map3.put("id", "order_amount");
        map3.put("name", "新增交易额");
        map3.put("value", service.getTotalAmount(date).toString());
        result.add(map3);
        return JSON.toJSONString(result);
    }

    @GetMapping("/realtime-hour")
    public String getRealtimeHour(String id, String date) {
        if ("dau".equals(id)) {
            Map<String, Long> today = service.getHourDau(date);
            Map<String, Long> yesterday = service.getHourDau(getYesterday(date));
            /*
            {"yesterday":{"11":383,"12":123,"17":88,"19":200 },
                "today":{"12":38,"13":1233,"17":123,"19":688 }}
             */
            Map<String, Map<String, Long>> result = new HashMap<>();
            result.put("today", today);
            result.put("yesterday", yesterday);

            return JSON.toJSONString(result);
        } else if("order_amount".equals(id)){ // http://localhost:8070/realtime-hour?id=order_amount&date=2020-02-14
            Map<String, Double> today = service.getHourAmount(date);
            Map<String, Double> yesterday = service.getHourAmount(getYesterday(date));
            /*
                {"yesterday":{"11":383,"12":123,"17":88,"19":200 },
                    "today":{"12":38,"13":1233,"17":123,"19":688 }}

            */
            Map<String, Map<String, Double>> result = new HashMap<>();
            result.put("today", today);
            result.put("yesterday", yesterday);
            return JSON.toJSONString(result);
        }else{
            return  null;
        }
    }

    /**
     * 返回昨天的年月日
     *
     * @param date
     * @return
     */
    private String getYesterday(String date) {

        return LocalDate.parse(date).plusDays(-1).toString();

    }

    //  	http://localhost:8070/sale_detail?date=2019-05-20&&startpage=1&&size=5&&keyword=手机小米
    @GetMapping("/sale_detail")
    public String saleDetail(String date, int startpage, int size, String keyword) throws IOException {
        Map<String, Object> genderAgg = service.getSaleDetailAndAgg(date, keyword, startpage, size, "user_gender", 2);
        Map<String, Object> ageAgg = service.getSaleDetailAndAgg(date, keyword, startpage, size, "user_age", 100);
        System.out.println(genderAgg);
        System.out.println(ageAgg);
        return "ok";
    }


}

/*
http://localhost:8070/realtime-hour?id=dau&date=2020-02-11
{"yesterday":{"11":383,"12":123,"17":88,"19":200 },
"today":{"12":38,"13":1233,"17":123,"19":688 }}





http://localhost:8070/realtime-total?date=2020-02-11

[{"id":"dau","name":"新增日活","value":1200},
{"id":"new_mid","name":"新增设备","value":233 },
{"id":"order_amount","name":"新增交易额","value":1000.2 }]




 */
