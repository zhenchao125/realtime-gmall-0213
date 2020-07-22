package com.atguigu.realtime.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.realtime.gmallpublisher.bean.Option;
import com.atguigu.realtime.gmallpublisher.bean.SaleInfo;
import com.atguigu.realtime.gmallpublisher.bean.Stat;
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

        // 1. 最终返回结果
        SaleInfo saleInfo = new SaleInfo();
        // 2. 设置总数
        saleInfo.setTotal((long)genderAgg.get("total"));
        // 3. 设置详情
        saleInfo.setDetail((List<Map>)genderAgg.get("detail"));
        // 4. 添加饼图
        // 4.1 性别的饼图
        Stat genderStat = new Stat();
        // 4.1.1 给性别的饼图设置title
        genderStat.setTitle("用户性别占比");
        // 4.1.2 向性别饼图插入选项
        Map<String, Long> agg1 = (Map<String, Long>) genderAgg.get("agg");
        for (String key : agg1.keySet()) {
            Option opt = new Option();
            opt.setName(key);
            opt.setValue(agg1.get(key));
            genderStat.addOption(opt);
        }
        saleInfo.addStat(genderStat);
        // 4.2 年龄段的饼图
        Stat ageStat = new Stat();
        ageStat.setTitle("用户年龄占比");
        ageStat.addOption(new Option("20岁以下", 0L));
        ageStat.addOption(new Option("20岁到30岁", 0L));
        ageStat.addOption(new Option("30岁以上", 0L));
        Map<String, Long> agg2 = (Map<String, Long>) ageAgg.get("agg");
        for (Map.Entry<String, Long> entry : agg2.entrySet()) {

            int age = Integer.parseInt(entry.getKey());
            System.out.println(age);
            if (age < 20) {
                Option opt = ageStat.getOptions().get(0);
                opt.setValue(opt.getValue() + entry.getValue());
            }else if(age < 30){
                Option opt = ageStat.getOptions().get(1);
                opt.setValue(opt.getValue() + entry.getValue());
            }else{
                Option opt = ageStat.getOptions().get(2);
                opt.setValue(opt.getValue() + entry.getValue());
            }
        }
        saleInfo.addStat(ageStat);

        return JSON.toJSONString(saleInfo);
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
