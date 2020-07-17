package com.atguigu.realtime.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface OrderMapper {
    // 当日的销售总额
    Double getTotalAmount(String date);

    List<Map<String, Object>> getHourAmount(String date);
}
