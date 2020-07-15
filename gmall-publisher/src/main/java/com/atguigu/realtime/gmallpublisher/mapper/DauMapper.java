package com.atguigu.realtime.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {
    // 得到当日总的日活
    Long getDau(String date);


    List<Map<String, Object>> getHourDau(String date);

    /*

    +----------+-----------+
    | LOGHOUR  | COUNT(1)  |
    +----------+-----------+
    | 10       | 44        |
    | 15       | 140       |
    +----------+-----------+


     */
}
