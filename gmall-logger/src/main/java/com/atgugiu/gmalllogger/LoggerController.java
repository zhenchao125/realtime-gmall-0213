package com.atgugiu.gmalllogger;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Author lzc
 * @Date 2020/7/13 16:40
 */
/*@Controller
@ResponseBody*/
@RestController  // 等价于 @Controller + @ResponseBody
public class LoggerController {
    // http://localhost:8080/log?log=abc

    @PostMapping("/log")
    public String doLog(String log){
        //1. 给日志添加一个时间戳

        // 2. 数据落盘(为离线数据做准备)

        // 3. 把数据写入到kafka, 需要写入到topic


        return "ok";
    }


}
