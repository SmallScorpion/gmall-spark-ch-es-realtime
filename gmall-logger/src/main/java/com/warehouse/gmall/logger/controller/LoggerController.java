package com.warehouse.gmall.logger.controller;


import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

import static org.apache.logging.log4j.message.MapMessage.MapFormat.JSON;


@RestController // => Controller + ResponseBody
@Slf4j
public class LoggerController {
    // @ResponseBody 决定方法返回值是文本还是网页（渲染还是非渲染）

    @Autowired
    KafkaTemplate kafkaTemplate;

    @RequestMapping("/applog")
    public String appLog( @RequestBody String logString ){
        System.out.println(logString);

        // TODO: 输出成log文件
        log.info(logString);


        // TODO: 发送到kafka
        // 分流
        JSONObject jsonObject = com.alibaba.fastjson.JSON.parseObject(logString);
        if( jsonObject.getString( "start" ) != null && jsonObject.getString( "start" ).length() > 0 ){
            // 启动日志
            kafkaTemplate.send( "GMALL_SPARK_CK_ES_START", 4, UUID.randomUUID().toString(), logString);
        } else {
            // 事件日志
            kafkaTemplate.send( "GMALL_SPARK_CK_ES_EVENT", 4, UUID.randomUUID().toString(), logString);
        }


        return logString;
    }

}
