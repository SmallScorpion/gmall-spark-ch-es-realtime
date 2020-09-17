package com.warehouse.gmall.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.warehouse.gmall.publisher.service.EsService;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {

    @Autowired
    EsService esService;

    /**
     * 总数
     * @param dt
     * @return
     */
    @RequestMapping(value = "realtime-total", method = RequestMethod.GET)
    public String realtimeTotal(@RequestParam("date") String dt) {

        List<Map<String, Object>> rsList = new ArrayList();

        // {"id":"dau","name":"新增日活","value":1200}
        Map<String, Object> dauMap = new HashMap();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        Long dauTotal = esService.getDauTotal(dt);
        if(dauTotal != null){
            dauMap.put("value", dauTotal);
        } else {
            dauMap.put("value", 0L);
        }

        // 添加
        rsList.add(dauMap);
        // {"id":"new_mid","name":"新增设备","value":233}
        Map<String, Object> newMap = new HashMap();
        newMap.put("id", "new_mid");
        newMap.put("name", "新增设备");
        newMap.put("value", 222);
        // 添加
        rsList.add(newMap);

        return JSON.toJSONString( rsList );
    }

    @GetMapping("realtime-hour")
    public String realtimeHour(@RequestParam("id") String id, @RequestParam("date") String dt){
        // 今天的
        Map dauHourMapTD = esService.getDauHour(dt);
        // 昨天
        String yd = getYd(dt);
        Map dauHourMapYD = esService.getDauHour(yd);

        Map<String, Map<String, Long>> rsMap = new HashMap();
        rsMap.put("yesterday", dauHourMapYD);
        rsMap.put("today", dauHourMapTD);
        return JSON.toJSONString(rsMap);
    }

    private String getYd(String today) {
        SimpleDateFormat dataFormat = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date todayDate = dataFormat.parse(today);
            Date ydDate = DateUtils.addDays(todayDate, -1);
            return dataFormat.format(ydDate);
        } catch (ParseException e) {
            e.printStackTrace();
            throw new RuntimeException("日期格式不正确");
        }
    }

}
