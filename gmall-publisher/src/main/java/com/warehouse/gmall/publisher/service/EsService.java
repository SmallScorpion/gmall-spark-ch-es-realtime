package com.warehouse.gmall.publisher.service;

import org.springframework.stereotype.Service;

import java.util.Map;


public interface EsService {

    /**
     * 某一天的总数
     * @param date
     * @return
     */
    public Long getDauTotal( String date );

    /**
     * 某一天的每个小时数量
     * @param date
     * @return
     */
    public Map getDauHour(String date );


}
