package com.warehouse.gmall.publisher.service.impl;

import com.warehouse.gmall.publisher.service.EsService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.lang.model.element.VariableElement;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class EsServiceImpl implements EsService {

    // 添加ES
    @Autowired
    JestClient jestClient;

    @Override
    public Long getDauTotal(String date) {
        // es index
        String indexName = "gmall_ch_dau_info" + date + "-query";
        // 查询语句
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query( new MatchAllQueryBuilder());

        // 构建查询
        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex(indexName)
                .addType("_doc")
                .build();

        try {
            // 执行查询
            SearchResult searchResult = jestClient.execute(search);
            // 获取总数
            return searchResult.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("es 查询异常");
        }

    }

    @Override
    public Map getDauHour(String date) {

        // es index
        String indexName = "gmall_ch_dau_info" + date + "-query";

        // 查询语句
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermsBuilder aggbuilder = AggregationBuilders.terms("groupby_hr")
                .field("hr")
                .size(24);
        searchSourceBuilder.aggregation(aggbuilder);

        // 构建查询
        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex(indexName)
                .addType("_doc")
                .build();

        try {
            SearchResult searchResult = jestClient.execute(search);
            Map<String, Long> aggMap = new HashMap();

            if(  searchResult.getAggregations().getTermsAggregation("groupby_hr") != null  ){

                // 获取结果
                List<TermsAggregation.Entry> buckets = searchResult.getAggregations()
                        .getTermsAggregation("groupby_hr")
                        .getBuckets();

                // 转换结构
                for (TermsAggregation.Entry bucket : buckets) {
                    aggMap.put(bucket.getKey(),bucket.getCount());
                }
            }


            return aggMap;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("es 查询异常");
        }

    }
}
