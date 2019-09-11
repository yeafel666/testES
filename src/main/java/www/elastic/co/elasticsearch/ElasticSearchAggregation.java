package www.elastic.co.elasticsearch;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @author Yeafel
 * 2019/9/11 17:45
 * Do or Die,To be a better man!
 */
public class ElasticSearchAggregation {

    public RestHighLevelClient client;

    /**
     * 测试：使用RestHighLevelClient连接ElasticSearch集群
     */
    @Before
    public void test0() {
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(
                                "192.168.1.68",
                                9200,
                                "http"
                        )
                )
        );
    }


    /**
     * 聚合查询
     * 分组按年龄统计
     */
    @Test
    public void test1() throws IOException {
        SearchRequest searchRequest = new SearchRequest();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder aggregation = AggregationBuilders.terms("by_age").field("age");
        searchSourceBuilder.query(QueryBuilders.matchAllQuery())
                            .aggregation(aggregation);

        searchRequest.indices("test");
        searchRequest.types("user");
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        
        //获取分组聚合后的信息
        Terms terms = searchResponse.getAggregations().get("by_age");
        for (Terms.Bucket bucket : terms.getBuckets()) {
            Object key = bucket.getKey();
            long docCount = bucket.getDocCount();
            System.out.println(key+"@"+docCount);
            //打印结果
            //17@1
            //27@1
            //52@1

        }

    }
}
