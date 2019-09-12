package www.elastic.co.elasticsearch;

import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
        client.close();
    }


    /**
     * 分组聚合2
     * 先按名称分组，再在此基础上按分数的总数进行分组。
     * 求出每个同学 的 总成绩为多少
     */
    @Test
    public void test2() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //按年龄分组聚合统计    注意： 如果字段类型为text，默认没有开启fieldData,要去该索引设置mapping,把需要聚合的字段设置为keyword才可以。
        TermsAggregationBuilder nameAggregation = AggregationBuilders.terms("by_name").field("name.keyword");

        SumAggregationBuilder scoreAggregation = AggregationBuilders.sum("by_score").field("score");

        //分数汇总是在 名称分组的基础下的
        nameAggregation.subAggregation(scoreAggregation);

        //只需要把名称聚合构造器传给 searchSourceBuilder即可，因为已经包含了分组聚合器
        searchSourceBuilder.query(QueryBuilders.matchAllQuery())
                .aggregation(nameAggregation);

        searchRequest.indices("test7");
        searchRequest.types("class");
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        //获取分组聚合后的信息
        Terms terms = searchResponse.getAggregations().get("by_name");
        for (Terms.Bucket bucket : terms.getBuckets()) {
            Sum sum = bucket.getAggregations().get("by_score");
            System.out.println(bucket.getKey()+":"+sum.getValue());
        }
        client.close();

    }


    /**
     * 支持多索引、多类型查询
     */
    @Test
    public void test3() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery())
                .from(0)
                .size(100);

        //searchRequest.indices("test");
        //多索引查询，多索引支持通配符
        //searchRequest.indices("test", "test2");
        searchRequest.indices("test*");

        //searchRequest.types("user");
        //多类型查询，多类型不支持通配符
        searchRequest.types("test", "class");
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        SearchHit[] hits2 = hits.getHits();
        for (SearchHit documentFields : hits2) {
            System.out.println(documentFields.getSourceAsString());
        }

        client.close();

    }


    /**
     * 极速查询插入测试数据
     */
    @Test
    public void test4() throws IOException {
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("phone", "13602456782");
        jsonMap.put("name", "tom");
        jsonMap.put("sex", "male");
        jsonMap.put("age", "16");

        Map<String, Object> jsonMap2 = new HashMap<>();
        jsonMap2.put("phone", "18912453678");
        jsonMap2.put("name", "john");
        jsonMap2.put("sex", "male");
        jsonMap.put("age", "28");

        Map<String, Object> jsonMap3 = new HashMap<>();
        jsonMap3.put("phone", "15212453678");
        jsonMap3.put("name", "lily");
        jsonMap3.put("sex", "female");
        jsonMap3.put("age", "18");

        BulkRequest request = new BulkRequest();
        request.add(new IndexRequest("test8", "user").routing(jsonMap.get("phone").toString().substring(0, 3)).source(jsonMap))
                .add(new IndexRequest("test8", "user").routing(jsonMap2.get("phone").toString().substring(0, 3)).source(jsonMap2))
                .add(new IndexRequest("test8", "user").routing(jsonMap3.get("phone").toString().substring(0, 3)).source(jsonMap3));

        BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
        if (bulkResponse.hasFailures()) {
            for (BulkItemResponse bulkItemResponse : bulkResponse) {
                if (bulkItemResponse.isFailed()) {
                    BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                    System.out.println(failure.getMessage());
                }
            }
        }


    }


    /**
     * 通过路由极速查询
     */
    @Test
    public void test5() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery())
                .from(0)
                .size(10);

        searchRequest.indices("test8");
        searchRequest.types("user");
        searchRequest.source(searchSourceBuilder);
        searchRequest.routing("18912453678".substring(0, 3));

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        SearchHit[] hits2 = hits.getHits();
        for (SearchHit documentFields : hits2) {
            System.out.println(documentFields.getSourceAsString());
        }

        client.close();

    }
}
