package www.elastic.co.elasticsearch;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @author Yeafel
 * 2019/9/11 17:18
 * Do or Die,To be a better man!
 */
public class ElasticSearchQuery {

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
     * query 查询
     * @throws IOException
     */
    @Test
    public void test1() throws IOException {
        SearchRequest searchRequest = new SearchRequest();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.//query(QueryBuilders.matchQuery("user", "kimchy"))
                    //多字段匹配某个内容，例如下面代码中 name和info字段中 出现 kimchy 的都会被匹配出来。
                    //query(QueryBuilders.multiMatchQuery("kimchy", "name","info"))
                    //根据字符串来匹配，如下代码，name字段中， tom1,tom2,tom3······都会被匹配到。
                    //query(QueryBuilders.queryStringQuery("name:tom*"))
                    //精准匹配，如下代码，name刚好等于tom才能被匹配。
                    query(QueryBuilders.termQuery("name","tom"))
                    //跟上后面两句分页操作
                    .from(0)
                    .size(10)
                    //跟上后面一句按 某字段 排序
                    .sort("age", SortOrder.DESC)
                    //跟上后面一句  过滤查询结果，范围内的数据才返回
                    .postFilter(QueryBuilders.rangeQuery("age").from(30).to(32))
                    //按查询匹配度
                    .explain(true);



        searchRequest.indices("posts");
        searchRequest.types("doc");
        searchRequest.searchType(SearchType.DFS_QUERY_THEN_FETCH);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        SearchHit[] hits2 = hits.getHits();
        for (SearchHit documentFields : hits2) {
            System.out.println(documentFields.getSourceAsString());
        }


    }
}
