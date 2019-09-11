package www.elastic.co.elasticsearch;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @author Yeafel
 * 2019/9/11 17:03
 * Do or Die,To be a better man!
 */
public class ElasticSearchSearchTypeAPI {

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
     * search API
     * @throws IOException
     */
    @Test
    public void test1() throws IOException {
        SearchRequest searchRequest = new SearchRequest();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.termQuery("field", "baz"));

        searchRequest.indices("posts");
        searchRequest.types("doc");
        searchRequest.source(searchSourceBuilder);
        searchRequest.searchType(SearchType.QUERY_THEN_FETCH);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        System.out.println(hits.getTotalHits());

        client.close();
    }
}
