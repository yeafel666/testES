package www.elastic.co.elasticsearch;

import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Yeafel
 * 2019/9/11 14:55
 * Do or Die,To be a better man!
 */
public class ElasticSearchDocumentAPIs {

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

    @Test
    public void test1() throws IOException {
        IndexRequest request = new IndexRequest(
                "posts",
                "doc",
                "1"
        );
        String jsonString = "{\n" +
                "\t\n" +
                "\t\"user\" : \"kimchy\",\n" +
                "\t\"postDate\" : \"2013-01-30\",\n" +
                "\t\"message\" : \"trying out ElasticSearch\"\n" +
                "}";
        request.source(jsonString, XContentType.JSON);

        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);

        System.out.println(indexResponse.getId());

        client.close();
    }


    /**
     * 方法二：  index   api   hashMap
     * @throws IOException
     */
    @Test
    public void test2() throws IOException {
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("user", "kimchy");
        jsonMap.put("postDate", new Date());
        jsonMap.put("message", "trying out ElasticSearch");
        IndexRequest request = new IndexRequest("posts", "doc", "2")
                .source(jsonMap);

        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);

        System.out.println(indexResponse.getId());

        client.close();
    }


    /**
     * 使用 XContentBuilder 创建数据
     * @throws IOException
     */
    @Test
    public void test3() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("user", "kimchy");
            builder.timeField("postDate", new Date());
            builder.field("message", "trying out ElasticSearch");
        }
        builder.endObject();
        IndexRequest request = new IndexRequest("posts", "doc", "3")
                .source(builder);

        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);

        System.out.println(indexResponse.getId());

        client.close();
    }


    /**
     * index api   object方式创建数据
     * @throws IOException
     */
    @Test
    public void test4() throws IOException {
        IndexRequest request = new IndexRequest("posts", "doc", "4")
                .source("user", "kimchy",
                        "postDate", new Date(),
                        "message", "trying out ElasticSearch"
                );

        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);

        System.out.println(indexResponse.getId());

        client.close();
    }


    /**
     * get api
     * @throws IOException
     */
    @Test
    public void test5() throws IOException {
        GetRequest getRequest = new GetRequest(
                "posts",
                "doc",
                "1"
        );

        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);

        System.out.println(getResponse.getSource());

        client.close();
    }







}
