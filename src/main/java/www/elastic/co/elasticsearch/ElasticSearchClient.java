package www.elastic.co.elasticsearch;

import com.sun.corba.se.spi.ior.IdentifiableFactory;
import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Test;

import java.io.IOException;

/**
 * @author Yeafel
 * 2019/9/11 14:39
 * Do or Die,To be a better man!
 */
public class ElasticSearchClient {

    /**
     * 测试： 使用RestHighLevelClient连接ElasticSearch集群
     * @throws IOException
     */
    @Test
    public void tese() throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(
                                "192.168.1.68",
                                9200,
                                "http")
                )
        );

        GetRequest getRequest = new GetRequest(
                "test",
                "user",
                "1"
        );

        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
        if (exists) {
            System.out.println("文档存在");
        } else {
            System.out.println("文档不存在");
        }

        client.close();
    }
}
