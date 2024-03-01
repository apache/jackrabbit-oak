package org.apache.jackrabbit.oak.plugins.index.elasticsearch;

import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class ElasticsearchConnectionTest {

    @Test
    public void uniqueClient() throws IOException {
        ElasticsearchConnection connection = ElasticsearchConnection.defaultConnection.get();

        RestHighLevelClient client1 = connection.getClient();
        RestHighLevelClient client2 = connection.getClient();

        assertEquals(client1, client2);

        connection.close();
    }

    @Test(expected = IllegalStateException.class)
    public void alreadyClosedConnection() throws IOException {
        ElasticsearchConnection connection = ElasticsearchConnection.defaultConnection.get();
        connection.close();

        connection.getClient();
    }
}
