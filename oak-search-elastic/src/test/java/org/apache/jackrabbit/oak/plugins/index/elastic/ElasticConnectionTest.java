/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.elastic;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

import co.elastic.clients.elasticsearch.ElasticsearchClient;

public class ElasticConnectionTest {

    @Test
    public void uniqueClient() throws IOException {
        ElasticConnection connection = ElasticConnection.newBuilder()
                .withIndexPrefix("my+test")
                .withDefaultConnectionParameters()
                .build();
        
        ElasticsearchClient client1 = connection.getClient();
        ElasticsearchClient client2 = connection.getClient();
        
        assertEquals(client1, client2);

        connection.close();
    }

    @Test(expected = IllegalStateException.class)
    public void alreadyClosedConnection() throws IOException {
        ElasticConnection connection = ElasticConnection.newBuilder()
                .withIndexPrefix("my.test")
                .withDefaultConnectionParameters()
                .build();

        connection.close();

        connection.getClient();
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptyIndexPrefix() {
        ElasticConnection.newBuilder()
                .withIndexPrefix("")
                .withDefaultConnectionParameters()
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void indexPrefixStartingWithNotAllowedChars() {
        ElasticConnection.newBuilder()
                .withIndexPrefix(".cannot_start_with_dot")
                .withDefaultConnectionParameters()
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void indexPrefixWithNotAllowedChars() {
        ElasticConnection.newBuilder()
                .withIndexPrefix("cannot_have_*_chars")
                .withDefaultConnectionParameters()
                .build();
    }
}
