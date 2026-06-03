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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.LocalDate;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadPoolExecutor;

import org.junit.After;
import org.junit.Test;

import co.elastic.clients.elasticsearch.ElasticsearchClient;

public class ElasticConnectionTest {

    @Test
    public void ft_oak_12234_toggleShouldBeRemoved() {
        // Time-bombed: if this test fails, the feature toggle FT_OAK_12234 and its guard in
        // ElasticConnection#getResponseExecutor should be cleaned up — the fix has been in production long enough.
        assertTrue("Feature toggle " + ElasticConnection.FT_OAK_12234 + " is overdue for removal",
                LocalDate.now().isBefore(LocalDate.of(2027, 6, 2)));
    }

    @After
    public void resetToggle() {
        ElasticConnection.FT_OAK_12234_DISABLE.set(false);
        System.clearProperty(ElasticConnection.PROP_RESPONSE_THREAD_POOL_SIZE);
    }

    private static ElasticConnection defaultConnection() {
        return ElasticConnection.newBuilder()
                .withIndexPrefix("my+test")
                .withDefaultConnectionParameters()
                .build();
    }

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

    @Test
    public void responseExecutorIsSharedAndDecoupledFromCommonPool() throws IOException {
        try (ElasticConnection c1 = defaultConnection(); ElasticConnection c2 = defaultConnection()) {
            Executor executor = c1.getResponseExecutor();
            // the dedicated executor decouples async response processing from the common pool (OAK-12234)
            assertNotSame(ForkJoinPool.commonPool(), executor);
            // the pool is shared JVM-wide, so every connection hands out the same instance
            assertSame(executor, c2.getResponseExecutor());
        }
    }

    @Test
    public void responseExecutorFallsBackToCommonPoolWhenToggleDisabled() throws IOException {
        ElasticConnection.FT_OAK_12234_DISABLE.set(true);
        try (ElasticConnection connection = defaultConnection()) {
            assertSame(ForkJoinPool.commonPool(), connection.getResponseExecutor());
        }
    }

    @Test
    public void responseExecutorHonoursPoolSizeSystemProperty() {
        // the shared pool is created once per JVM, so verify the sizing via the factory directly
        System.setProperty(ElasticConnection.PROP_RESPONSE_THREAD_POOL_SIZE, "3");
        ExecutorService executor = ElasticConnection.createResponseExecutor();
        try {
            assertEquals(3, ((ThreadPoolExecutor) executor).getMaximumPoolSize());
        } finally {
            executor.shutdownNow();
        }
    }
}
