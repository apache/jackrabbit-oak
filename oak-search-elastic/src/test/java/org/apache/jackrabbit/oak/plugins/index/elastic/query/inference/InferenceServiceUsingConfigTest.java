/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.elastic.query.inference;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class InferenceServiceUsingConfigTest {

    private static final List<Float> EMBEDDING = List.of(0.1f, 0.2f, 0.3f);

    @Rule
    public WireMockRule wireMock = new WireMockRule(WireMockConfiguration.options().dynamicPort().disableRequestJournal());

    @Test
    public void cacheHitIsNotAffectedByEviction() {
        AtomicReference<InferenceServiceUsingConfig> service = new AtomicReference<>();
        AtomicBoolean evicted = new AtomicBoolean();
        // a cache hit of another query evicts the entry ("b" replaces "a" in the single-entry cache)
        service.set(createService(new InferenceServiceMetrics(StatisticsProvider.NOOP, "eviction-test", 1) {
            @Override
            public void cacheHit() {
                super.cacheHit();
                if (evicted.compareAndSet(false, true)) {
                    service.get().embeddings("b");
                }
            }
        }));
        assertEquals(EMBEDDING, service.get().embeddings("a"));
        assertEquals(EMBEDDING, service.get().embeddings("a"));
        assertTrue(evicted.get());
    }

    @Test
    public void concurrentCacheHitsAndEvictions() throws Exception {
        InferenceServiceUsingConfig service = createService(
                new InferenceServiceMetrics(StatisticsProvider.NOOP, "concurrency-test", 1));
        long end = System.currentTimeMillis() + 2000;
        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < 8; t++) {
                // one thread keeps replacing the cached entry, the others read it
                boolean inserter = t == 0;
                futures.add(executor.submit(() -> {
                    for (int i = 0; System.currentTimeMillis() < end; i++) {
                        String text = inserter ? (i % 2 == 0 ? "b" : "c") : "a";
                        assertEquals(EMBEDDING, service.embeddings(text));
                    }
                    return null;
                }));
            }
            for (Future<?> future : futures) {
                future.get(60, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    /**
     * Creates a service with a single-entry cache, so that every insertion evicts the cached entry.
     */
    private InferenceServiceUsingConfig createService(InferenceServiceMetrics metrics) {
        wireMock.stubFor(WireMock.post("/v1/embeddings").willReturn(WireMock.okJson(
                "{\"data\":[{\"embedding\":[0.1,0.2,0.3]}]}")));
        NodeBuilder config = EmptyNodeState.EMPTY_NODE.builder();
        config.setProperty(InferenceConstants.TYPE, InferenceModelConfig.TYPE);
        config.setProperty(InferenceConstants.ENABLED, true);
        config.setProperty(InferenceModelConfig.EMBEDDING_SERVICE_URL, "http://localhost:" + wireMock.port() + "/v1/embeddings");
        config.setProperty(InferenceModelConfig.CACHE_SIZE, 1);
        config.setProperty(InferenceModelConfig.TIMEOUT, 30_000L);
        config.child(InferenceModelConfig.HEADER).setProperty("Content-Type", "application/json");
        config.child(InferenceModelConfig.INFERENCE_PAYLOAD).setProperty("input", "");
        return new InferenceServiceUsingConfig(new InferenceModelConfig("model", config.getNodeState()), metrics);
    }
}
