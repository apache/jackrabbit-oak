/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.index.search.spi.binary;

import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class FulltextBinaryTextExtractorTest {
    private final NodeState root = INITIAL_CONTENT;

    private final NodeBuilder builder = root.builder();
    private final ExtractedTextCache cache = new ExtractedTextCache(1000, 10000);

    @Test
    public void tikaConfigServiceLoader() throws Exception {
        IndexDefinition idxDefn = new IndexDefinition(root, builder.getNodeState(), "/foo");
        FulltextBinaryTextExtractor extractor = new FulltextBinaryTextExtractor(cache, idxDefn, false);
        assertTrue(extractor.getTikaConfig().getServiceLoader().isDynamic());
    }

    @Test
    public void concurrentTest() throws Exception {
        IndexDefinition idxDefn = new IndexDefinition(root, builder.getNodeState(), "/foo");
        FulltextBinaryTextExtractor extractor = new FulltextBinaryTextExtractor(cache, idxDefn, false);
        int THREAD_COUNT = 100;
        final int LOOP_COUNT = 10;
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(THREAD_COUNT);
        Exception[] firstException = new Exception[1];
        for (int i = 0; i < THREAD_COUNT; i++) {
            executorService.submit(() -> {
                try {
                    // wait for the signal
                    startLatch.await();
                    for (int j = 0; j < LOOP_COUNT; j++) {
                        extractor.isSupportedMediaType("image/png");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    firstException[0] = e;
                } finally {
                    doneLatch.countDown();
                }
            });
        }
        // signal
        startLatch.countDown();
        doneLatch.await();
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);
        if (firstException[0] != null) {
            throw firstException[0];
        }
    }

}