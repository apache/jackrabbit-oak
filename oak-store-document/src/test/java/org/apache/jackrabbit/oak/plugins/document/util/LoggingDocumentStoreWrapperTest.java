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
package org.apache.jackrabbit.oak.plugins.document.util;

import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LoggingDocumentStoreWrapperTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private LogCustomizer customizer = LogCustomizer.forLogger(LoggingDocumentStoreWrapper.class)
                                                    .create();

    @After
    public void after() {
        customizer.finished();
    }

    @Test
    public void withLogging() throws Exception {
        customizer.starting();
        DocumentNodeStore ns = builderProvider.newBuilder().setLogging(true).build();
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo");
        merge(ns, builder);
        assertFalse(customizer.getLogs().isEmpty());
    }

    @Test
    public void loggingWithPrefix() throws Exception {
        String logPrefix = "testPrefix";
        customizer.starting();
        DocumentNodeStore ns = builderProvider.newBuilder().setLogging(true)
                                              .setLoggingPrefix(logPrefix).build();
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo");
        merge(ns, builder);
        assertFalse(customizer.getLogs().isEmpty());
        int prefixCount = 0;
        // Check there are log lines starting with the prefix
        for (String log : customizer.getLogs()) {
            if (log.startsWith(logPrefix)) {
                prefixCount++;
            }
        }
        assertTrue(prefixCount > 0);
    }

    @Test
    public void loggingWithPrefix2() {
        String logPrefix = "testPrefix";
        customizer.starting();
        DocumentStore store = new LoggingDocumentStoreWrapper(new MemoryDocumentStore()).withPrefix(
            logPrefix);
        store.find(Collection.NODES, "some-id");
        assertFalse(customizer.getLogs().isEmpty());
        int prefixCount = 0;
        // Check there are log lines starting with the prefix
        for (String log : customizer.getLogs()) {
            if (log.startsWith(logPrefix)) {
                prefixCount++;
            }
        }
        assertTrue(prefixCount > 0);
    }
}
