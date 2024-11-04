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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import ch.qos.logback.classic.Level;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.ActiveDeletedBlobCollectorFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;

import static org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider.compose;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests AsyncIndexStatsUpdateCallback works as scheduled callback
 */
public class AsyncIndexStatsUpdateCallbackTest {
    private int SCHEDULED_CALLBACK_TIME_IN_MILLIS = 1000; //1 second
    protected Root root;
    private AsyncIndexUpdate asyncIndexUpdate;
    private LuceneIndexEditorProvider luceneIndexEditorProvider;
    private LogCustomizer customLogger;
    private AsyncIndexesSizeStatsUpdateImpl asyncIndexesSizeStatsUpdate =
            new AsyncIndexesSizeStatsUpdateImpl(SCHEDULED_CALLBACK_TIME_IN_MILLIS);
    private LuceneIndexMBeanImpl mBean = Mockito.mock(LuceneIndexMBeanImpl.class);

    @Before
    public void before() throws Exception {
        customLogger = LogCustomizer
                .forLogger(
                        "org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexStatsUpdateCallback")
                .enable(Level.DEBUG).create();
        ContentSession session = createRepository().login(null, null);
        Mockito.when(mBean.getDocCount("/oak:index/lucenePropertyIndex")).thenReturn("100");
        Mockito.when(mBean.getSize("/oak:index/lucenePropertyIndex")).thenReturn("100");
        root = session.getLatestRoot();
    }

    @After
    public void shutDown() {
        customLogger.finished();
    }

    protected ContentRepository createRepository() throws IOException {
        NodeStore nodeStore = new MemoryNodeStore();
        luceneIndexEditorProvider = new LuceneIndexEditorProvider(null, null,
                null,
                null, Mounts.defaultMountInfoProvider(),
                ActiveDeletedBlobCollectorFactory.NOOP, mBean, StatisticsProvider.NOOP);

        asyncIndexUpdate = new AsyncIndexUpdate("async", nodeStore, compose(List.of(
                luceneIndexEditorProvider,
                new NodeCounterEditorProvider()
        )), StatisticsProvider.NOOP, false);
        return new Oak(nodeStore)
                .with(new InitialContent())
                .with(new OpenSecurityProvider())
                .createContentRepository();
    }

    @Test
    public void testCallbackWorksAsScheduled() throws Exception {
        luceneIndexEditorProvider.withAsyncIndexesSizeStatsUpdate(asyncIndexesSizeStatsUpdate);
        LuceneIndexDefinitionBuilder idxb = new LuceneIndexDefinitionBuilder();
        idxb.indexRule("nt:base")
                .property("foo").analyzed().nodeScopeIndex().ordered().useInExcerpt().propertyIndex();
        idxb.build(root.getTree("/oak:index").addChild("lucenePropertyIndex"));

        // Add content and index it successfully
        root.getTree("/").addChild("content").addChild("c1").setProperty("foo", "bar");
        root.commit();
        customLogger.starting();
        asyncIndexUpdate.run();
        List<String> logs = customLogger.getLogs();
        assertTrue(logs.size() == 1);
        root.getTree("/content").addChild("c2").setProperty("foo", "bar");
        root.commit();
        asyncIndexUpdate.run();
        assertTrue(logs.size() == 1);
        root.getTree("/content").addChild("c3").setProperty("foo", "bar");
        root.commit();
        Thread.sleep(2000);
        asyncIndexUpdate.run();
        assertTrue(logs.size() == 2);
        validateLogs(logs);
    }

    @Test
    public void testNOOPDonotPerformCallbackStatsUpdate() throws Exception {
        luceneIndexEditorProvider.withAsyncIndexesSizeStatsUpdate(AsyncIndexesSizeStatsUpdate.NOOP);
        LuceneIndexDefinitionBuilder idxb = new LuceneIndexDefinitionBuilder();
        idxb.indexRule("nt:base")
                .property("foo").analyzed().nodeScopeIndex().ordered().useInExcerpt().propertyIndex();
        idxb.build(root.getTree("/oak:index").addChild("lucenePropertyIndex"));

        // Add content and index it successfully
        root.getTree("/").addChild("content").addChild("c1").setProperty("foo", "bar");
        root.commit();
        customLogger.starting();
        asyncIndexUpdate.run();
        List<String> logs = customLogger.getLogs();
        assertTrue(logs.size() == 0);
        root.getTree("/content").addChild("c2").setProperty("foo", "bar");
        root.commit();
        asyncIndexUpdate.run();
        assertTrue(logs.size() == 0);
        root.getTree("/content").addChild("c3").setProperty("foo", "bar");
        root.commit();
        Thread.sleep(2000);
        asyncIndexUpdate.run();
        assertTrue(logs.size() == 0);
        //validateLogs(logs);
    }


    private void validateLogs(List<String> logs) {
        for (String log : logs) {
            assertThat("logs were recorded by custom logger", log,
                    containsString("/oak:index/lucenePropertyIndex stats updated; docCount 100, size 100, timeToUpdate"));
        }
    }
}
