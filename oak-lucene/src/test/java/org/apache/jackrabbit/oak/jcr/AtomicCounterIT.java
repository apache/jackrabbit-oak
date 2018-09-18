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
package org.apache.jackrabbit.oak.jcr;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.jcr.SimpleCredentials;

import com.google.common.collect.ImmutableSet;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.hybrid.DocumentQueue;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static com.google.common.collect.ImmutableList.of;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.oak.api.Type.BOOLEAN;
import static org.apache.jackrabbit.oak.api.Type.LONG;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.newLuceneIndexDefinition;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.MIX_ATOMIC_COUNTER;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class AtomicCounterIT {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    private ContentSession session;

    @Before
    public void setup() throws Exception {
        DocumentNodeStore ns = builderProvider.newBuilder().getNodeStore();
        NodeBuilder builder = ns.getRoot().builder();
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        NodeBuilder lucene = newLuceneIndexDefinition(index, "lucene", ImmutableSet.of("String"), null, "async");
        lucene.setProperty("async", of("async", "nrt"), STRINGS);
        IndexDefinition.updateDefinition(index.child("lucene"));
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        LuceneIndexEditorProvider editorProvider = new LuceneIndexEditorProvider();
        editorProvider.setIndexingQueue(mock(DocumentQueue.class));
        LuceneIndexProvider provider = new LuceneIndexProvider();
        ContentRepository repository = new Oak(ns)
                .with(ns) // Clusterable
                .with(new OpenSecurityProvider())
                .with((QueryIndexProvider) provider)
                .with((Observer) provider)
                .with(editorProvider)
                .with(executorService)
                .withAtomicCounter()
                .withAsyncIndexing("async", 1)
                .withFailOnMissingIndexProvider()
                .createContentRepository();
        session = repository.login(
                new SimpleCredentials("admin", "admin".toCharArray()), null);
        while (isReindexing(session)) {
            Thread.sleep(100);
        }
    }

    @After
    public void close() throws Exception {
        session.close();
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.SECONDS);
    }

    @Test
    public void consolidate() throws Exception {
        Root root = session.getLatestRoot();
        Tree t = root.getTree("/");
        Tree counter = t.addChild("counter");
        counter.setProperty(JCR_MIXINTYPES, of(MIX_ATOMIC_COUNTER), NAMES);
        root.commit();

        root = session.getLatestRoot();
        counter = root.getTree("/counter");
        counter.setProperty("oak:increment", 1);
        counter.setProperty("foo", "bar");
        root.commit();

        for (int i = 0; i < 50; i++) {
            if (counterValue(session, counter.getPath()) != 0) {
                break;
            }
            Thread.sleep(100);
        }
        assertEquals(1, counterValue(session, counter.getPath()));
    }

    private static boolean isReindexing(ContentSession s) {
        Tree idx = s.getLatestRoot().getTree("/oak:index/lucene");
        return idx.getProperty("reindex").getValue(BOOLEAN);
    }

    private static long counterValue(ContentSession s, String path) {
        Tree counter = s.getLatestRoot().getTree(path);
        if (counter.hasProperty("oak:counter")) {
            return counter.getProperty("oak:counter").getValue(LONG);
        }
        return 0;
    }
}
