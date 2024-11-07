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

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.TrackingCorruptIndexHandler;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider.compose;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests marking index as corrupt if blob is missing.
 */
public class AsyncIndexUpdateCorruptMarkingTest {

    private final long INDEX_CORRUPT_INTERVAL_IN_MILLIS = 100;

    private MemoryBlobStore blobStore;

    protected Root root;

    private AsyncIndexUpdate asyncIndexUpdate;

    @Before
    public void before() throws Exception {
        ContentSession session = createRepository().login(null, null);
        root = session.getLatestRoot();
    }

    protected ContentRepository createRepository() {
        NodeStore nodeStore = new MemoryNodeStore();
        blobStore = new MemoryBlobStore();
        blobStore.setBlockSizeMin(48);//make it as small as possible

        LuceneIndexEditorProvider luceneIndexEditorProvider = new LuceneIndexEditorProvider();
        LuceneIndexProvider provider = new LuceneIndexProvider();
        luceneIndexEditorProvider.setBlobStore(blobStore);

        asyncIndexUpdate = new AsyncIndexUpdate("async", nodeStore, compose(List.of(
                luceneIndexEditorProvider,
                new NodeCounterEditorProvider()
        )));
        TrackingCorruptIndexHandler trackingCorruptIndexHandler = new TrackingCorruptIndexHandler();
        trackingCorruptIndexHandler.setCorruptInterval(INDEX_CORRUPT_INTERVAL_IN_MILLIS, TimeUnit.MILLISECONDS);
        asyncIndexUpdate.setCorruptIndexHandler(trackingCorruptIndexHandler);
        return new Oak(nodeStore)
                .with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with((QueryIndexProvider) provider)
                .with((Observer) provider)
                .with(luceneIndexEditorProvider)
                .with(new PropertyIndexEditorProvider())
                .with(new NodeTypeIndexProvider())
                .createContentRepository();
    }

    @Test
    public void testLuceneIndexSegmentStats() throws Exception {
        LuceneIndexDefinitionBuilder idxb = new LuceneIndexDefinitionBuilder();
        idxb.indexRule("nt:base")
                .property("foo").analyzed().nodeScopeIndex().ordered().useInExcerpt().propertyIndex();
        idxb.build(root.getTree("/oak:index").addChild("lucenePropertyIndex"));

        // Add content and index it successfully
        root.getTree("/").addChild("content").addChild("c1").setProperty("foo", "bar");
        root.commit();
        asyncIndexUpdate.run();
        assertFalse("Indexing must succeed without us intervening", asyncIndexUpdate.isFailing());

        // create some content and delete blobs such that the indexing run fails
        deleteBlobs();
        root.getTree("/content").addChild("c2").setProperty("foo", "bar");
        root.commit();
        asyncIndexUpdate.run(); // As blobs are deleted at this point index will be marked as bad.
        assertTrue("Indexing must fail after blob deletion", asyncIndexUpdate.isFailing());
        root.refresh();
        assertNull("Corrupt flag must not be set immediately after failure",
                root.getTree("/oak:index/lucenePropertyIndex").getProperty("corrupt"));

        // sleep to cross over corrupt interval and run indexing to maek corrupt
        Thread.sleep(INDEX_CORRUPT_INTERVAL_IN_MILLIS + 10);// 10ms buffer to definitely be ahead of corrupt interval
        asyncIndexUpdate.run(); // after corrupt interval index will be marked as corrupt.

        assertTrue("Indexing must continue to fail after blob deletion", asyncIndexUpdate.isFailing());
        root.refresh();
        assertNotNull("Corrupt flag must get set",
                root.getTree("/oak:index/lucenePropertyIndex").getProperty("corrupt"));
    }

    private void deleteBlobs() throws IOException {
        blobStore.clearInUse();
        blobStore.startMark();
        blobStore.sweep();
    }
}
