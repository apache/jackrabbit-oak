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
package org.apache.jackrabbit.oak.plugins.index.luceneNg;

import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.directory.OakDirectory;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

/**
 * Tests for LuceneNgIndexNode acquire/release/close lifecycle.
 */
public class LuceneNgIndexNodeTest {

    private static NodeState buildIndexWithData(String indexPath) throws Exception {
        NodeBuilder builder = InitialContentHelper.INITIAL_CONTENT.builder();
        NodeBuilder indexDef = builder.child("oak:index").child("testIndex");
        indexDef.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

        String indexName = indexPath.substring(indexPath.lastIndexOf('/') + 1);
        NodeBuilder storageBuilder = indexDef.child(LuceneNgIndexStorage.STORAGE_NODE_NAME);

        OakDirectory directory = new OakDirectory(storageBuilder, indexName, false);
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
            writer.commit();
        }
        directory.close();

        return builder.getNodeState();
    }

    private static LuceneNgIndexNode openNode(NodeState root, String indexPath) {
        NodeState indexState = root.getChildNode("oak:index").getChildNode("testIndex");
        return new LuceneNgIndexNode(indexPath, root, indexState);
    }

    @Test
    public void acquireReturnsNonNullWhenDataExists() throws Exception {
        NodeState root = buildIndexWithData("/oak:index/testIndex");
        LuceneNgIndexNode node = openNode(root, "/oak:index/testIndex");
        try {
            LuceneNgIndexNode.AcquiredNode acquired = node.acquire();
            assertNotNull("acquire() must return non-null when index data exists", acquired);
            assertNotNull("AcquiredNode must expose a searcher", acquired.getSearcher());
            assertNotNull("AcquiredNode must expose a definition", acquired.getDefinition());
            acquired.release();
        } finally {
            node.close();
        }
    }

    @Test
    public void acquireReturnsNullAfterClose() throws Exception {
        NodeState root = buildIndexWithData("/oak:index/testIndex");
        LuceneNgIndexNode node = openNode(root, "/oak:index/testIndex");
        node.close();
        assertNull("acquire() must return null after node is closed", node.acquire());
    }

    @Test
    public void releaseIsIdempotent() throws Exception {
        NodeState root = buildIndexWithData("/oak:index/testIndex");
        LuceneNgIndexNode node = openNode(root, "/oak:index/testIndex");
        try {
            LuceneNgIndexNode.AcquiredNode acquired = node.acquire();
            assertNotNull(acquired);
            acquired.release();
            // second release must not throw
            acquired.release();
        } finally {
            node.close();
        }
    }

    @Test
    public void closeBlocksUntilAllAcquiredNodesAreReleased() throws Exception {
        NodeState root = buildIndexWithData("/oak:index/testIndex");
        LuceneNgIndexNode node = openNode(root, "/oak:index/testIndex");

        LuceneNgIndexNode.AcquiredNode acquired = node.acquire();
        assertNotNull(acquired);

        CountDownLatch closeDone = new CountDownLatch(1);
        AtomicReference<Throwable> closeError = new AtomicReference<>();

        Thread closeThread = new Thread(() -> {
            try {
                node.close();
            } catch (Throwable t) {
                closeError.set(t);
            } finally {
                closeDone.countDown();
            }
        });
        closeThread.start();

        // Give the close thread time to reach the write-lock acquisition
        Thread.sleep(100);
        assertEquals("close() must block while a node is still acquired", 1, closeDone.getCount());

        // Releasing the acquired node allows close() to proceed
        acquired.release();
        assertTrue("close() must complete after all acquired nodes are released",
                closeDone.await(2, TimeUnit.SECONDS));
        assertNull("close() must not throw", closeError.get());
    }
}
