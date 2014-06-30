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
package org.apache.jackrabbit.oak.kernel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.InputStream;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK.Builder;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests if cache is used for repeated reads on unmodified subtree.
 * See also OAK-591.
 */
public class KernelNodeStoreCacheTest extends AbstractKernelTest {

    private static final String PROP_FILTER = "{\"properties\":[\"*\"]}";
    private static final String PROP_FILTER_WITH_HASH = "{\"properties\":[\"*\",\":hash\"]}";
    private static final String PROP_FILTER_WITH_ID = "{\"properties\":[\"*\",\":id\"]}";

    private KernelNodeStore store;

    private MicroKernelWrapper wrapper;

    @Before
    public void setUp() throws Exception {
        wrapper = new MicroKernelWrapper(new Builder().open());
        store = new KernelNodeStore(wrapper);

        NodeBuilder builder = store.getRoot().builder();
        builder.child("a");
        NodeBuilder b = builder.child("b");
        b.child("c");
        b.child("d");
        b.child("e");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    /**
     * Provide both :hash and :id
     */
    @Test
    public void withDefaultFilter() throws Exception {
        int uncachedReads = readTreeWithCleanedCache();
        modifyContent();
        int cachedReads = readTreeWithCache();
        assertTrue("cachedReads: " + cachedReads + " uncachedReads: " + uncachedReads, 
                cachedReads < uncachedReads);
    }

    /**
     * Don't provide :hash nor :id. This will not reduce the number of
     * MK.getNodes() after a commit.
     */
    @Test
    public void withSimpleFilter() throws Exception {
        wrapper.filter = PROP_FILTER;
        int uncachedReads = readTreeWithCleanedCache();
        modifyContent();
        int cachedReads = readTreeWithCache();
        assertEquals("cachedReads: " + cachedReads + " uncachedReads: " + uncachedReads, 
                cachedReads, uncachedReads);
    }

    /**
     * Only provide :hash in MK.getNodes()
     */
    @Test
    public void withHashFilter() throws Exception {
        wrapper.filter = PROP_FILTER_WITH_HASH;
        int uncachedReads = readTreeWithCleanedCache();
        modifyContent();
        int cachedReads = readTreeWithCache();
        assertTrue("cachedReads: " + cachedReads + " uncachedReads: " + uncachedReads, 
                cachedReads < uncachedReads);
    }

    /**
     * Only provide :id in MK.getNodes()
     */
    @Test
    public void withIdFilter() throws Exception {
        wrapper.filter = PROP_FILTER_WITH_ID;
        int uncachedReads = readTreeWithCleanedCache();
        // System.out.println("Uncached reads: " + uncachedReads);

        modifyContent();

        int cachedReads = readTreeWithCache();

        // System.out.println("Cached reads: " + cachedReads);
        assertTrue("cachedReads: " + cachedReads + " uncachedReads: " + uncachedReads, 
                cachedReads < uncachedReads);
    }

    //---------------------------< internal >-----------------------------------

    private int readTreeWithCache() {
        NodeState root = store.getRoot();
        int cachedReads = wrapper.numGetNodes;
        readTree(root);
        return wrapper.numGetNodes - cachedReads;
    }

    private int readTreeWithCleanedCache() {
        // start with virgin store / empty cache
        store = new KernelNodeStore(wrapper);
        KernelNodeState root = store.getRoot();
        int uncachedReads = wrapper.numGetNodes;
        readTree(root);
        return wrapper.numGetNodes - uncachedReads;
    }

    private void modifyContent() throws Exception {
        NodeBuilder builder = store.getRoot().builder();
        builder.child("a").setProperty("foo", "bar");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private void readTree(NodeState root) {
        for (ChildNodeEntry cne : root.getChildNodeEntries()) {
            readTree(cne.getNodeState());
        }
    }

    private static final class MicroKernelWrapper implements MicroKernel {

        private final MicroKernel kernel;

        String filter = null;
        int numGetNodes = 0;

        MicroKernelWrapper(MicroKernel kernel) {
            this.kernel = kernel;
        }

        @Override
        public String getHeadRevision() throws MicroKernelException {
            return kernel.getHeadRevision();
        }

        @Override @Nonnull
        public String checkpoint(long lifetime) throws MicroKernelException {
            return kernel.checkpoint(lifetime);
        }

        @Override
        public String getRevisionHistory(long since,
                                         int maxEntries,
                                         String path)
                throws MicroKernelException {
            return kernel.getRevisionHistory(since, maxEntries, path);
        }

        @Override
        public String waitForCommit(String oldHeadRevisionId, long timeout)
                throws MicroKernelException, InterruptedException {
            return kernel.waitForCommit(oldHeadRevisionId, timeout);
        }

        @Override
        public String getJournal(String fromRevisionId,
                                 String toRevisionId,
                                 String path) throws MicroKernelException {
            return kernel.getJournal(fromRevisionId, toRevisionId, path);
        }

        @Override
        public String diff(String fromRevisionId,
                           String toRevisionId,
                           String path,
                           int depth) throws MicroKernelException {
            return kernel.diff(fromRevisionId, toRevisionId, path, depth);
        }

        @Override
        public boolean nodeExists(String path, String revisionId)
                throws MicroKernelException {
            return kernel.nodeExists(path, revisionId);
        }

        @Override
        public long getChildNodeCount(String path, String revisionId)
                throws MicroKernelException {
            return kernel.getChildNodeCount(path, revisionId);
        }

        @Override
        public String getNodes(String path,
                               String revisionId,
                               int depth,
                               long offset,
                               int maxChildNodes,
                               String filter) throws MicroKernelException {
            numGetNodes++;
            if (this.filter != null) {
                filter = this.filter;
            }
            return kernel.getNodes(path, revisionId, depth, offset, maxChildNodes, filter);
        }

        @Override
        public String commit(String path,
                             String jsonDiff,
                             String revisionId,
                             String message) throws MicroKernelException {
            return kernel.commit(path, jsonDiff, revisionId, message);
        }

        @Override
        public String branch(String trunkRevisionId)
                throws MicroKernelException {
            return kernel.branch(trunkRevisionId);
        }

        @Override
        public String merge(String branchRevisionId, String message)
                throws MicroKernelException {
            return kernel.merge(branchRevisionId, message);
        }

        @Nonnull
        @Override
        public String rebase(@Nonnull String branchRevisionId,
                             String newBaseRevisionId)
                throws MicroKernelException {
            return kernel.rebase(branchRevisionId, newBaseRevisionId);
        }

        @Nonnull
        @Override
        public String reset(@Nonnull String branchRevisionId,
                            @Nonnull String ancestorRevisionId)
                throws MicroKernelException {
            return kernel.reset(branchRevisionId, ancestorRevisionId);
        }

        @Override
        public long getLength(String blobId) throws MicroKernelException {
            return kernel.getLength(blobId);
        }

        @Override
        public int read(String blobId,
                        long pos,
                        byte[] buff,
                        int off,
                        int length) throws MicroKernelException {
            return kernel.read(blobId, pos, buff, off, length);
        }

        @Override
        public String write(InputStream in) throws MicroKernelException {
            return kernel.write(in);
        }

    }
}
