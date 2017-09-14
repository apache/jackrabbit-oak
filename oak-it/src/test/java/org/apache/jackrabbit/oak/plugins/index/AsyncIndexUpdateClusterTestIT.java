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
package org.apache.jackrabbit.oak.plugins.index;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;

public class AsyncIndexUpdateClusterTestIT {

    private DocumentNodeStore ns1;
    private DocumentNodeStore ns2;

    private MemoryDocumentStore ds;
    private MemoryBlobStore bs;

    private Random random = new Random();
    private final List<String> values = ImmutableList.of("a", "b", "c", "d",
            "e");

    private Closer closer = Closer.create();
    private final AtomicBoolean illegalReindex = new AtomicBoolean(false);

    @Before
    public void before() throws Exception {
        ns1 = create(0);
        ns2 = create(1);
    }

    @After
    public void after() {
        shutdown();
        ns1.dispose();
        ns2.dispose();
        assertFalse("Reindexing should not happen", illegalReindex.get());
    }

    private void shutdown() {
        try {
            closer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void missingCheckpointDueToEventualConsistency() throws Exception {
        IndexStatusListener l = new IndexStatusListener();

        AsyncIndexUpdate async1 = createAsync(ns1, l);
        closer.register(async1);
        AsyncIndexUpdate async2 = createAsync(ns2, l);
        closer.register(async2);

        // Phase 1 - Base setup - Index definition creation and
        // performing initial indexing
        // Create index definition on NS1
        NodeBuilder b1 = ns1.getRoot().builder();
        createIndexDefinition(b1);
        ns1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // Trigger indexing on NS1
        async1.run();
        // make sure initial index is visible on both cluster nodes
        ns1.runBackgroundOperations();
        ns2.runBackgroundOperations();
        l.initDone();

        ScheduledExecutorService executorService = Executors
                .newScheduledThreadPool(5);
        closer.register(new ExecutorCloser(executorService));

        executorService.scheduleWithFixedDelay(async1, 1, 3, TimeUnit.SECONDS);
        executorService.scheduleWithFixedDelay(async2, 1, 2, TimeUnit.SECONDS);
        executorService.scheduleWithFixedDelay(
                new PropertyMutator(ns1, "node1"), 500, 500,
                TimeUnit.MILLISECONDS);
        executorService.scheduleWithFixedDelay(
                new PropertyMutator(ns2, "node2"), 500, 500,
                TimeUnit.MILLISECONDS);

        for (int i = 0; i < 4 && !illegalReindex.get(); i++) {
            TimeUnit.SECONDS.sleep(5);
        }
        shutdown();
    }

    private static AsyncIndexUpdate createAsync(DocumentNodeStore ns,
            final IndexStatusListener l) {
        IndexEditorProvider p = new TestEditorProvider(
                new PropertyIndexEditorProvider(), l);
        AsyncIndexUpdate aiu = new AsyncIndexUpdate("async", ns, p) {
            protected boolean updateIndex(NodeState before,
                    String beforeCheckpoint, NodeState after,
                    String afterCheckpoint, String afterTime,
                    AsyncUpdateCallback callback) throws CommitFailedException {
                if (MISSING_NODE == before) {
                    l.reindexing();
                }
                return super.updateIndex(before, beforeCheckpoint, after,
                        afterCheckpoint, afterTime, callback);
            }
        };
        aiu.setCloseTimeOut(1);
        return aiu;
    }

    private static void createIndexDefinition(NodeBuilder builder) {
        IndexUtils.createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async");
    }

    private DocumentNodeStore create(int clusterId) {
        DocumentMK.Builder builder = new DocumentMK.Builder();
        if (ds == null) {
            ds = new MemoryDocumentStore();
        }
        if (bs == null) {
            bs = new MemoryBlobStore();
        }
        builder.setDocumentStore(ds).setBlobStore(bs);

        DocumentNodeStore store = builder.setClusterId(++clusterId)
                .setLeaseCheck(false).open().getNodeStore();
        return store;
    }

    private class PropertyMutator implements Runnable {
        private final NodeStore nodeStore;
        private final String nodeName;

        public PropertyMutator(NodeStore nodeStore, String nodeName) {
            this.nodeStore = nodeStore;
            this.nodeName = nodeName;
        }

        @Override
        public void run() {
            NodeBuilder b = nodeStore.getRoot().builder();
            b.child(nodeName).setProperty("foo",
                    values.get(random.nextInt(values.size())));
            try {
                nodeStore.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            } catch (CommitFailedException e) {
                e.printStackTrace();
            }
        }
    }

    private class IndexStatusListener {

        boolean reindexOk = true;

        public void reindexing() {
            if (!reindexOk) {
                illegalReindex.set(true);
                shutdown();
            }
        }

        public void initDone() {
            reindexOk = false;
        }

        public void waitRandomly() {
            try {
                TimeUnit.SECONDS.sleep(random.nextInt(1));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static class TestEditorProvider implements IndexEditorProvider {
        private final IndexEditorProvider delegate;
        private final IndexStatusListener listener;

        private TestEditorProvider(IndexEditorProvider delegate,
                IndexStatusListener listener) {
            this.delegate = delegate;
            this.listener = listener;
        }

        @Override
        public Editor getIndexEditor(@Nonnull String type,
                @Nonnull NodeBuilder definition, @Nonnull NodeState root,
                @Nonnull IndexUpdateCallback callback)
                throws CommitFailedException {
            Editor e = delegate
                    .getIndexEditor(type, definition, root, callback);
            if (e != null) {
                e = new TestEditor(e, listener);
            }
            return e;
        }
    }

    private static class TestEditor implements Editor {
        private final Editor editor;
        private final TestEditor parent;
        private final IndexStatusListener listener;

        TestEditor(Editor editor, IndexStatusListener listener) {
            this(editor, listener, null);
        }

        TestEditor(Editor editor, IndexStatusListener listener,
                TestEditor parent) {
            this.editor = editor;
            this.listener = listener;
            this.parent = parent;
        }

        @Override
        public void enter(NodeState before, NodeState after)
                throws CommitFailedException {
            if (MISSING_NODE == before && parent == null) {
                listener.reindexing();
            }
            editor.enter(before, after);
        }

        @Override
        public void leave(NodeState before, NodeState after)
                throws CommitFailedException {
            listener.waitRandomly();
            editor.leave(before, after);
        }

        @Override
        public void propertyAdded(PropertyState after)
                throws CommitFailedException {
            editor.propertyAdded(after);
        }

        @Override
        public void propertyChanged(PropertyState before, PropertyState after)
                throws CommitFailedException {
            editor.propertyChanged(before, after);
        }

        @Override
        public void propertyDeleted(PropertyState before)
                throws CommitFailedException {
            editor.propertyDeleted(before);
        }

        @Override
        public Editor childNodeAdded(String name, NodeState after)
                throws CommitFailedException {
            return createChildEditor(editor.childNodeAdded(name, after), name);
        }

        @Override
        public Editor childNodeChanged(String name, NodeState before,
                NodeState after) throws CommitFailedException {
            return createChildEditor(
                    editor.childNodeChanged(name, before, after), name);
        }

        @Override
        public Editor childNodeDeleted(String name, NodeState before)
                throws CommitFailedException {
            return createChildEditor(editor.childNodeDeleted(name, before),
                    name);
        }

        private TestEditor createChildEditor(Editor editor, String name) {
            if (editor == null) {
                return null;
            } else {
                return new TestEditor(editor, listener, this);
            }
        }
    }
}