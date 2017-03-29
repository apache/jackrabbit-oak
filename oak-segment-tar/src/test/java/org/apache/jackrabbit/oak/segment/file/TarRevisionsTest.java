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

package org.apache.jackrabbit.oak.segment.file;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.SegmentNodeBuilder;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TarRevisionsTest {
    private FileStore store;
    private TarRevisions revisions;
    private SegmentReader reader;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private File getFileStoreFolder() {
        return folder.getRoot();
    }

    @Before
    public void setup() throws Exception {
        store = FileStoreBuilder.fileStoreBuilder(getFileStoreFolder()).build();
        revisions = store.getRevisions();
        reader = store.getReader();
        store.flush();
    }

    @After
    public void tearDown() throws IOException {
        store.close();
    }

    @Test(expected = IllegalStateException.class)
    public void unboundRevisions() throws IOException {
        try (TarRevisions tarRevisions = new TarRevisions(folder.getRoot())) {
            tarRevisions.getHead();
        }
    }

    @Nonnull
    private JournalReader createJournalReader() throws IOException {
        return new JournalReader(new File(getFileStoreFolder(), TarRevisions.JOURNAL_FILE_NAME));
    }

    @Test
    public void getHead() throws IOException {
        try (JournalReader reader = createJournalReader()) {
            assertTrue(reader.hasNext());
            assertEquals(revisions.getHead().toString10(), reader.next().getRevision());
        }
    }

    @Nonnull
    private static SegmentNodeState addChild(SegmentNodeState node, String name) {
        SegmentNodeBuilder builder = node.builder();
        builder.setChildNode(name);
        return builder.getNodeState();
    }

    @Test
    public void setHead() throws IOException {
        RecordId headId = revisions.getHead();
        SegmentNodeState newRoot = addChild(reader.readNode(headId), "a");
        assertTrue(revisions.setHead(headId, newRoot.getRecordId()));
        store.flush();

        try (JournalReader reader = createJournalReader()) {
            assertTrue(reader.hasNext());
            assertEquals(newRoot.getRecordId().toString10(), reader.next().getRevision());
        }
    }

    @Test
    public void setHeadFromFunction() throws IOException, InterruptedException {
        RecordId headId = revisions.getHead();
        SegmentNodeState root = reader.readNode(headId);

        final SegmentNodeState newRoot = addChild(root, "a");
        assertNotNull(revisions.setHead(new Function<RecordId, RecordId>() {
            @Nullable
            @Override
            public RecordId apply(RecordId headId) {
                return newRoot.getRecordId();
            }
        }));
        store.flush();

        try (JournalReader reader = createJournalReader()) {
            assertTrue(reader.hasNext());
            assertEquals(newRoot.getRecordId().toString10(), reader.next().getRevision());
        }
    }

    @Test
    public void concurrentSetHead() {
        RecordId headId = revisions.getHead();
        SegmentNodeState rootA = addChild(reader.readNode(headId), "a");
        SegmentNodeState rootB = addChild(reader.readNode(headId), "a");
        assertTrue(revisions.setHead(headId, rootA.getRecordId()));
        assertFalse(revisions.setHead(headId, rootB.getRecordId()));
        assertEquals(rootA, reader.readHeadState(revisions));
    }

    @Test
    public void concurrentSetHeadFromFunction()
    throws IOException, InterruptedException, ExecutionException, TimeoutException {
        ListeningExecutorService executor = listeningDecorator(newFixedThreadPool(2));
        try {
            ListenableFuture<Boolean> t1 = executor.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    return null != revisions.setHead(new Function<RecordId, RecordId>() {
                        @Nullable
                        @Override
                        public RecordId apply(RecordId headId) {
                            return addChild(reader.readNode(headId), "a").getRecordId();
                        }
                    });
                }
            });
            ListenableFuture<Boolean> t2 = executor.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    return null != revisions.setHead(new Function<RecordId, RecordId>() {
                        @Nullable
                        @Override
                        public RecordId apply(RecordId headId) {
                            return addChild(reader.readNode(headId), "b").getRecordId();
                        }
                    });
                }
            });

            assertTrue(t1.get(500, MILLISECONDS));
            assertTrue(t2.get(500, MILLISECONDS));

            SegmentNodeState root = reader.readNode(revisions.getHead());
            assertTrue(root.hasChildNode("a"));
            assertTrue(root.hasChildNode("b"));
        } finally {
            executor.shutdown();
        }
    }

    @Test
    public void setFromFunctionBlocks()
    throws ExecutionException, InterruptedException, TimeoutException {
        ListeningExecutorService executor = listeningDecorator(newFixedThreadPool(2));
        try {
            final CountDownLatch latch = new CountDownLatch(1);

            ListenableFuture<Boolean> t1 = executor.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    latch.await();
                    return null != revisions.setHead(Functions.<RecordId>identity());
                }
            });

            try {
                t1.get(500, MILLISECONDS);
                fail("SetHead from function should block");
            } catch (TimeoutException expected) {}

            ListenableFuture<Boolean> t2 = executor.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    latch.countDown();
                    return null != revisions.setHead(Functions.<RecordId>identity());
                }
            });

            assertTrue(t2.get(500, MILLISECONDS));
            assertTrue(t1.get(500, MILLISECONDS));
        } finally {
            executor.shutdown();
        }
    }

}
