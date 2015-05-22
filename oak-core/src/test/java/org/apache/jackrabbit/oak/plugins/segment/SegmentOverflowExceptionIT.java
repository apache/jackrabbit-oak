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

package org.apache.jackrabbit.oak.plugins.segment;

import static org.apache.commons.io.FileUtils.deleteDirectory;
import static org.apache.commons.lang.RandomStringUtils.randomAlphabetic;
import static org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy.CleanupType.CLEAN_OLD;
import static org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy.MEMORY_THRESHOLD_DEFAULT;
import static org.apache.jackrabbit.oak.plugins.segment.file.FileStore.newFileStore;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Callable;

import javax.annotation.Nonnull;

import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for reproducing OAK-2662. This test will never terminate unless it fails,
 * thus it is marked as @Ignored for now.
 */
@Ignore("long running")
public class SegmentOverflowExceptionIT {
    private static final Logger LOG = LoggerFactory.getLogger(SegmentOverflowExceptionIT.class);

    private final Random rnd = new Random();

    private File directory;

    @Before
    public void setUp() throws IOException {
        directory = File.createTempFile(getClass().getSimpleName(), "dir", new File("target"));
        directory.delete();
        directory.mkdir();
    }

    @After
    public void cleanDir() {
        try {
            deleteDirectory(directory);
        } catch (IOException e) {
            LOG.error("Error cleaning directory", e);
        }
    }

    private volatile boolean compact = true;

    private final GCMonitor gcMonitor = new GCMonitor.Empty() {
        @Override
        public void skipped(String reason, Object... arguments) {
            compact = true;
        }

        @Override
        public void cleaned(long reclaimedSize, long currentSize) {
            compact = true;
        }
    };

    @Test
    public void run() throws IOException, CommitFailedException, InterruptedException {
        FileStore fileStore = newFileStore(directory).withGCMonitor(gcMonitor).create();
        try {
            final SegmentNodeStore nodeStore = new SegmentNodeStore(fileStore);
            fileStore.setCompactionStrategy(new CompactionStrategy(false, false, CLEAN_OLD, 1000, MEMORY_THRESHOLD_DEFAULT) {
                @Override
                public boolean compacted(@Nonnull Callable<Boolean> setHead) throws Exception {
                    return nodeStore.locked(setHead);
                }
            });

            while (true) {
                NodeBuilder root = nodeStore.getRoot().builder();
                while (rnd.nextInt(100) != 0) {
                    modify(nodeStore, root);
                }
                nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);

                if (compact) {
                    compact = false;
                    fileStore.maybeCompact(true);
                }
            }
        } finally {
            fileStore.close();
        }
    }

    private void modify(NodeStore nodeStore, NodeBuilder nodeBuilder) throws IOException {
        int k = rnd.nextInt(100);
        if (k < 10) {
            if (!nodeBuilder.remove()) {
                descent(nodeStore, nodeBuilder);
            }
        } else if (k < 40)  {
            nodeBuilder.setChildNode("N" + rnd.nextInt(1000));
        } else if (k < 80) {
            nodeBuilder.setProperty("P" + rnd.nextInt(1000), randomAlphabetic(rnd.nextInt(10000)));
        } else if (k < 90) {
            nodeBuilder.setProperty("B" + rnd.nextInt(1000), createBlob(nodeStore, 10000000));
        } else {
            descent(nodeStore, nodeBuilder);
        }
    }

    private void descent(NodeStore nodeStore, NodeBuilder nodeBuilder) throws IOException {
        long count = nodeBuilder.getChildNodeCount(Long.MAX_VALUE);
        if (count > 0) {
            int c = rnd.nextInt((int) count);
            String name = Iterables.get(nodeBuilder.getChildNodeNames(), c);
            modify(nodeStore, nodeBuilder.getChildNode(name));
        }
    }

    private static Blob createBlob(NodeStore nodeStore, int size) throws IOException {
        byte[] data = new byte[size];
        new Random().nextBytes(data);
        return nodeStore.createBlob(new ByteArrayInputStream(data));
    }

}
