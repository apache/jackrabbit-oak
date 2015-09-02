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
import static org.apache.jackrabbit.oak.commons.CIHelper.travis;
import static org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy.CleanupType.CLEAN_OLD;
import static org.junit.Assume.assumeTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HeavyWriteIT {

    private File directory;

    @Before
    public void setUp() throws IOException {
        directory = File.createTempFile(
                "FileStoreTest", "dir", new File("target"));
        directory.delete();
        directory.mkdir();
    }

    @After
    public void cleanDir() throws IOException {
        deleteDirectory(directory);
    }

    @Test
    public void heavyWrite() throws IOException, CommitFailedException, InterruptedException {
        assumeTrue(!travis());  // FIXME OAK-2375. Often fails on Travis
        final FileStore store = new FileStore(directory, 128, false);
        final SegmentNodeStore nodeStore = new SegmentNodeStore(store);
        store.setCompactionStrategy(new CompactionStrategy(false, false,
                CLEAN_OLD, 30000, (byte) 0) {
            @Override
            public boolean compacted(@Nonnull Callable<Boolean> setHead) throws Exception {
                return nodeStore.locked(setHead);
            }
        });

        int writes = 100;
        final AtomicBoolean run = new AtomicBoolean(true);
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int k = 1; run.get(); k++) {
                    store.gc();
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        });
        thread.start();

        try {
            for (int k = 1; k<=writes; k++) {
                NodeBuilder root = nodeStore.getRoot().builder();
                NodeBuilder test = root.setChildNode("test");
                createNodes(nodeStore, test, 10, 2);
                nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);

                root = nodeStore.getRoot().builder();
                root.getChildNode("test").remove();
                nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            }

        } finally {
            run.set(false);
            thread.join();
            store.close();
        }
    }

    private static void createNodes(NodeStore nodeStore, NodeBuilder builder, int count, int depth) throws IOException {
        if (depth > 0) {
            for (int k = 0; k < count; k++) {
                NodeBuilder child = builder.setChildNode("node" + k);
                createProperties(nodeStore, child, count);
                createNodes(nodeStore, child, count, depth - 1);
            }
        }
    }

    private static void createProperties(NodeStore nodeStore, NodeBuilder builder, int count) throws IOException {
        for (int k = 0; k < count; k++) {
            builder.setProperty("property-" + k, createBlob(nodeStore, 100000));
        }
    }

    private static Blob createBlob(NodeStore nodeStore, int size) throws IOException {
        byte[] data = new byte[size];
        new Random().nextBytes(data);
        return nodeStore.createBlob(new ByteArrayInputStream(data));
    }

}
