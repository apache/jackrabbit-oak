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

package org.apache.jackrabbit.oak.segment;

import static org.apache.commons.lang.RandomStringUtils.randomAlphabetic;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.junit.Assume.assumeTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Random;

import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Tests verifying if the repository gets corrupted or not: {@code OAK-2662 SegmentOverflowException in HeavyWriteIT on Jenkins}</p>
 *
 * <p><b>This test will run for one hour unless it fails</b>, thus it is disabled by default. On the
 * command line specify {@code -DSegmentOverflowExceptionIT=true} to enable it. To specify a different
 * time out {@code t} value use {@code -Dtimeout=t}
 * </p>
 *
 *<p>If you only want to run this test:<br>
 * {@code mvn verify -Dsurefire.skip.ut=true -PintegrationTesting -Dit.test=SegmentOverflowExceptionIT -DSegmentOverflowExceptionIT=true}
 * </p>
 */
public class SegmentOverflowExceptionIT {
    private static final Logger LOG = LoggerFactory
            .getLogger(SegmentOverflowExceptionIT.class);
    private static final boolean ENABLED = Boolean
            .getBoolean(SegmentOverflowExceptionIT.class.getSimpleName());
    private static final long TIMEOUT = Long
            .getLong("timeout", 60*60*1000);

    private final Random rnd = new Random();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private File getFileStoreFolder() {
        return folder.getRoot();
    }

    @Before
    public void setUp() throws IOException {
        assumeTrue(ENABLED);
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
    public void run() throws Exception {
        FileStore fileStore = fileStoreBuilder(getFileStoreFolder()).withGCMonitor(gcMonitor).build();
        try {
            final SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
            long start = System.currentTimeMillis();
            int snfeCount = 0;
            while (System.currentTimeMillis() - start < TIMEOUT) {
                try {
                    NodeBuilder root = nodeStore.getRoot().builder();
                    while (rnd.nextInt(100) != 0) {
                        modify(nodeStore, root);
                    }
                    nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);

                    if (compact) {
                        compact = false;
                        fileStore.fullGC();
                    }
                } catch (SegmentNotFoundException snfe) {
                    // Usually this can be ignored as SNFEs are somewhat expected here
                    // due the small retention value for segments.
                    if (snfeCount++ > 100) {
                        throw snfe;
                    }
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
