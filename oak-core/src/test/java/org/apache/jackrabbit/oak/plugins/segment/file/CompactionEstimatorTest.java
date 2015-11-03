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

package org.apache.jackrabbit.oak.plugins.segment.file;

import static org.apache.commons.io.FileUtils.deleteDirectory;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Random;

import com.google.common.base.Suppliers;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CompactionEstimatorTest {

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
    public void testGainEstimator() throws Exception {
        final int MB = 1024 * 1024;
        final int blobSize = 2 * MB;

        FileStore fileStore = new FileStore(directory, 2, false);
        SegmentNodeStore nodeStore = new SegmentNodeStore(fileStore);

        // 1. Create some blob properties
        NodeBuilder builder = nodeStore.getRoot().builder();

        NodeBuilder c1 = builder.child("c1");
        c1.setProperty("a", createBlob(nodeStore, blobSize));
        c1.setProperty("b", "foo");

        NodeBuilder c2 = builder.child("c2");
        c2.setProperty("a", createBlob(nodeStore, blobSize));
        c2.setProperty("b", "foo");

        NodeBuilder c3 = builder.child("c3");
        c3.setProperty("a", createBlob(nodeStore, blobSize));
        c3.setProperty("b", "foo");
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // 2. Now remove the property
        builder = nodeStore.getRoot().builder();
        builder.child("c1").remove();
        builder.child("c2").remove();
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        fileStore.flush();
        try {
            // should be at 66%
            assertTrue(fileStore.estimateCompactionGain(Suppliers.ofInstance(false))
                    .estimateCompactionGain(0) > 60);
        } finally {
            fileStore.close();
        }
    }

    private static Blob createBlob(NodeStore nodeStore, int size) throws IOException {
        byte[] data = new byte[size];
        new Random().nextBytes(data);
        return nodeStore.createBlob(new ByteArrayInputStream(data));
    }

}
