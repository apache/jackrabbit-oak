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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.segment.Compactor;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CompactionAndCleanupTest {

    private File directory;

    @Before
    public void setUp() throws IOException {
        directory = File.createTempFile(
                "FileStoreTest", "dir", new File("target"));
        directory.delete();
        directory.mkdir();
    }

    @Test
    public void compactionAndWeakReferenceMagic() throws Exception{
        final int MB = 1024 * 1024;
        final int blobSize = 5 * MB;

        FileStore fileStore = new FileStore(directory, 1, 1, false);
        SegmentNodeStore nodeStore = new SegmentNodeStore(fileStore);

        //1. Create a property with 5 MB blob
        NodeBuilder builder = nodeStore.getRoot().builder();
        builder.setProperty("a1", createBlob(nodeStore, blobSize));
        builder.setProperty("b", "foo");

        //Keep a reference to this nodeState to simulate long
        //running session
        NodeState ns1 = nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        System.out.printf("File store pre removal %d%n", mb(fileStore.size()));
        assertEquals(mb(fileStore.size()), mb(blobSize));


        //2. Now remove the property
        builder = nodeStore.getRoot().builder();
        builder.removeProperty("a1");
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        //Size remains same
        System.out.printf("File store pre compaction %d%n", mb(fileStore.size()));
        assertEquals(mb(fileStore.size()), mb(blobSize));

        //3. Compact
        fileStore.compact();

        //Size still remains same
        System.out.printf("File store post compaction %d%n", mb(fileStore.size()));
        assertEquals(mb(fileStore.size()), mb(blobSize));

        //4. Add some more property to flush the current TarWriter
        builder = nodeStore.getRoot().builder();
        builder.setProperty("a2", createBlob(nodeStore, blobSize));
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        //Size is double
        System.out.printf("File store pre cleanup %d%n", mb(fileStore.size()));
        assertEquals(mb(fileStore.size()), 2 * mb(blobSize));

        //5. Cleanup
        cleanup(fileStore);

        //Size is still double. Deleted space not reclaimed
        System.out.printf("File store post cleanup %d%n", mb(fileStore.size()));
        assertEquals(mb(fileStore.size()), 2*mb(blobSize));

        //6. Null out any hard reference
        ns1 = null;
        builder = null;
        cleanup(fileStore);

        //Size should not come back to 5 and deleted data
        //space reclaimed
        System.out.printf("File store post cleanup and nullification %d%n", mb(fileStore.size()));
        assertEquals(mb(fileStore.size()), mb(blobSize));
    }

    @After
    public void cleanDir() throws IOException {
        FileUtils.deleteDirectory(directory);
    }

    private static void cleanup(FileStore fileStore) throws IOException {
        fileStore.getTracker().setCompactionMap(new Compactor(null).getCompactionMap());
        fileStore.getTracker().getWriter().dropCache();

        fileStore.cleanup();
    }

    private static Blob createBlob(NodeStore nodeStore, int size) throws IOException {
        byte[] data = new byte[size];
        new Random().nextBytes(data);
        return nodeStore.createBlob(new ByteArrayInputStream(data));
    }

    private static long mb(long size){
        return size / (1024 * 1024);
    }
}
