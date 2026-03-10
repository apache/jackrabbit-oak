/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.segment.file;

import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.tar.TarFiles;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests creating a writable {@link FileStore} from an existing
 * {@link ReadOnlyFileStore}, reusing the already-opened TAR readers.
 */
public class FileStoreInitFromTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Test
    public void testUpgradeReadOnlyToWritable() throws Exception {
        File directory = folder.getRoot();

        // Step 1: Create a writable FileStore and write some nodes and properties
        try (FileStore original = fileStoreBuilder(directory)
                .withMaxFileSize(1)
                .withMemoryMapping(false)
                .build()) {
            SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(original).build();
            NodeBuilder builder = nodeStore.getRoot().builder();
            builder.child("content").setProperty("title", "Hello");
            builder.child("content").child("page1").setProperty("text", "First page");
            builder.child("content").child("page2").setProperty("text", "Second page");
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            original.flush();
        }

        // Step 2: Open as read-only and verify the content is there
        ReadOnlyFileStore readOnly = fileStoreBuilder(directory)
                .withMemoryMapping(false)
                .buildReadOnly();

        TarFiles readOnlyTarFiles = readOnly.getTarFiles();
        int readerCount = readOnlyTarFiles.readerCount();
        assertTrue("Expected at least one TAR reader", readerCount > 0);

        NodeState roRoot = readOnly.getHead().getChildNode("root");
        assertEquals("Hello", roRoot.getChildNode("content").getString("title"));

        // Step 3: Create a new writable FileStore, transferring readers from read-only
        FileStore writable = fileStoreBuilder(directory)
                .withMaxFileSize(1)
                .withMemoryMapping(false)
                .withExistingTarFiles(readOnlyTarFiles)
                .build();

        // Verify readers were transferred, not re-opened
        assertEquals(readerCount, writable.readerCount());
        assertEquals(0, readOnlyTarFiles.readerCount());

        // Close the read-only store — transferred readers must remain unaffected
        readOnly.close();

        // Step 4: Verify old content is accessible from the new writable store
        SegmentNodeStore rwNodeStore = SegmentNodeStoreBuilders.builder(writable).build();
        NodeState rwRoot = rwNodeStore.getRoot();
        assertNotNull(rwRoot.getChildNode("content"));
        assertEquals("Hello", rwRoot.getChildNode("content").getString("title"));
        assertEquals("First page", rwRoot.getChildNode("content").getChildNode("page1").getString("text"));
        assertEquals("Second page", rwRoot.getChildNode("content").getChildNode("page2").getString("text"));

        // Step 5: Write new content
        NodeBuilder rwBuilder = rwNodeStore.getRoot().builder();
        rwBuilder.child("content").child("page3").setProperty("text", "Third page");
        rwBuilder.child("config").setProperty("version", 2);
        rwNodeStore.merge(rwBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        writable.flush();

        // Step 6: Verify both old and new content are present
        NodeState finalRoot = rwNodeStore.getRoot();
        assertEquals("Hello", finalRoot.getChildNode("content").getString("title"));
        assertEquals("First page", finalRoot.getChildNode("content").getChildNode("page1").getString("text"));
        assertEquals("Second page", finalRoot.getChildNode("content").getChildNode("page2").getString("text"));
        assertEquals("Third page", finalRoot.getChildNode("content").getChildNode("page3").getString("text"));
        assertNotNull(finalRoot.getChildNode("config").getProperty("version"));
        assertEquals(2, (long) finalRoot.getChildNode("config").getProperty("version").getValue(org.apache.jackrabbit.oak.api.Type.LONG));

        writable.close();
    }
}
