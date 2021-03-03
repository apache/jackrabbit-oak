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

import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class BinariesInlineThresholdIT {
    private static final int SMALL_BINARIES_INLINE_THRESHOLD = 1024;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private BlobStore createBlobStore(File dir) {
        FileDataStore fds = new FileDataStore();
        fds.setMinRecordLength(4096);
        fds.init(dir.getAbsolutePath());
        return new DataStoreBlobStore(fds);
    }

	 private FileStore createFileStore(File dir, BlobStore blobStore, int binariesInlineThreshold) throws Throwable {
	     FileStoreBuilder builder = fileStoreBuilder(dir)
                .withMaxFileSize(1)
                .withMemoryMapping(false)
                .withNodeDeduplicationCacheSize(1)
                .withSegmentCacheSize(256)
                .withStringCacheSize(0)
                .withTemplateCacheSize(0)
                .withBinariesInlineThreshold(binariesInlineThreshold);
        
	     if (blobStore != null) {
	         builder.withBlobStore(blobStore);
	     }

	     return builder.build();
    }
	
    @Test
    public void testInlineBinaries() throws Throwable {
        File fileStoreDir = folder.newFolder();
        File blobStoreDir = folder.newFolder();

        BlobStore blobStore = createBlobStore(blobStoreDir);

        // set binaries inline threshold to default Segment.MEDIUM limit, i.e. 16512
        // all binaries under this limit should be inlined, if over the limit they
        // will be stored in the blobstore
        FileStore fileStore = createFileStore(fileStoreDir, blobStore, Segment.MEDIUM_LIMIT);

        SegmentNodeStore sns = SegmentNodeStoreBuilders.builder(fileStore).build();

        Blob b1 = addTestContent(sns, "a", SMALL_BINARIES_INLINE_THRESHOLD - 1);
        assertTrue(b1 instanceof SegmentBlob);
        assertNull(((SegmentBlob) b1).getBlobId());
        assertFalse(((SegmentBlob) b1).isExternal());
        assertFalse(b1.isInlined());

        Blob b2 = addTestContent(sns, "b", SMALL_BINARIES_INLINE_THRESHOLD);
        assertTrue(b2 instanceof SegmentBlob);
        assertNull(((SegmentBlob) b2).getBlobId());
        assertFalse(((SegmentBlob) b2).isExternal());
        assertFalse(b2.isInlined());

        Blob b3 = addTestContent(sns, "c", Segment.MEDIUM_LIMIT - 1);
        assertTrue(b3 instanceof SegmentBlob);
        assertNull(((SegmentBlob) b3).getBlobId());
        assertFalse(((SegmentBlob) b3).isExternal());
        assertFalse(b3.isInlined());

        Blob b4 = addTestContent(sns, "d", Segment.MEDIUM_LIMIT);
        assertTrue(b4 instanceof SegmentBlob);
        assertNotNull(b4.getReference());
        assertEquals(b4.getContentIdentity(), ((SegmentBlob) b4).getBlobId());
        assertFalse(b4.isInlined());

        fileStore.close();

        fileStore = createFileStore(fileStoreDir, blobStore, SMALL_BINARIES_INLINE_THRESHOLD);
        sns = SegmentNodeStoreBuilders.builder(fileStore).build();

        Blob b5 = addTestContent(sns, "e", SMALL_BINARIES_INLINE_THRESHOLD - 1);
        assertTrue(b5 instanceof SegmentBlob);
        assertNull(((SegmentBlob) b5).getBlobId());
        assertFalse(((SegmentBlob) b5).isExternal());
        assertFalse(b5.isInlined());

        Blob b6 = addTestContent(sns, "f", SMALL_BINARIES_INLINE_THRESHOLD);
        assertTrue(b6 instanceof SegmentBlob);
        assertNull(b6.getReference());
        assertEquals(b6.getContentIdentity(), ((SegmentBlob) b6).getBlobId());
        assertTrue(b6.isInlined());

        Blob b7 = addTestContent(sns, "g", Segment.MEDIUM_LIMIT - 1);
        assertTrue(b7 instanceof SegmentBlob);
        assertNotNull(b7.getReference());
        assertEquals(b7.getContentIdentity(), ((SegmentBlob) b7).getBlobId());
        assertFalse(b7.isInlined());

        Blob b8 = addTestContent(sns, "h", Segment.MEDIUM_LIMIT);
        assertTrue(b8 instanceof SegmentBlob);
        assertNotNull(b8.getReference());
        assertEquals(b8.getContentIdentity(), ((SegmentBlob) b8).getBlobId());
        assertFalse(b8.isInlined());

        fileStore.compactFull();

        // b2 and b3 blobs should have ended now in blob store
        // as a result of compaction rewriting the repository
        // using SMALL_BINARIES_INLINE_THRESHOLD

        b2 = getBlob(sns, "b");
        assertTrue(b2 instanceof SegmentBlob);
        assertNotNull(((SegmentBlob) b2).getBlobId());
        assertTrue(((SegmentBlob) b2).isExternal());
        assertTrue(b2.isInlined());

        b3 = getBlob(sns, "c");
        assertTrue(b3 instanceof SegmentBlob);
        assertNotNull(b3.getReference());
        assertEquals(b3.getContentIdentity(), ((SegmentBlob) b3).getBlobId());
        assertFalse(b3.isInlined());

        blobStore.close();
        fileStore.close();
    }

    private Blob getBlob(SegmentNodeStore sns, String child) {
        PropertyState ps = sns.getRoot().getChildNode(child).getProperty("testBlob");
        return ps.getValue(Type.BINARY);
    }

    private Blob addTestContent(NodeStore store, String child, int size) throws CommitFailedException, IOException {
        NodeBuilder builder = store.getRoot().builder();
        builder.child(child).setProperty("ts", System.currentTimeMillis());

        byte[] data = new byte[size];
        new Random().nextBytes(data);
        Blob blob = store.createBlob(new ByteArrayInputStream(data));

        builder.child(child).setProperty("testBlob", blob);

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        return blob;
    }
}
