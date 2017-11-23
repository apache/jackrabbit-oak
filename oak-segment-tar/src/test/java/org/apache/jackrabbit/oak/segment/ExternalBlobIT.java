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

import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.defaultGCOptions;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.memory.AbstractBlob;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.file.FileBlob;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.blob.BlobOptions;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ExternalBlobIT {

    private FileStore store;
    private SegmentNodeStore nodeStore;
    private FileBlob fileBlob;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Test
    @Ignore("would need a FileBlobStore for this")
    public void testFileBlob() throws Exception {
        nodeStore = getNodeStore(new TestBlobStore());
        testCreateAndRead(getFileBlob());
    }

    @Test
    public void testDataStoreBlob() throws Exception {
        FileDataStore fds = createFileDataStore();
        DataStoreBlobStore dbs = new DataStoreBlobStore(fds);
        nodeStore = getNodeStore(dbs);

        //Test for Blob which get inlined
        Blob b1 = testCreateAndRead(createBlob(fds.getMinRecordLength()-2));
        assertTrue(b1 instanceof SegmentBlob);
        assertNull(((SegmentBlob) b1).getBlobId());

        //Test for Blob which need to be pushed to BlobStore
        byte[] data2 = new byte[Segment.MEDIUM_LIMIT + 1];
        new Random().nextBytes(data2);
        Blob b2 = testCreateAndRead(nodeStore.createBlob(new ByteArrayInputStream(data2)));
        assertTrue(b2 instanceof SegmentBlob);
        assertNotNull(b2.getReference());
        assertEquals(b2.getContentIdentity(), ((SegmentBlob) b2).getBlobId());

        InputStream is = dbs.getInputStream(((SegmentBlob) b2).getBlobId());
        assertNotNull(IOUtils.contentEquals(new ByteArrayInputStream(data2), is));
        is.close();
    }

    @Test
    public void testNullBlobId() throws Exception{
        FileDataStore fds = createFileDataStore();
        DataStoreBlobStore dbs = new DataStoreBlobStore(fds);
        nodeStore = getNodeStore(dbs);

        NodeBuilder nb = nodeStore.getRoot().builder();
        NodeBuilder cb = nb.child("hello");
        cb.setProperty("blob1", createBlob(Segment.MEDIUM_LIMIT - 1));

        int noOfBlobs = 4000;
        for(int i = 0; i < noOfBlobs; i++){
            cb.setProperty("blob"+i, createBlob(Segment.MEDIUM_LIMIT+1));
        }

        cb.setProperty("anotherBlob2", createBlob(Segment.MEDIUM_LIMIT + 1));
        cb.setProperty("anotherBlob3", createBlob(Segment.MEDIUM_LIMIT + 1));
        nodeStore.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        final List<String> references = Lists.newArrayList();
        store.collectBlobReferences(reference -> {
                assertNotNull(reference);
                references.add(reference);
        });

        assertEquals(noOfBlobs + 2, references.size());
    }

    private Blob testCreateAndRead(Blob blob) throws Exception {
        NodeState state = nodeStore.getRoot().getChildNode("hello");
        if (!state.exists()) {
            NodeBuilder builder = nodeStore.getRoot().builder();
            builder.child("hello");
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }

        NodeBuilder builder = nodeStore.getRoot().builder();
        builder.getChildNode("hello").setProperty("world", blob);
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        state = nodeStore.getRoot().getChildNode("hello");
        blob = state.getProperty("world").getValue(Type.BINARY);

        assertTrue("Blob written and read must be equal",
                AbstractBlob.equal(blob, blob));
        return blob;
    }

    @After
    public void close() throws IOException {
        if (store != null) {
            store.close();
        }
        nodeStore = null;
    }

    protected SegmentNodeStore getNodeStore(BlobStore blobStore) throws Exception {
        if (nodeStore == null) {
            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
            
            store = fileStoreBuilder(getWorkDir()).withBlobStore(blobStore)
                    .withMaxFileSize(1)
                    .withStatisticsProvider(new DefaultStatisticsProvider(executor))
                    .build();
            nodeStore = SegmentNodeStoreBuilders.builder(store).build();
        }
        return nodeStore;
    }

    private Blob createBlob(int size) throws IOException {
        byte[] data = new byte[size];
        new Random().nextBytes(data);
        return nodeStore.createBlob(new ByteArrayInputStream(data));
    }

    private FileDataStore createFileDataStore() {
        FileDataStore fds = new FileDataStore();
        fds.setMinRecordLength(4092);
        fds.init(getWorkDir().getAbsolutePath());
        return fds;
    }

    private File getWorkDir(){
        return folder.getRoot();
    }

    private FileBlob getFileBlob() throws IOException {
        if (fileBlob == null) {
            File file = folder.newFile();

            byte[] data = new byte[2345];
            new Random().nextBytes(data);
            FileUtils.writeByteArrayToFile(file, data);

            fileBlob = new FileBlob(file.getPath());
        }
        return fileBlob;
    }

    private class TestBlobStore implements BlobStore {
        @Override
        public String writeBlob(InputStream in) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public String writeBlob(InputStream in, BlobOptions options) throws IOException {
            return writeBlob(in);
        }

        @Override
        public int readBlob(String blobId, long pos, byte[] buff, int off, int length) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getBlobLength(String blobId) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public InputStream getInputStream(String blobId) throws IOException {
            if(blobId.equals(fileBlob.getReference())){
                return fileBlob.getNewStream();
            }
            return null;
        }

        @Override
        public String getBlobId(@Nonnull String reference) {
            return reference;
        }

        @Override
        public String getReference(@Nonnull String blobId) {
            return blobId;
        }
    }

    @Test
    public void testSize() throws Exception {
        FileDataStore fds = createFileDataStore();
        DataStoreBlobStore dbs = new DataStoreBlobStore(fds);
        nodeStore = getNodeStore(dbs);

        int size = Segment.MEDIUM_LIMIT + 1;
        byte[] data2 = new byte[size];
        new Random().nextBytes(data2);

        Blob b = nodeStore.createBlob(new ByteArrayInputStream(data2));
        NodeBuilder builder = nodeStore.getRoot().builder();
        builder.child("hello").setProperty("world", b);
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);


        PropertyState ps = nodeStore.getRoot().getChildNode("hello").getProperty("world");
        // world = {2318851547697882338 bytes}

        assertEquals(size, ps.size());
        // assertEquals("{" + size + " bytes}", ps.toString());
    }

    @Test
    public void testOfflineCompaction() throws Exception {
        FileDataStore fds = createFileDataStore();
        DataStoreBlobStore dbs = new DataStoreBlobStore(fds);
        nodeStore = getNodeStore(dbs);

        int size = 2 * 1024 * 1024;
        byte[] data2 = new byte[size];
        new Random().nextBytes(data2);

        Blob b = nodeStore.createBlob(new ByteArrayInputStream(data2));
        NodeBuilder builder = nodeStore.getRoot().builder();
        builder.child("hello").setProperty("world", b);
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store.flush();

        // blob went to the external store
        assertTrue(store.getStats().getApproximateSize() < 10 * 1024);
        close();

        SegmentGCOptions gcOptions = defaultGCOptions().setOffline();
        store = fileStoreBuilder(getWorkDir()).withMaxFileSize(1)
                .withGCOptions(gcOptions).build();
        assertTrue(store.getStats().getApproximateSize() < 10 * 1024);

        store.compactFull();
        store.cleanup();

    }
}
