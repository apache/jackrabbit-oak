/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.segment;

import com.google.common.collect.Lists;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.blob.ReferenceCollector;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.memory.AbstractBlob;
import org.apache.jackrabbit.oak.plugins.segment.file.FileBlob;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Random;

import javax.annotation.Nonnull;

import static junit.framework.Assert.assertTrue;
import static org.apache.jackrabbit.oak.commons.FixturesHelper.getFixtures;
import static org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture.SEGMENT_MK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assume.assumeTrue;

public class ExternalBlobIT {

    private SegmentStore store;
    private SegmentNodeStore nodeStore;
    private FileBlob fileBlob;

    @BeforeClass
    public static void assumptions() {
        assumeTrue(getFixtures().contains(SEGMENT_MK));
    }
    
    @Test @Ignore("would need a FileBlobStore for this")
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

        final List<String> refrences = Lists.newArrayList();
        store.getTracker().collectBlobReferences(new ReferenceCollector() {
            @Override
            public void addReference(String reference, String nodeId) {
                assertNotNull(reference);
                refrences.add(reference);
            }
        });

        assertEquals(noOfBlobs + 2, refrences.size());
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
        FileUtils.cleanDirectory(getWorkDir());
    }

    protected SegmentNodeStore getNodeStore(BlobStore blobStore) throws IOException {
        if (nodeStore == null) {
            store = new FileStore(blobStore, getWorkDir(), 256, false);
            nodeStore = new SegmentNodeStore(store);
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
        return new File("target", "ExternalBlobTest");
    }

    private FileBlob getFileBlob() throws IOException {
        if (fileBlob == null) {
            File file = File.createTempFile("blob", "tmp");
            file.deleteOnExit();

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
}
