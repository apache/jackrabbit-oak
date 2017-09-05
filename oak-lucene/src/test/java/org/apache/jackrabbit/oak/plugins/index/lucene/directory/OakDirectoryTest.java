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

package org.apache.jackrabbit.oak.plugins.index.lucene.directory;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static org.apache.commons.io.FileUtils.ONE_GB;
import static org.apache.commons.io.FileUtils.ONE_MB;
import static org.apache.jackrabbit.JcrConstants.JCR_DATA;
import static org.apache.jackrabbit.oak.InitialContent.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INDEX_DATA_CHILD_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.directory.OakDirectory.PROP_BLOB_SIZE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.directory.OakDirectory.UNIQUE_KEY_SIZE;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.NullInputStream;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.ActiveDeletedBlobCollectorFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.BlobFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.OakDirectory;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.OakIndexFile;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.SegmentTestConstants;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.ReadOnlyBuilder;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.InputStreamDataInput;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class OakDirectoryTest {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder(new File("target"));

    private Random rnd = new Random();

    private NodeState root = INITIAL_CONTENT;

    private NodeBuilder builder = root.builder();

    int fileSize = IndexDefinition.DEFAULT_BLOB_SIZE * 2 + rnd.nextInt(1000);

    @Test
    public void writes_DefaultSetup() throws Exception{
        Directory dir = createDir(builder, false, "/foo");
        assertWrites(dir, IndexDefinition.DEFAULT_BLOB_SIZE);
    }

    @Test
    public void writes_CustomBlobSize() throws Exception{
        builder.setProperty(LuceneIndexConstants.BLOB_SIZE, 300);
        Directory dir = createDir(builder, false, "/foo");
        assertWrites(dir, 300);
    }

    @Test
    public void testCompatibility() throws Exception{
        builder.setProperty(LuceneIndexConstants.BLOB_SIZE, OakIndexFile.DEFAULT_BLOB_SIZE);
        Directory dir = createDir(builder, false, "/foo");
        byte[] data = assertWrites(dir, OakIndexFile.DEFAULT_BLOB_SIZE);

        NodeBuilder testNode = builder.child(INDEX_DATA_CHILD_NAME).child("test");
        //Remove the size property to simulate old behaviour
        testNode.removeProperty(PROP_BLOB_SIZE);

        //Read should still work even if the size property is removed
        IndexInput i = dir.openInput("test", IOContext.DEFAULT);
        assertEquals(fileSize, i.length());

        byte[] result = new byte[fileSize];
        i.readBytes(result, 0, result.length);

        assertTrue(Arrays.equals(data, result));
    }

    @Test //OAK-2388
    public void testOverflow() throws Exception{
        Directory dir = createDir(builder, false, "/foo");
        NodeBuilder file = builder.child(INDEX_DATA_CHILD_NAME).child("test.txt");
        int blobSize = 32768;
        int dataSize = 90844;
        file.setProperty(OakDirectory.PROP_BLOB_SIZE, blobSize);

        List<? super Blob> blobs = new ArrayList<Blob>(dataSize);
        for (int i = 0; i < dataSize; i++) {
            blobs.add(new ArrayBasedBlob(new byte[0]));
        }
        file.setProperty(PropertyStates.createProperty("jcr:data", blobs, Type.BINARIES));

        IndexInput input  = dir.openInput("test.txt", IOContext.DEFAULT);
        assertEquals((long) blobSize * (dataSize - 1), input.length());
    }

    @Test
    public void saveListing() throws Exception{
        builder.setProperty(LuceneIndexConstants.SAVE_DIR_LISTING, true);
        Directory dir = createDir(builder, false, "/foo");
        Set<String> fileNames = newHashSet();
        for (int i = 0; i < 10; i++) {
            String fileName = "foo" + i;
            createFile(dir, fileName);
            fileNames.add(fileName);
        }
        dir.close();

        dir = createDir(builder, true, "/foo");
        assertEquals(fileNames, newHashSet(dir.listAll()));
    }

    @Test
    public void skipSaveListingIfUnchanged() throws Exception{
        builder.setProperty(LuceneIndexConstants.SAVE_DIR_LISTING, true);
        Directory dir = createDir(builder, false, "/foo");
        Set<String> fileNames = newHashSet();
        for (int i = 0; i < 10; i++) {
            String fileName = "foo" + i;
            createFile(dir, fileName);
            fileNames.add(fileName);
        }
        dir.close();

        dir = createDir(new ReadOnlyBuilder(builder.getNodeState()), false, "/foo");
        Set<String> files =  newHashSet(dir.listAll());
        dir.close();
        assertEquals(fileNames, files);
    }

    byte[] assertWrites(Directory dir, int blobSize) throws IOException {
        byte[] data = randomBytes(fileSize);
        IndexOutput o = dir.createOutput("test", IOContext.DEFAULT);
        o.writeBytes(data, data.length);
        o.close();

        assertTrue(dir.fileExists("test"));
        assertEquals(fileSize, dir.fileLength("test"));

        IndexInput i = dir.openInput("test", IOContext.DEFAULT);
        assertEquals(fileSize, i.length());

        byte[] result = new byte[fileSize];
        i.readBytes(result, 0, result.length);

        assertTrue(Arrays.equals(data, result));

        NodeBuilder testNode = builder.child(INDEX_DATA_CHILD_NAME).child("test");
        assertEquals(blobSize, testNode.getProperty(PROP_BLOB_SIZE).getValue(Type.LONG).longValue());

        List<Blob> blobs = newArrayList(testNode.getProperty(JCR_DATA).getValue(BINARIES));
        assertEquals(blobSize + UNIQUE_KEY_SIZE, blobs.get(0).length());

        return data;
    }

    private int createFile(Directory dir, String fileName) throws IOException {
        int size = rnd.nextInt(1000) + 1;
        byte[] data = randomBytes(size);
        IndexOutput o = dir.createOutput(fileName, IOContext.DEFAULT);
        o.writeBytes(data, data.length);
        o.close();
        return size;
    }

    private OakDirectory createDir(NodeBuilder builder, boolean readOnly, String indexPath){
        return new OakDirectory(builder,
                new IndexDefinition(root, builder.getNodeState(), indexPath), readOnly);
    }

    byte[] randomBytes(int size) {
        byte[] data = new byte[size];
        rnd.nextBytes(data);
        return data;
    }

    @Test
    public void testCloseOnOriginalIndexInput() throws Exception {
        Directory dir = createDir(builder, false, "/foo");
        NodeBuilder file = builder.child(INDEX_DATA_CHILD_NAME).child("test.txt");
        int dataSize = 1024;
        List<? super Blob> blobs = new ArrayList<Blob>(dataSize);
        for (int i = 0; i < dataSize; i++) {
            blobs.add(new ArrayBasedBlob(new byte[0]));
        }
        file.setProperty(PropertyStates.createProperty("jcr:data", blobs, Type.BINARIES));
        IndexInput input = dir.openInput("test.txt", IOContext.DEFAULT);
        input.close();
        assertClosed(input);
    }

    @Test
    public void testCloseOnClonedIndexInputs() throws Exception {
        Directory dir = createDir(builder, false, "/foo");
        NodeBuilder file = builder.child(INDEX_DATA_CHILD_NAME).child("test.txt");
        int dataSize = 1024;
        List<? super Blob> blobs = new ArrayList<Blob>(dataSize);
        for (int i = 0; i < dataSize; i++) {
            blobs.add(new ArrayBasedBlob(new byte[0]));
        }
        file.setProperty(PropertyStates.createProperty("jcr:data", blobs, Type.BINARIES));
        IndexInput input = dir.openInput("test.txt", IOContext.DEFAULT);
        IndexInput clone1 = input.clone();
        IndexInput clone2 = input.clone();
        input.close();
        assertClosed(input);
        assertClosed(clone1);
        assertClosed(clone2);
    }

    private void assertClosed(IndexInput input) throws IOException {
        try {
            input.length();
            fail("cannot use IndexInput once closed");
        } catch (AlreadyClosedException e) {
            // expected exception
        }
        try {
            input.seek(0);
            fail("cannot use IndexInput once closed");
        } catch (AlreadyClosedException e) {
            // expected exception
        }
        try {
            input.getFilePointer();
            fail("cannot use IndexInput once closed");
        } catch (AlreadyClosedException e) {
            // expected exception
        }
        try {
            input.readInt();
            fail("cannot use IndexInput once closed");
        } catch (AlreadyClosedException e) {
            // expected exception
        }
        try {
            input.readShort();
            fail("cannot use IndexInput once closed");
        } catch (AlreadyClosedException e) {
            // expected exception
        }
        try {
            input.readLong();
            fail("cannot use IndexInput once closed");
        } catch (AlreadyClosedException e) {
            // expected exception
        }
        try {
            input.readByte();
            fail("cannot use IndexInput once closed");
        } catch (AlreadyClosedException e) {
            // expected exception
        }
        try {
            input.readString();
            fail("cannot use IndexInput once closed");
        } catch (AlreadyClosedException e) {
            // expected exception
        }
        try {
            input.readStringSet();
            fail("cannot use IndexInput once closed");
        } catch (AlreadyClosedException e) {
            // expected exception
        }
        try {
            input.readStringStringMap();
            fail("cannot use IndexInput once closed");
        } catch (AlreadyClosedException e) {
            // expected exception
        }
        try {
            input.readVInt();
            fail("cannot use IndexInput once closed");
        } catch (AlreadyClosedException e) {
            // expected exception
        }
        try {
            input.readVLong();
            fail("cannot use IndexInput once closed");
        } catch (AlreadyClosedException e) {
            // expected exception
        }
        try {
            input.readBytes(null, 0, 0);
            fail("cannot use IndexInput once closed");
        } catch (AlreadyClosedException e) {
            // expected exception
        }
        try {
            input.readBytes(null, 0, 0, false);
            fail("cannot use IndexInput once closed");
        } catch (AlreadyClosedException e) {
            // expected exception
        }
    }

    @Test
    public void largeFile() throws Exception{
        FileStore store = FileStoreBuilder.fileStoreBuilder(tempFolder.getRoot())
                .withMemoryMapping(false)
                .withBlobStore(new BlackHoleBlobStore())
                .build();
        SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(store).build();
        IndexDefinition defn = new IndexDefinition(INITIAL_CONTENT, EmptyNodeState.EMPTY_NODE, "/foo");
        Directory directory = new OakDirectory(nodeStore.getRoot().builder(), defn, false);

        long expectedSize = ONE_GB * 2 + ONE_MB;
        String fileName = "test";
        writeFile(directory, fileName, expectedSize);
        assertEquals(expectedSize, directory.fileLength(fileName));

        IndexInput input  = directory.openInput(fileName, IOContext.DEFAULT);
        readInputToEnd(expectedSize, input);
        store.close();
    }

    @Test
    public void dirNameInExceptionMessage() throws Exception{
        String indexPath = "/foo/bar";
        Directory dir = createDir(builder, false, indexPath);

        try {
            dir.openInput("foo.txt", IOContext.DEFAULT);
            fail();
        } catch (IOException e){
            assertThat(e.getMessage(), containsString(indexPath));
        }

        int fileSize = createFile(dir, "test.txt");
        IndexInput in = dir.openInput("test.txt", IOContext.DEFAULT);

        try {
            in.seek(fileSize + 1);
            fail();
        } catch (IOException e){
            assertThat(e.getMessage(), containsString(indexPath));
        }

        IndexInput in2 = dir.openInput("test.txt", IOContext.DEFAULT);

        try {
            byte[] data = new byte[fileSize + 1];
            in2.readBytes(data, 0, fileSize + 1);
            fail();
        } catch (IOException e){
            assertThat(e.getMessage(), containsString(indexPath));
        }
    }

    @Test
    public void dirNameInException_Writes() throws Exception{
        FailOnDemandBlobStore blobStore = new FailOnDemandBlobStore();
        FileStore store = FileStoreBuilder.fileStoreBuilder(tempFolder.getRoot())
                .withMemoryMapping(false)
                .withBlobStore(blobStore)
                .build();
        SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(store).build();

        String indexPath = "/foo/bar";

        int minFileSize = SegmentTestConstants.MEDIUM_LIMIT;
        int blobSize = minFileSize + 1000;

        builder = nodeStore.getRoot().builder();
        builder.setProperty(LuceneIndexConstants.BLOB_SIZE, blobSize);
        Directory dir = createDir(builder, false, indexPath);

        blobStore.startFailing();
        IndexOutput o = dir.createOutput("test1.txt", IOContext.DEFAULT);
        try{
            o.writeBytes(randomBytes(blobSize + 10), blobSize + 10);
            fail();
        } catch (IOException e){
            assertThat(e.getMessage(), containsString(indexPath));
            assertThat(e.getMessage(), containsString("test1.txt"));
        }

        blobStore.reset();

        IndexOutput o3 = dir.createOutput("test3.txt", IOContext.DEFAULT);
        o3.writeBytes(randomBytes(minFileSize), minFileSize);

        blobStore.startFailing();
        try{
            o3.flush();
            fail();
        } catch (IOException e){
            assertThat(e.getMessage(), containsString(indexPath));
            assertThat(e.getMessage(), containsString("test3.txt"));
        }

        store.close();
    }

    @Test
    public void readOnlyDirectory() throws Exception{
        Directory dir = new OakDirectory(new ReadOnlyBuilder(builder.getNodeState()),
                new IndexDefinition(root, builder.getNodeState(), "/foo"), true);
        assertEquals(0, dir.listAll().length);
    }

    @Test
    public void testDirty() throws Exception{
        OakDirectory dir = createDir(builder, false, "/foo");
        assertFalse(dir.isDirty());
        createFile(dir, "a");
        assertTrue(dir.isDirty());
        dir.close();

        dir = createDir(builder, false, "/foo");
        assertFalse(dir.isDirty());
        dir.openInput("a", IOContext.DEFAULT);
        assertFalse(dir.isDirty());
        dir.deleteFile("a");
        assertTrue(dir.isDirty());
        dir.close();
    }

    // OAK-6503
    @Test
    public void dontMarkNonBlobStoreBlobsAsDeleted() throws Exception{
        final String deletedBlobId = "blobIdentifier";
        final String blobIdToString = "NeverEver-Ever-Ever-ShouldThisBeMarkedAsDeleted";
        final int fileSize = 1;

        final AtomicBoolean identifiableBlob = new AtomicBoolean(false);

        IndexDefinition def = new IndexDefinition(root, builder.getNodeState(), "/foo");
        BlobFactory factory = new BlobFactory() {
            @Override
            public Blob createBlob(InputStream in) throws IOException {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                IOUtils.copy(in, out);
                byte[] data = out.toByteArray();
                return new ArrayBasedBlob(data) {
                    @Override
                    public String getContentIdentity() {
                        return identifiableBlob.get()?deletedBlobId:null;
                    }

                    @Override
                    public String toString() {
                        return blobIdToString;
                    }
                };
            }
        };

        OakDirectory dir = new OakDirectory(builder, INDEX_DATA_CHILD_NAME, def, false, factory,
                new ActiveDeletedBlobCollectorFactory.BlobDeletionCallback() {
                    @Override
                    public void deleted(String blobId, Iterable<String> ids) {
                        assertEquals("Only blobs with content identity must be reported as deleted", deletedBlobId, blobId);
                    }

                    @Override
                    public void commitProgress(IndexProgress indexProgress) {
                    }
                });

        writeFile(dir, "file1", fileSize);
        writeFile(dir, "file2", fileSize);

        dir.deleteFile("file1");

        identifiableBlob.set(true);
        dir.deleteFile("file2");

        dir.close();
    }

    @Test
    public void blobFactory() throws Exception {
        final AtomicInteger numBlobs = new AtomicInteger();
        final int fileSize = 1024;
        IndexDefinition def = new IndexDefinition(root, builder.getNodeState(), "/foo");
        BlobFactory factory = new BlobFactory() {
            @Override
            public Blob createBlob(InputStream in) throws IOException {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                IOUtils.copy(in, out);
                byte[] data = out.toByteArray();
                assertEquals(fileSize + UNIQUE_KEY_SIZE, data.length);
                numBlobs.incrementAndGet();
                return new ArrayBasedBlob(data);
            }
        };
        OakDirectory dir = new OakDirectory(builder, INDEX_DATA_CHILD_NAME, def, false, factory);
        numBlobs.set(0);
        writeFile(dir, "file", fileSize);
        assertEquals(1, numBlobs.get());
        dir.close();
    }

    @Test
    public void fileLength() throws Exception {
        final int fileSize = 1024;
        final String fileName = "file";
        OakDirectory dir = createDir(builder, false, "/foo");
        writeFile(dir, fileName, fileSize);
        assertEquals(fileSize, dir.fileLength(fileName));
        try {
            dir.fileLength("unknown");
            fail("must throw FileNotFoundException");
        } catch (FileNotFoundException expected) {
            // expected
        }
        dir.close();
    }

    private static void readInputToEnd(long expectedSize, IndexInput input) throws IOException {
        int COPY_BUFFER_SIZE = 16384;
        byte[] copyBuffer = new byte[(int) ONE_MB];
        long left = expectedSize;
        while (left > 0) {
            final int toCopy;
            if (left > COPY_BUFFER_SIZE) {
                toCopy = COPY_BUFFER_SIZE;
            } else {
                toCopy = (int) left;
            }
            input.readBytes(copyBuffer, 0, toCopy);
            left -= toCopy;
        }
    }

    private static void writeFile(Directory directory, String fileName,  long size) throws Exception{
        IndexOutput o = directory.createOutput(fileName, IOContext.DEFAULT);
        o.copyBytes(new InputStreamDataInput(new NullInputStream(size)), size);
        o.close();
    }

    private static class BlackHoleBlobStore extends MemoryBlobStore {
        private String blobId;
        private byte[] data;
        @Override
        protected synchronized void storeBlock(byte[] digest, int level, byte[] data) {
            //Eat up all the writes
        }

        @Override
        public String writeBlob(InputStream in) throws IOException {
            //Avoid expensive digest calculation as all content is 0 byte. So memorize
            //the id if same content is passed
            if (blobId == null) {
                data = IOUtils.toByteArray(in);
                blobId = super.writeBlob(new ByteArrayInputStream(data));
                return blobId;
            } else {
                byte[] bytes = IOUtils.toByteArray(in);
                if (Arrays.equals(data, bytes)) {
                    return blobId;
                }
                return super.writeBlob(new ByteArrayInputStream(bytes));
            }
        }

        @Override
        protected byte[] readBlockFromBackend(BlockId id) {
            return data;
        }
    }

    private static class FailOnDemandBlobStore extends MemoryBlobStore {
        private boolean fail;

        @Override
        public String writeBlob(InputStream in) throws IOException {
            if (fail) {
                throw new IOException("Failing on demand");
            }
            return super.writeBlob(in);
        }

        public void startFailing(){
            fail = true;
        }

        public void reset(){
            fail = false;
        }
    }
}
