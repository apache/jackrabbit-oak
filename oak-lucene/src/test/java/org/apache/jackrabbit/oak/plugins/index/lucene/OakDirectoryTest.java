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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.junit.Test;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.JcrConstants.JCR_DATA;
import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INDEX_DATA_CHILD_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.OakDirectory.PROP_BLOB_SIZE;
import static org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent.INITIAL_CONTENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class OakDirectoryTest {
    private Random rnd = new Random();

    private NodeState root = INITIAL_CONTENT;

    private NodeBuilder builder = root.builder();

    int fileSize = IndexDefinition.DEFAULT_BLOB_SIZE * 2 + rnd.nextInt(1000);

    @Test
    public void writes_DefaultSetup() throws Exception{
        Directory dir = createDir(builder, false);
        assertWrites(dir, IndexDefinition.DEFAULT_BLOB_SIZE);
    }

    @Test
    public void writes_CustomBlobSize() throws Exception{
        builder.setProperty(LuceneIndexConstants.BLOB_SIZE, 300);
        Directory dir = createDir(builder, false);
        assertWrites(dir, 300);
    }

    @Test
    public void testCompatibility() throws Exception{
        builder.setProperty(LuceneIndexConstants.BLOB_SIZE, OakDirectory.DEFAULT_BLOB_SIZE);
        Directory dir = createDir(builder, false);
        byte[] data = assertWrites(dir, OakDirectory.DEFAULT_BLOB_SIZE);

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
        Directory dir = createDir(builder, false);
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
        Directory dir = createDir(builder, false);
        Set<String> fileNames = newHashSet();
        for (int i = 0; i < 10; i++) {
            String fileName = "foo" + i;
            createFile(dir, fileName);
            fileNames.add(fileName);
        }
        dir.close();

        dir = createDir(builder, true);
        assertEquals(fileNames, newHashSet(dir.listAll()));
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
        assertEquals(blobSize + OakDirectory.UNIQUE_KEY_SIZE, blobs.get(0).length());

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

    private Directory createDir(NodeBuilder builder, boolean readOnly){
        return new OakDirectory(builder,
                new IndexDefinition(root, builder.getNodeState()), readOnly);
    }

    byte[] randomBytes(int size) {
        byte[] data = new byte[size];
        rnd.nextBytes(data);
        return data;
    }

    @Test
    public void testCloseOnOriginalIndexInput() throws Exception {
        Directory dir = createDir(builder, false);
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
        Directory dir = createDir(builder, false);
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
}
