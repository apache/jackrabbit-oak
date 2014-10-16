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
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.junit.Test;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.JcrConstants.JCR_DATA;
import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INDEX_DATA_CHILD_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.OakDirectory.PROP_BLOB_SIZE;
import static org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent.INITIAL_CONTENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OakDirectoryTest {
    private Random rnd = new Random();

    private NodeState root = INITIAL_CONTENT;

    private NodeBuilder builder = root.builder();

    int fileSize = OakDirectory.DEFAULT_BLOB_SIZE + rnd.nextInt(1000);

    @Test
    public void writes_DefaultSetup() throws Exception{
        Directory dir = createDir(builder);
        assertWrites(dir, IndexDefinition.DEFAULT_BLOB_SIZE);
    }

    @Test
    public void writes_CustomBlobSize() throws Exception{
        builder.setProperty(LuceneIndexConstants.BLOB_SIZE, 300);
        Directory dir = createDir(builder);
        assertWrites(dir, 300);
    }

    @Test
    public void testCompatibility() throws Exception{
        builder.setProperty(LuceneIndexConstants.BLOB_SIZE, OakDirectory.DEFAULT_BLOB_SIZE);
        Directory dir = createDir(builder);
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
        assertEquals(blobSize, blobs.get(0).length());

        return data;
    }

    private Directory createDir(NodeBuilder builder){
        return new OakDirectory(builder.child(INDEX_DATA_CHILD_NAME), new IndexDefinition(builder));
    }

    byte[] randomBytes(int size) {
        byte[] data = new byte[size];
        rnd.nextBytes(data);
        return data;
    }
}
