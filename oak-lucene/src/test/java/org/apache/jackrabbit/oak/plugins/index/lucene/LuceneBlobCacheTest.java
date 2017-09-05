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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.OakFileDataStore;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.OakDirectory;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class LuceneBlobCacheTest {
    private Random rnd = new Random();
    
    @Rule
    public final TemporaryFolder tempFolder = new TemporaryFolder(new File("target"));
    
    FileStore store;
    ReadAccessCountingDataStore fileDataStore;

    NodeBuilder builder;
    NodeState root;
    
    @Before 
    public void setUp() throws Exception {
        fileDataStore = new ReadAccessCountingDataStore();
        fileDataStore.init(tempFolder.newFolder().getAbsolutePath());
        FileStoreBuilder fileStoreBuilder = FileStoreBuilder.fileStoreBuilder(tempFolder.newFolder())
                                        .withBlobStore(new DataStoreBlobStore(fileDataStore)).withMaxFileSize(256)
                                        .withSegmentCacheSize(64).withMemoryMapping(false);
        store = fileStoreBuilder.build();
        NodeStore nodeStore = SegmentNodeStoreBuilders.builder(store).build();
        root = nodeStore.getRoot();
        builder = root.builder();
    }
    
    @Test
    public void testLuceneBlobCached() throws Exception {
        Directory dir = createDir(builder, false);
        assertWrites(dir, IndexDefinition.DEFAULT_BLOB_SIZE);
    }
    
    @After
    public void close() throws Exception {
        if (store != null) {
            store.close();
        }
    }    

    byte[] assertWrites(Directory dir, int blobSize) throws IOException {
        byte[] data = randomBytes(blobSize);
        IndexOutput o = dir.createOutput("test", IOContext.DEFAULT);
        o.writeBytes(data, data.length);
        o.close();
        
        IndexInput i = dir.openInput("test", IOContext.DEFAULT);
        assertEquals(blobSize, i.length());
        byte[] result = new byte[blobSize];
        i.readBytes(result, 0, result.length);
        assertTrue(Arrays.equals(data, result));
        
        // Load agagin to see if cached
        i = dir.openInput("test", IOContext.DEFAULT);
        assertEquals(blobSize, i.length());
        result = new byte[blobSize];
        i.readBytes(result, 0, result.length);
        assertTrue(Arrays.equals(data, result));
        
        assertEquals(1, fileDataStore.count);
        
        return data;
    }

    private Directory createDir(NodeBuilder builder, boolean readOnly){
        return new OakDirectory(builder,
                new IndexDefinition(root, builder.getNodeState(), "/foo"), readOnly);
    }

    byte[] randomBytes(int size) {
        byte[] data = new byte[size];
        rnd.nextBytes(data);
        return data;
    }
    
    class ReadAccessCountingDataStore extends OakFileDataStore {
        int count;
        
        @Override
        public DataRecord getRecord(DataIdentifier identifier) throws DataStoreException {
            count++;
            return super.getRecord(identifier);
        }
    }
}