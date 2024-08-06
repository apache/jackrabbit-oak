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
package org.apache.jackrabbit.oak.plugins.document;

import static org.apache.jackrabbit.guava.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.guava.common.collect.Sets.newHashSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.mongodb.ReadPreference;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoTestUtils;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class DocumentPropertyStateTest {

    private static final int BLOB_SIZE = 16 * 1024;

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private Set<String> reads = newHashSet();

    private BlobStore bs = new MemoryBlobStore() {
        @Override
        public InputStream getInputStream(String blobId)
                throws IOException {
            reads.add(blobId);
            return super.getInputStream(blobId);
        }
    };

    private DocumentNodeStore ns;

    @Before
    public void before() throws Exception {
        ns = builderProvider.newBuilder().setBlobStore(bs).getNodeStore();
    }

    // OAK-5462
    @Test
    public void multiValuedBinarySize() throws Exception {
        NodeBuilder builder = ns.getRoot().builder();
        List<Blob> blobs = newArrayList();
        for (int i = 0; i < 3; i++) {
            blobs.add(builder.createBlob(new RandomStream(BLOB_SIZE, i)));
        }
        builder.child("test").setProperty("p", blobs, Type.BINARIES);
        TestUtils.merge(ns, builder);

        PropertyState p = ns.getRoot().getChildNode("test").getProperty("p");
        assertEquals(Type.BINARIES, p.getType());
        assertEquals(3, p.count());

        reads.clear();
        assertEquals(BLOB_SIZE, p.size(0));
        // must not read the blob via stream
        assertEquals(0, reads.size());
    }

    @Test
    public void testBrokenSurrogateWithoutCompressionForMongo() throws CommitFailedException {
        getBrokenSurrogateAndInitializeDifferentStores(DocumentStoreFixture.MONGO, false);
    }

    @Test
    public void testBrokenSurrogateWithoutCompressionForRDB() throws CommitFailedException {
        getBrokenSurrogateAndInitializeDifferentStores(DocumentStoreFixture.RDB_H2, false);
    }

    @Test
    public void testBrokenSurrogateWithoutCompressionForInMemory() throws CommitFailedException {
        getBrokenSurrogateAndInitializeDifferentStores(DocumentStoreFixture.MEMORY, false);
    }

    @Test
    public void testBrokenSurrogateWithCompressionForMongo() throws CommitFailedException {
        CompressedDocumentPropertyState.setCompressionThreshold(1);
        getBrokenSurrogateAndInitializeDifferentStores(DocumentStoreFixture.MONGO, true);
    }

    @Test
    public void testBrokenSurrogateWithCompressionForRDB() throws CommitFailedException {
        CompressedDocumentPropertyState.setCompressionThreshold(1);
        getBrokenSurrogateAndInitializeDifferentStores(DocumentStoreFixture.RDB_H2, true);
    }

    @Test
    public void testBrokenSurrogateWithCompressionForInMemory() throws CommitFailedException {
        CompressedDocumentPropertyState.setCompressionThreshold(1);
        getBrokenSurrogateAndInitializeDifferentStores(DocumentStoreFixture.MEMORY, true);
    }

    private void getBrokenSurrogateAndInitializeDifferentStores(DocumentStoreFixture fixture, boolean compressionEnabled) throws CommitFailedException {
        assumeTrue(fixture.isAvailable());
        String test = "brokensurrogate:dfsa\ud800";
        DocumentStore store = null;
        DocumentNodeStore nodeStore = null;

        try {
            store = fixture.createDocumentStore();

            if (store instanceof MongoDocumentStore) {
                // Enforce primary read preference, otherwise tests may fail on a
                // replica set with a read preference configured to secondary.
                // Revision GC usually runs with a modified range way in the past,
                // which means changes made it to the secondary, but not in this
                // test using a virtual clock
                MongoTestUtils.setReadPreference(store, ReadPreference.primary());
            }
            nodeStore = new DocumentMK.Builder().setDocumentStore(store).getNodeStore();

            createPropAndCheckValue(nodeStore, test, compressionEnabled);

        } finally {
            if (nodeStore != null) {
                nodeStore.dispose();
            }
        }

    }

    private void createPropAndCheckValue(DocumentNodeStore nodeStore, String test, boolean compressionEnabled) throws CommitFailedException {
        NodeBuilder builder = nodeStore.getRoot().builder();

        if (compressionEnabled) {
            CompressedDocumentPropertyState.setCompressionThreshold(1);
        }
        builder.child("test").setProperty("p", test, Type.STRING);
        TestUtils.merge(nodeStore, builder);

        PropertyState p = nodeStore.getRoot().getChildNode("test").getProperty("p");
        assertEquals(Objects.requireNonNull(p).getValue(Type.STRING), test);
    }
}