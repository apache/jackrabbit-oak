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

package org.apache.jackrabbit.oak.plugins.document;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.blob.ReferencedBlob;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.jackrabbit.oak.plugins.document.MongoBlobGCTest.randomStream;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class BlobReferenceIteratorTest {
    private DocumentStoreFixture fixture;

    private DocumentNodeStore store;

    public BlobReferenceIteratorTest(DocumentStoreFixture fixture) {
        this.fixture = fixture;
    }

    @Parameterized.Parameters(name="{0}")
    public static java.util.Collection<Object[]> fixtures() throws IOException {
        List<Object[]> fixtures = Lists.newArrayList();
        fixtures.add(new Object[] { new DocumentStoreFixture.MemoryFixture() });

        DocumentStoreFixture mongo = new DocumentStoreFixture.MongoFixture();
        if (mongo.isAvailable()) {
            fixtures.add(new Object[] { mongo });
        }

        DocumentStoreFixture rdb = new DocumentStoreFixture.RDBFixture();
        if (rdb.isAvailable()) {
            fixtures.add(new Object[] { rdb });
        }
        return fixtures;
    }

    @Before
    public void setUp() throws InterruptedException {
        store = new DocumentMK.Builder()
                .setDocumentStore(fixture.createDocumentStore())
                .setAsyncDelay(0)
                .getNodeStore();
    }

    @After
    public void tearDown() throws Exception {
        store.dispose();
        fixture.dispose();
    }

    @Test
    public void testBlobIterator() throws Exception {
        List<ReferencedBlob> blobs = Lists.newArrayList();

        // 1. Set some single value Binary property
        for (int i = 0; i < 10; i++) {
            NodeBuilder b1 = store.getRoot().builder();
            Blob b = store.createBlob(randomStream(i, 4096));
            b1.child("x").child("y" + 1).setProperty("b" + i, b);
            blobs.add(new ReferencedBlob(b, "/x/y" + 1));
            store.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }

        List<ReferencedBlob> collectedBlobs = ImmutableList.copyOf(store.getReferencedBlobsIterator());
        assertEquals(blobs.size(), collectedBlobs.size());
        assertEquals(new HashSet<ReferencedBlob>(blobs), new HashSet<ReferencedBlob>(collectedBlobs));
    }
}
