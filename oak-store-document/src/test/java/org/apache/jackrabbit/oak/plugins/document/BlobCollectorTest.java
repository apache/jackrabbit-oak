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

import java.util.HashSet;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.blob.ReferencedBlob;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.plugins.memory.PropertyBuilder;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.After;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.MongoBlobGCTest.randomStream;
import static org.junit.Assert.assertEquals;

public class BlobCollectorTest {
    private DocumentNodeStore store = new DocumentMK.Builder().getNodeStore();
    private BlobCollector blobCollector = new BlobCollector(store);

    @After
    public void tearDown() throws Exception {
        store.dispose();
    }

    @Test
    public void testCollect() throws Exception {
        NodeBuilder b1 = store.getRoot().builder();
        List<ReferencedBlob> blobs = Lists.newArrayList();

        b1.child("x").child("y");
        store.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        //1. Set some single value Binary property
        for(int i = 0; i < 2; i++){
            b1 = store.getRoot().builder();
            Blob b = store.createBlob(randomStream(i, 4096));
            b1.child("x").child("y").setProperty("b" + i, b);
            blobs.add(new ReferencedBlob(b, "/x/y"));
            store.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }

        //2. Set some multi value property
        PropertyBuilder<Blob> p1 = PropertyBuilder.array(Type.BINARY)
                .setName("barr");
        for(int i = 0; i < 2; i++){
            Blob b = store.createBlob(randomStream(i, 4096));
            p1.addValue(b);
            blobs.add(new ReferencedBlob(b, "/x/y"));
        }
        b1 = store.getRoot().builder();
        b1.child("x").child("y").setProperty(p1.getPropertyState());
        store.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        //3. Create some new rev for the property b1 and b2
        for(int i = 0; i < 2; i++){
            b1 = store.getRoot().builder();
            //Change the see to create diff binary
            Blob b = store.createBlob(randomStream(i+1, 4096));
            b1.child("x").child("y").setProperty("b" + i, b);
            blobs.add(new ReferencedBlob(b, "/x/y"));
            store.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }

        NodeDocument doc =
                store.getDocumentStore().find(Collection.NODES, Utils.getIdFromPath("/x/y"));
        List<ReferencedBlob> collectedBlobs = Lists.newArrayList();
        blobCollector.collect(doc, collectedBlobs);

        assertEquals(blobs.size(), collectedBlobs.size());
        assertEquals(new HashSet<ReferencedBlob>(blobs), new HashSet<ReferencedBlob>(collectedBlobs));
    }
}
