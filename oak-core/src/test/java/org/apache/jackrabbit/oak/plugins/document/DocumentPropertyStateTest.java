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

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static org.junit.Assert.assertEquals;

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

}
