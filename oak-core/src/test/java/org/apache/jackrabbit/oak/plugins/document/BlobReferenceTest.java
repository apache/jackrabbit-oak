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
package org.apache.jackrabbit.oak.plugins.document;

import static junit.framework.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.blob.ReferencedBlob;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

/**
 * Test the BlobReferenceIterator.
 */
public class BlobReferenceTest {

    @Test
    public void test() throws Exception {
        DocumentNodeStore s = new DocumentMK.Builder().getNodeStore();
        NodeBuilder a = s.getRoot().builder();
        HashSet<String> set = new HashSet<String>();
        for (int i = 0; i < 100; i++) {
            Blob b = a.createBlob(randomStream(i, 10));
            set.add(new ReferencedBlob(b, "/c" + i).toString());
            a.child("c" + i).setProperty("x", b);
        }
        s.merge(a, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        Iterator<ReferencedBlob> it = s.getReferencedBlobsIterator();
        while (it.hasNext()) {
            ReferencedBlob b = it.next();
            set.remove(b.toString());
        }
        assertTrue(set.isEmpty());
        s.dispose();
    }

    static InputStream randomStream(int seed, int size) {
        Random r = new Random(seed);
        byte[] data = new byte[size];
        r.nextBytes(data);
        return new ByteArrayInputStream(data);
    }

}
