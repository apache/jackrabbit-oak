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

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableList;

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.Collection.SETTINGS;
import static org.apache.jackrabbit.oak.plugins.document.FormatVersion.V0;
import static org.apache.jackrabbit.oak.plugins.document.FormatVersion.V1_0;
import static org.apache.jackrabbit.oak.plugins.document.FormatVersion.V1_2;
import static org.apache.jackrabbit.oak.plugins.document.FormatVersion.V1_4;
import static org.apache.jackrabbit.oak.plugins.document.FormatVersion.V1_6;
import static org.apache.jackrabbit.oak.plugins.document.FormatVersion.V1_8;
import static org.apache.jackrabbit.oak.plugins.document.FormatVersion.valueOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class FormatVersionTest {

    @Test
    public void canRead() {
        assertTrue(V1_8.canRead(V1_8));
        assertTrue(V1_8.canRead(V1_6));
        assertTrue(V1_8.canRead(V1_4));
        assertTrue(V1_8.canRead(V1_2));
        assertTrue(V1_8.canRead(V1_0));
        assertTrue(V1_8.canRead(V0));
        assertFalse(V1_6.canRead(V1_8));
        assertTrue(V1_6.canRead(V1_6));
        assertTrue(V1_6.canRead(V1_4));
        assertTrue(V1_6.canRead(V1_2));
        assertTrue(V1_6.canRead(V1_0));
        assertTrue(V1_6.canRead(V0));
        assertFalse(V1_4.canRead(V1_8));
        assertFalse(V1_4.canRead(V1_6));
        assertTrue(V1_4.canRead(V1_4));
        assertTrue(V1_4.canRead(V1_2));
        assertTrue(V1_4.canRead(V1_0));
        assertTrue(V1_4.canRead(V0));
        assertFalse(V1_2.canRead(V1_8));
        assertFalse(V1_2.canRead(V1_6));
        assertFalse(V1_2.canRead(V1_4));
        assertTrue(V1_2.canRead(V1_2));
        assertTrue(V1_2.canRead(V1_0));
        assertTrue(V1_2.canRead(V0));
        assertFalse(V1_0.canRead(V1_8));
        assertFalse(V1_0.canRead(V1_6));
        assertFalse(V1_0.canRead(V1_4));
        assertFalse(V1_0.canRead(V1_2));
        assertTrue(V1_0.canRead(V1_0));
        assertTrue(V1_0.canRead(V0));
    }

    @Test
    public void toStringValueOf() {
        for (FormatVersion v : FormatVersion.values()) {
            String s = v.toString();
            assertSame(v, valueOf(s));
        }
    }

    @Test
    public void valueOfUnknown() {
        String s = "0.9.7";
        FormatVersion v = valueOf(s);
        assertEquals(s, v.toString());
    }

    @Test
    public void versionOf() throws Exception {
        DocumentStore store = new MemoryDocumentStore();
        FormatVersion v = FormatVersion.versionOf(store);
        assertSame(V0, v);
    }

    @Test
    public void writeTo() throws Exception {
        DocumentStore store = new MemoryDocumentStore();
        // must not write dummy version
        assertFalse(V0.writeTo(store));
        // upgrade
        for (FormatVersion v : ImmutableList.of(V1_0, V1_2, V1_4, V1_6, V1_8)) {
            assertTrue(v.writeTo(store));
            assertSame(v, FormatVersion.versionOf(store));
        }
    }

    @Test(expected = DocumentStoreException.class)
    public void downgrade() throws Exception {
        DocumentStore store = new MemoryDocumentStore();
        assertTrue(V1_4.writeTo(store));
        // must not downgrade
        V1_2.writeTo(store);
    }

    @Test(expected = DocumentStoreException.class)
    public void activeClusterNodes() throws Exception {
        DocumentStore store = new MemoryDocumentStore();
        V1_0.writeTo(store);
        ClusterNodeInfo info = ClusterNodeInfo.getInstance(store, 1);
        info.renewLease();
        V1_2.writeTo(store);
    }

    @Test(expected = DocumentStoreException.class)
    public void concurrentUpdate1() throws Exception {
        DocumentStore store = new MemoryDocumentStore() {
            private final AtomicBoolean once = new AtomicBoolean(false);
            @Override
            public <T extends Document> T findAndUpdate(Collection<T> collection,
                                                        UpdateOp update) {
                if (collection == SETTINGS
                        && !once.getAndSet(true)) {
                    V1_2.writeTo(this);
                }
                return super.findAndUpdate(collection, update);
            }
        };
        V1_0.writeTo(store);
        V1_2.writeTo(store);
    }

    @Test(expected = DocumentStoreException.class)
    public void concurrentUpdate2() throws Exception {
        DocumentStore store = new MemoryDocumentStore() {
            private final AtomicBoolean once = new AtomicBoolean(false);

            @Override
            public <T extends Document> boolean create(Collection<T> collection,
                                                       List<UpdateOp> updateOps) {
                if (collection == SETTINGS
                        && !once.getAndSet(true)) {
                    V1_0.writeTo(this);
                }
                return super.create(collection, updateOps);
            }
        };
        V1_0.writeTo(store);
    }
}
