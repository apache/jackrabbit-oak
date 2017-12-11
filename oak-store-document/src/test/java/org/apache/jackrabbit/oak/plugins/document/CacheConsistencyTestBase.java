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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class CacheConsistencyTestBase {

    private DocumentStoreFixture fixture;
    private DocumentStore ds;
    private List<String> removeMe = new ArrayList<String>();

    public abstract DocumentStoreFixture getFixture() throws Exception;

    public abstract void setTemporaryUpdateException(String msg);

    @Before
    public void before() throws Exception{
        fixture = getFixture();
        ds = fixture.createDocumentStore(1);
    }

    @After
    public void after() throws Exception {
        if (ds != null) {
            for (String id : removeMe) {
                ds.remove(Collection.NODES, id);
            }
            ds.dispose();
        }
        fixture.dispose();
    }

    @Test
    public void testExceptionInvalidatesCache() {
        String id1 = this.getClass().getName() + ".testExceptionInvalidatesCache1";
        UpdateOp up1 = new UpdateOp(id1, true);
        up1.set("_test", "oldvalue");
        String id2 = this.getClass().getName() + ".testExceptionInvalidatesCache2";
        UpdateOp up2 = new UpdateOp(id2, true);
        up2.set("_test", "oldvalue");
        String id3 = this.getClass().getName() + ".testExceptionInvalidatesCache3";
        UpdateOp up3 = new UpdateOp(id3, true);
        up3.set("_test", "oldvalue");
        ds.create(Collection.NODES, Lists.newArrayList(up1, up2, up3));
        removeMe.add(id1);
        removeMe.add(id2);
        removeMe.add(id3);
        // make sure cache is populated
        NodeDocument olddoc = ds.find(Collection.NODES, id1, 1000);
        assertEquals("oldvalue", olddoc.get("_test"));
        olddoc = ds.find(Collection.NODES, id2, 1000);
        assertEquals("oldvalue", olddoc.get("_test"));
        olddoc = ds.find(Collection.NODES, id3, 1000);
        assertEquals("oldvalue", olddoc.get("_test"));

        // findAndUpdate
        try {
            // make sure cache is populated
            olddoc = ds.find(Collection.NODES, id1);

            String random = UUID.randomUUID().toString();
            setTemporaryUpdateException(random);
            try {
                up1 = new UpdateOp(id1, false);
                up1.set("_test", random);
                ds.findAndUpdate(Collection.NODES, up1);
                fail("should have failed with DocumentStoreException");
            } catch (DocumentStoreException ex) {
                assertEquals("should fail with enforced exception", ex.getCause().getMessage(), random);
                // make sure cache was invalidated
                NodeDocument newdoc = ds.find(Collection.NODES, id1, 1000);
                assertNotNull(newdoc);
                assertEquals(random, newdoc.get("_test"));
            }
        } finally {
            setTemporaryUpdateException(null);
        }

        // createOrUpdate
        try {
            // make sure cache is populated
            olddoc = ds.find(Collection.NODES, id1);

            String random = UUID.randomUUID().toString();
            setTemporaryUpdateException(random);
            try {
                up1 = new UpdateOp(id1, false);
                up1.set("_test", random);
                ds.createOrUpdate(Collection.NODES, up1);
                fail("should have failed with DocumentStoreException");
            } catch (DocumentStoreException ex) {
                assertEquals("should fail with enforced exception", ex.getCause().getMessage(), random);
                // make sure cache was invalidated
                NodeDocument newdoc = ds.find(Collection.NODES, id1, 1000);
                assertNotNull(newdoc);
                assertEquals(random, newdoc.get("_test"));
            }
        } finally {
            setTemporaryUpdateException(null);
        }

        // createOrUpdate multiple
        try {
            // make sure cache is populated
            olddoc = ds.find(Collection.NODES, id1);
            assertNotNull(olddoc);
            olddoc = ds.find(Collection.NODES, id2);
            assertNotNull(olddoc);
            olddoc = ds.find(Collection.NODES, id3);
            assertNotNull(olddoc);

            String random = UUID.randomUUID().toString();
            setTemporaryUpdateException(random);
            try {
                up1 = new UpdateOp(id1, false);
                up1.set("_test", random);
                up2 = new UpdateOp(id2, false);
                up2.set("_test", random);
                up3 = new UpdateOp(id3, false);
                up3.set("_test", random);

                ds.createOrUpdate(Collection.NODES, Lists.newArrayList(up1, up2, up3));
                fail("should have failed with DocumentStoreException");
            } catch (DocumentStoreException ex) {
                assertEquals("should fail with enforced exception", ex.getCause().getMessage(), random);

                // expected post conditions:
                // 1) at least one of the documents should be updated
                // 2) for all documents: reading from cache and uncached
                // should return the same document
                Set<String> modifiedDocuments = Sets.newHashSet();

                for (String id : new String[] { id1, id2, id3 }) {
                    // get cached value
                    NodeDocument newdoc = ds.find(Collection.NODES, id, 1000);
                    if (random.equals(newdoc.get("_test"))) {
                        modifiedDocuments.add(id);
                    }
                    // compare with persisted value
                    assertEquals(newdoc.get("_test"), ds.find(Collection.NODES, id, 0).get("_test"));
                }

                assertTrue("at least one document should have been updated", !modifiedDocuments.isEmpty());
            }
        } finally {
            setTemporaryUpdateException(null);
        }

        // delete
        try {
            String random = UUID.randomUUID().toString();
            setTemporaryUpdateException(random);
            try {
                ds.remove(Collection.NODES, id1);
                fail("should have failed with DocumentStoreException");
            } catch (DocumentStoreException ex) {
                assertEquals("should fail with enforced exception", ex.getCause().getMessage(), random);
                // make sure cache was invalidated
                NodeDocument newdoc = ds.find(Collection.NODES, id1, 1000);
                assertNull(newdoc);
            }
        } finally {
            setTemporaryUpdateException(null);
        }
    }
}
