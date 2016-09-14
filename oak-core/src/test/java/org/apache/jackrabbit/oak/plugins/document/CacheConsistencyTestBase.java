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
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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
        for (String id : removeMe) {
            ds.remove(Collection.NODES, id);
        }
        ds.dispose();
        fixture.dispose();
    }

    @Test
    public void testExceptionInvalidatesCache() {
        String id = this.getClass().getName() + ".testExceptionInvalidatesCache";
        UpdateOp up = new UpdateOp(id, true);
        up.set("_id", id);
        up.set("_test", "oldvalue");
        ds.create(Collection.NODES, Collections.singletonList(up));
        removeMe.add(id);
        // make sure cache is populated
        NodeDocument olddoc = ds.find(Collection.NODES, id, 1000);
        assertEquals("oldvalue", olddoc.get("_test"));

        // findAndUpdate
        try {
            // make sure cache is populated
            olddoc = ds.find(Collection.NODES, id);

            String random = UUID.randomUUID().toString();
            setTemporaryUpdateException(random);
            try {
                up = new UpdateOp(id, false);
                up.set("_id", id);
                up.set("_test", random);
                ds.findAndUpdate(Collection.NODES, up);
                fail("should have failed with DocumentStoreException");
            } catch (DocumentStoreException ex) {
                assertEquals("should fail with enforced exception", ex.getCause().getMessage(), random);
                // make sure cache was invalidated
                NodeDocument newdoc = ds.find(Collection.NODES, id, 1000);
                assertNotNull(newdoc);
                assertEquals(random, newdoc.get("_test"));
            }
        } finally {
            setTemporaryUpdateException(null);
        }

        // update
        try {
            // make sure cache is populated
            olddoc = ds.find(Collection.NODES, id);

            String random = UUID.randomUUID().toString();
            setTemporaryUpdateException(random);
            try {
                up = new UpdateOp(id, false);
                up.set("_id", id);
                up.set("_test", random);
                ds.update(Collection.NODES, Collections.singletonList(id), up);
                fail("should have failed with DocumentStoreException");
            } catch (DocumentStoreException ex) {
                assertEquals("should fail with enforced exception", ex.getCause().getMessage(), random);
                // make sure cache was invalidated
                NodeDocument newdoc = ds.find(Collection.NODES, id, 1000);
                assertNotNull(newdoc);
                assertEquals(random, newdoc.get("_test"));
            }
        } finally {
            setTemporaryUpdateException(null);
        }

        // createOrUpdate
        try {
            // make sure cache is populated
            olddoc = ds.find(Collection.NODES, id);

            String random = UUID.randomUUID().toString();
            setTemporaryUpdateException(random);
            try {
                up = new UpdateOp(id, false);
                up.set("_id", id);
                up.set("_test", random);
                ds.createOrUpdate(Collection.NODES, up);
                fail("should have failed with DocumentStoreException");
            } catch (DocumentStoreException ex) {
                assertEquals("should fail with enforced exception", ex.getCause().getMessage(), random);
                // make sure cache was invalidated
                NodeDocument newdoc = ds.find(Collection.NODES, id, 1000);
                assertNotNull(newdoc);
                assertEquals(random, newdoc.get("_test"));
            }
        } finally {
            setTemporaryUpdateException(null);
        }

        // delete
        try {
            String random = UUID.randomUUID().toString();
            setTemporaryUpdateException(random);
            try {
                ds.remove(Collection.NODES, id);
                fail("should have failed with DocumentStoreException");
            } catch (DocumentStoreException ex) {
                assertEquals("should fail with enforced exception", ex.getCause().getMessage(), random);
                // make sure cache was invalidated
                NodeDocument newdoc = ds.find(Collection.NODES, id, 1000);
                assertNull(newdoc);
            }
        } finally {
            setTemporaryUpdateException(null);
        }
    }
}
