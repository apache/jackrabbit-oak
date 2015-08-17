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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;

import org.junit.Test;

public class MultiDocumentStoreTest extends AbstractMultiDocumentStoreTest {

    public MultiDocumentStoreTest(DocumentStoreFixture dsf) {
        super(dsf);
    }

    @Test
    public void testInterleavedUpdate() {
        String id = this.getClass().getName() + ".testInterleavedUpdate";

        // remove if present
        NodeDocument nd = super.ds1.find(Collection.NODES, id);
        if (nd != null) {
            super.ds1.remove(Collection.NODES, id);
        }

        UpdateOp up = new UpdateOp(id, true);
        up.set("_id", id);
        up.set("_foo", 0l);
        assertTrue(super.ds1.create(Collection.NODES, Collections.singletonList(up)));
        removeMe.add(id);

        long increments = 10;

        for (int i = 0; i < increments; i++) {
            up = new UpdateOp(id, true);
            up.set("_id", id);
            up.increment("_foo", 1l);
            if (i % 2 == 0) {
                super.ds1.update(Collection.NODES, Collections.singletonList(id), up);
            }
            else {
                super.ds2.update(Collection.NODES, Collections.singletonList(id), up);
            }
        }

        // read uncached
        nd = super.ds1.find(Collection.NODES, id, 0);
        assertEquals("_foo should have been incremented 10 times", increments, nd.get("_foo"));
    }

    @Test
    public void testInterleavedUpdate2() {
        String id = this.getClass().getName() + ".testInterleavedUpdate2";

        // remove if present
        NodeDocument nd1 = super.ds1.find(Collection.NODES, id);
        if (nd1 != null) {
            super.ds1.remove(Collection.NODES, id);
        }

        UpdateOp up = new UpdateOp(id, true);
        up.set("_id", id);
        up.set("_modified", 1L);
        assertTrue(super.ds1.create(Collection.NODES, Collections.singletonList(up)));
        removeMe.add(id);

        nd1 = super.ds1.find(Collection.NODES, id, 0);
        Number n = nd1.getModCount();
        if (n != null) {
            // Document store uses modCount
            int n1 = n.intValue();

            // get the document into ds2's cache
            NodeDocument nd2 = super.ds2.find(Collection.NODES, id, 0);
            int n2 = nd2.getModCount().intValue();
            assertEquals(n1, n2);

            UpdateOp upds1 = new UpdateOp(id, true);
            upds1.set("_id", id);
            upds1.set("foo", "bar");
            upds1.set("_modified", 2L);
            super.ds1.update(Collection.NODES, Collections.singletonList(id), upds1);
            nd1 = super.ds1.find(Collection.NODES, id);
            int oldn1 = n1;
            n1 = nd1.getModCount().intValue();
            assertEquals(oldn1 + 1, n1);
            assertEquals("bar", nd1.get("foo"));

            // modify in DS2
            UpdateOp upds2 = new UpdateOp(id, true);
            upds2.set("_id", id);
            upds2.set("foo", "qux");
            upds2.set("_modified", 3L);
            super.ds2.update(Collection.NODES, Collections.singletonList(id), upds2);
            nd2 = super.ds2.find(Collection.NODES, id);
            n2 = nd2.getModCount().intValue();
            assertEquals(oldn1 + 1, n2);
            assertEquals("qux", nd2.get("foo"));

            // both stores are now at the same modCount with different contents
            upds1 = new UpdateOp(id, true);
            upds1.set("_id", id);
            upds1.set("foo", "barbar");
            upds1.max("_modified", 0L);
            NodeDocument prev = super.ds1.findAndUpdate(Collection.NODES, upds1);
            // prev document should contain mod from DS2
            assertEquals("qux", prev.get("foo"));
            assertEquals(oldn1 + 2, prev.getModCount().intValue());
            assertEquals(3L, prev.getModified().intValue());

            // the new document must not have a _modified time smaller than
            // before the update
            nd1 = super.ds1.find(Collection.NODES, id, 0);
            assertEquals(super.dsname + ": _modified value must never ever get smaller", 3L, nd1.getModified().intValue());

            // verify that _modified can indeed be *set* to a smaller value, see
            // https://jira.corp.adobe.com/browse/GRANITE-8903
            upds1 = new UpdateOp(id, true);
            upds1.set("_id", id);
            upds1.set("_modified", 0L);
            super.ds1.findAndUpdate(Collection.NODES, upds1);
            nd1 = super.ds1.find(Collection.NODES, id, 0);
            assertEquals(super.dsname + ": _modified value must be set to 0", 0L, nd1.getModified().intValue());
        }
    }

    @Test
    public void testInvalidateCache() {
        // use a "proper" ID because otherwise Mongo's cache invalidation will fail
        // see OAK-2588
        String id = "1:/" + this.getClass().getName() + ".testInvalidateCache";

        // remove if present
        NodeDocument nd = super.ds1.find(Collection.NODES, id);
        if (nd != null) {
            super.ds1.remove(Collection.NODES, id);
        }

        UpdateOp up = new UpdateOp(id, true);
        up.set("_id", id);
        up.set("_foo", "bar");
        assertTrue(super.ds1.create(Collection.NODES, Collections.singletonList(up)));
        removeMe.add(id);

        // fill both caches
        NodeDocument nd1 = super.ds1.find(Collection.NODES, id);
        NodeDocument nd2 = super.ds2.find(Collection.NODES, id);
        assertNotNull(nd1);
        assertNotNull(nd2);
        long firstVersion = nd1.getModCount().longValue();
        assertEquals(firstVersion, nd2.getModCount().longValue());

        // letTimeElapse();

        // update through ds1
        UpdateOp upds1 = new UpdateOp(id, true);
        upds1.set("_id", id);
        upds1.set("foo", "qux");
        super.ds1.update(Collection.NODES, Collections.singletonList(id), upds1);
        nd1 = super.ds1.find(Collection.NODES, id);
        assertEquals("modcount should have changed in ds1", firstVersion + 1, nd1.getModCount().longValue());

        // check cached version in ds2
        nd2 = super.ds2.find(Collection.NODES, id);
        assertEquals("ds2 should still be on first version", firstVersion, nd2.getModCount().longValue());

        // check uncached version in ds2
        nd2 = super.ds2.find(Collection.NODES, id, 0);
        assertEquals("ds2 should now see the second version", firstVersion + 1, nd2.getModCount().longValue());

        // check cached version in ds2 (was the cache refreshed?)
        NodeDocument nd2b = super.ds2.find(Collection.NODES, id);
        assertEquals("ds2 should now see the second version", firstVersion + 1, nd2b.getModCount().longValue());

        // update through ds2
        UpdateOp upds2 = new UpdateOp(id, true);
        upds2.set("_id", id);
        upds2.set("foo", "blub");
        super.ds2.update(Collection.NODES, Collections.singletonList(id), upds1);
        nd2 = super.ds2.find(Collection.NODES, id);
        assertEquals("modcount should have incremented again", firstVersion + 2, nd2.getModCount().longValue());

        long ds1checktime = nd1.getLastCheckTime();
        letTimeElapse();

        // try the invalidation
        ds1.invalidateCache();

        // ds1 should see the same version even when doing a cached read
        nd1 = super.ds1.find(Collection.NODES, id);
        assertEquals("modcount should have incremented again", firstVersion + 2, nd1.getModCount().longValue());
        assertTrue(nd1.getLastCheckTime() > ds1checktime);
    }

    private static long letTimeElapse() {
        long ts = System.currentTimeMillis();
        while (System.currentTimeMillis() == ts) {
            // busy wait
        }
        return System.currentTimeMillis();
    }
}
