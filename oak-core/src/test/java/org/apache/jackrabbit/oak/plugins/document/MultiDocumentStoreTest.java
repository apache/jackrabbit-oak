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
        removeMe.add(id);

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
        assertTrue(super.ds1.create(Collection.NODES, Collections.singletonList(up)));
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
            super.ds2.update(Collection.NODES, Collections.singletonList(id), upds2);
            nd2 = super.ds2.find(Collection.NODES, id);
            n2 = nd2.getModCount().intValue();
            assertEquals(oldn1 + 1, n2);
            assertEquals("qux", nd2.get("foo"));

            // both stores are now at the same modCount with different contents
            upds1 = new UpdateOp(id, true);
            upds1.set("_id", id);
            upds1.set("foo", "barbar");
            NodeDocument prev = super.ds1.findAndUpdate(Collection.NODES, upds1);
            // prev document should contain mod from DS2
            assertEquals("qux", prev.get("foo"));
            assertEquals(oldn1 + 2, prev.getModCount().intValue());
        }
    }
}
