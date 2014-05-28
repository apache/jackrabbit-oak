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
}
