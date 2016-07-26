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
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class BulkCreateOrUpdateTest extends AbstractDocumentStoreTest {

    public BulkCreateOrUpdateTest(DocumentStoreFixture dsf) {
        super(dsf);
    }

    /**
     * This tests create multiple items using createOrUpdate() method. The
     * return value should be a list of null values.
     */
    @Test
    public void testCreateMultiple() {
        final int amount = 100;

        List<UpdateOp> updates = new ArrayList<UpdateOp>(amount);

        for (int i = 0; i < amount; i++) {
            String id = this.getClass().getName() + ".testCreateMultiple" + i;
            UpdateOp up = new UpdateOp(id, true);
            up.set("_id", id);
            updates.add(up);
            removeMe.add(id);
        }

        List<NodeDocument> docs = ds.createOrUpdate(Collection.NODES, updates);
        assertEquals(amount, docs.size());
        for (int i = 0; i < amount; i++) {
            assertNull("There shouldn't be a value for created doc", docs.get(i));
            assertNotNull("The node hasn't been created", ds.find(Collection.NODES, updates.get(i).getId()));
        }
    }

    /**
     * This method updates multiple items using createOrUpdate() method. The
     * return value should be a list of items before the update.
     */
    @Test
    public void testUpdateMultiple() {
        final int amount = 100;
        List<UpdateOp> updates = new ArrayList<UpdateOp>(amount);

        for (int i = 0; i < amount; i++) {
            String id = this.getClass().getName() + ".testUpdateMultiple" + i;
            UpdateOp up = new UpdateOp(id, true);
            up.set("_id", id);
            up.set("prop", 100);
            updates.add(up);
            removeMe.add(id);
        }

        ds.create(Collection.NODES, updates);

        for (int i = 0; i < amount; i++) {
            UpdateOp up = updates.get(i).copy();
            up.set("prop", 200);
            updates.set(i, up);
        }

        List<NodeDocument> docs = ds.createOrUpdate(Collection.NODES, updates);
        assertEquals(amount, docs.size());
        for (int i = 0; i < amount; i++) {
            NodeDocument oldDoc = docs.get(i);
            String id = oldDoc.getId();
            NodeDocument newDoc = ds.find(Collection.NODES, id);
            assertEquals("The result list order is incorrect", updates.get(i).getId(), id);
            assertEquals("The old value is not correct", 100l, oldDoc.get("prop"));
            assertEquals("The document hasn't been updated", 200l, newDoc.get("prop"));
        }
    }

    /**
     * This method creates or updates multiple items using createOrUpdate()
     * method. New items have odd indexes and updates items have even indexes.
     * The return value should be a list of old documents (for the updates) or
     * nulls (for the inserts).
     */
    @Test
    public void testCreateOrUpdateMultiple() {
        int amount = 100;
        List<UpdateOp> updates = new ArrayList<UpdateOp>(amount);

        // create even items
        for (int i = 0; i < amount; i += 2) {
            String id = this.getClass().getName() + ".testCreateOrUpdateMultiple" + i;
            UpdateOp up = new UpdateOp(id, true);
            up.set("_id", id);
            up.set("prop", 100);
            updates.add(up);
            removeMe.add(id);
        }
        ds.create(Collection.NODES, updates);
        updates.clear();

        // createOrUpdate all items
        for (int i = 0; i < amount; i++) {
            String id = this.getClass().getName() + ".testCreateOrUpdateMultiple" + i;
            UpdateOp up = new UpdateOp(id, true);
            up.set("_id", id);
            up.set("prop", 200);
            updates.add(up);
            removeMe.add(id);
        }
        List<NodeDocument> docs = ds.createOrUpdate(Collection.NODES, updates);

        assertEquals(amount, docs.size());
        for (int i = 0; i < amount; i++) {
            String id = this.getClass().getName() + ".testCreateOrUpdateMultiple" + i;

            NodeDocument oldDoc = docs.get(i);
            NodeDocument newDoc = ds.find(Collection.NODES, id);
            if (i % 2 == 1) {
                assertNull("The returned value should be null for created doc", oldDoc);
            } else {
                assertNotNull("The returned doc shouldn't be null for updated doc", oldDoc);
                assertEquals("The old value is not correct", 100l, oldDoc.get("prop"));
                assertEquals("The result list order is incorrect", updates.get(i).getId(), oldDoc.getId());
            }
            assertEquals("The document hasn't been updated", 200l, newDoc.get("prop"));
        }
    }
}
