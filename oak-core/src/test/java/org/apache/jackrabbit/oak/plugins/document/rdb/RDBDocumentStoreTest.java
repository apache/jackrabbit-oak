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
package org.apache.jackrabbit.oak.plugins.document.rdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.jackrabbit.oak.plugins.document.AbstractDocumentStoreTest;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreFixture;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore.QueryCondition;
import org.junit.Test;

public class RDBDocumentStoreTest extends AbstractDocumentStoreTest {

    public RDBDocumentStoreTest(DocumentStoreFixture dsf) {
        super(dsf);
    }

    @Test
    public void testRDBQueryConditions() {
        if (ds instanceof RDBDocumentStore) {
            RDBDocumentStore rds = (RDBDocumentStore) ds;
            // create ten documents
            long now = System.currentTimeMillis();
            String base = this.getClass().getName() + ".testRDBQuery-";
            for (int i = 0; i < 10; i++) {
                String id = base + i;
                UpdateOp up = new UpdateOp(id, true);
                up.set(NodeDocument.DELETED_ONCE, i % 2 == 1);
                up.set(NodeDocument.MODIFIED_IN_SECS, now++);
                boolean success = super.ds.create(Collection.NODES, Collections.singletonList(up));
                assertTrue("document with " + id + " not created", success);
                removeMe.add(id);
            }

            List<QueryCondition> conditions = new ArrayList<QueryCondition>();
            // matches every second
            conditions.add(new QueryCondition(NodeDocument.DELETED_ONCE, "=", 0));
            // matches first eight
            conditions.add(new QueryCondition(NodeDocument.MODIFIED_IN_SECS, "<", now - 2));
            List<NodeDocument> result = rds.query(Collection.NODES, base, base + "A", RDBDocumentStore.EMPTY_KEY_PATTERN,
                    conditions, 10);
            assertEquals(4, result.size());
        }
    }

    @Test
    public void testRDBQueryKeyPatterns() {
        if (ds instanceof RDBDocumentStore) {
            RDBDocumentStore rds = (RDBDocumentStore) ds;
            // create ten documents
            String base = this.getClass().getName() + ".testRDBQuery-";
            for (int i = 0; i < 10; i++) {
                // every second is a "regular" path
                String id = "1:" + (i % 2 == 1 ? "p" : "") + "/" + base + i;
                UpdateOp up = new UpdateOp(id, true);
                up.set("_test", base);
                boolean success = super.ds.create(Collection.NODES, Collections.singletonList(up));
                assertTrue("document with " + id + " not created", success);
                removeMe.add(id);
            }

            List<QueryCondition> conditions = new ArrayList<QueryCondition>();
            List<NodeDocument> result = rds.query(Collection.NODES, NodeDocument.MIN_ID_VALUE, NodeDocument.MAX_ID_VALUE,
                    Arrays.asList("_:/%", "__:/%", "___:/%"), conditions, 10000);
            for (NodeDocument d : result) {
                if (base.equals(d.get("_test"))) {
                    assertTrue(d.getId().startsWith("1:p"));
                }
            }
        }
    }
}
