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
import static org.junit.Assume.assumeTrue;

import java.util.ArrayList;
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
    public void testRDBQuery() {
        if (ds instanceof RDBDocumentStore) {
            RDBDocumentStore rds = (RDBDocumentStore) ds;
            // create ten documents
            long now = System.currentTimeMillis();
            String base = this.getClass().getName() + ".testRDBQuery-";
            for (int i = 0; i < 10; i++) {
                String id = base + i;
                UpdateOp up = new UpdateOp(id, true);
                up.set("_id", id);
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
            List<NodeDocument> result = rds.query(Collection.NODES, base, base + "A", conditions, 10);
            assertEquals(4, result.size());
        }
    }
}
