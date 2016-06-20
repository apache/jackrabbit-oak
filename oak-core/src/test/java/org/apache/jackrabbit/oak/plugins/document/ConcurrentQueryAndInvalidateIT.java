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

import java.util.List;

import com.google.common.collect.Lists;

import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getIdFromPath;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getKeyLowerLimit;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getKeyUpperLimit;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

public class ConcurrentQueryAndInvalidateIT extends AbstractDocumentStoreTest {

    public ConcurrentQueryAndInvalidateIT(DocumentStoreFixture dsf) {
        super(dsf);
        assumeTrue(dsf.hasSinglePersistence());
        assumeFalse(dsf instanceof DocumentStoreFixture.RDBFixture);
    }

    protected static final int NUM_NODES = 50;

    private volatile long counter;

    private DocumentStore ds2;

    @Before
    public void setupSecondDocumentStore() {
        ds2 = dsf.createDocumentStore(2);
    }

    @Test
    public void cacheUpdate() throws Exception {
        Revision r = newRevision();
        List<UpdateOp> ops = Lists.newArrayList();
        List<String> ids = Lists.newArrayList();
        for (int i = 0; i < NUM_NODES; i++) {
            String id = Utils.getIdFromPath("/node-" + i);
            ids.add(id);
            UpdateOp op = new UpdateOp(id, true);
            op.set(Document.ID, id);
            NodeDocument.setLastRev(op, r);
            ops.add(op);
            removeMe.add(id);
        }
        ds.remove(NODES, ids);
        ds.create(NODES, ops);

        for (int i = 0; i < 100; i++) {
            Thread q = new Thread(new Runnable() {
                @Override
                public void run() {
                    // query docs on ds1
                    queryDocuments();
                }
            });
            Thread u = new Thread(new Runnable() {
                @Override
                public void run() {
                    // update docs on ds2
                    Iterable<String> ids = updateDocuments();
                    // invalidate docs on ds1
                    invalidateDocuments(ids);
                }
            });
            q.start();
            u.start();
            q.join();
            u.join();
            for (int j = 0; j < NUM_NODES; j++) {
                NodeDocument doc = ds.find(NODES, Utils.getIdFromPath("/node-" + j));
                assertNotNull(doc);
                assertEquals("Unexpected revision timestamp for " + doc.getId(),
                        counter, doc.getLastRev().get(1).getTimestamp());
            }
        }
    }

    private Revision newRevision() {
        return new Revision(++counter, 0, 1);
    }

    private void queryDocuments() {
        ds.query(NODES, getKeyLowerLimit("/"), getKeyUpperLimit("/"), 100);
    }

    private void invalidateDocuments(Iterable<String> ids) {
        ds.invalidateCache(ids);
    }

    private Iterable<String> updateDocuments() {
        List<String> ids = Lists.newArrayList();
        for (int i = 0; i < NUM_NODES; i++) {
            ids.add(getIdFromPath("/node-" + i));
        }
        UpdateOp op = new UpdateOp("foo", false);
        NodeDocument.setLastRev(op, newRevision());
        ds2.update(NODES, ids, op);
        return ids;
    }
}
