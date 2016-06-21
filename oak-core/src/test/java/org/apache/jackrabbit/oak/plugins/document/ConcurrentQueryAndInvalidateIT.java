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
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getIdFromPath;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getKeyLowerLimit;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getKeyUpperLimit;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeFalse;

public class ConcurrentQueryAndInvalidateIT extends AbstractMultiDocumentStoreTest {

    public ConcurrentQueryAndInvalidateIT(DocumentStoreFixture dsf) {
        super(dsf);
        assumeFalse(dsf instanceof DocumentStoreFixture.RDBFixture);
    }

    protected static final int NUM_NODES = 50;

    private volatile long counter;

    @Test
    public void cacheUpdate() throws Exception {
        cacheUpdate(false);
    }

    @Test
    public void cacheUpdateInvalidateAll() throws Exception {
        cacheUpdate(true);
    }

    private void cacheUpdate(final boolean invalidateAll) throws Exception {
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
        ds1.remove(NODES, ids);
        ds1.create(NODES, ops);

        for (int i = 0; i < 200; i++) {
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
                    if (invalidateAll) {
                        ds1.invalidateCache();
                    } else {
                        ds1.invalidateCache(ids);
                    }
                }
            });
            q.start();
            u.start();
            q.join();
            u.join();
            for (int j = 0; j < NUM_NODES; j++) {
                NodeDocument doc = ds1.find(NODES, Utils.getIdFromPath("/node-" + j));
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
        ds1.query(NODES, getKeyLowerLimit("/"), getKeyUpperLimit("/"), 100);
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
