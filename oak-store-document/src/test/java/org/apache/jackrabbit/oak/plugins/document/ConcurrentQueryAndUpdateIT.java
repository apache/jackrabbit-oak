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

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getIdFromPath;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getKeyLowerLimit;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getKeyUpperLimit;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;

import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.junit.Test;

import com.google.common.collect.Lists;

public class ConcurrentQueryAndUpdateIT extends AbstractDocumentStoreTest {

    public ConcurrentQueryAndUpdateIT(DocumentStoreFixture dsf) {
        super(dsf);
    }

    protected static final int NUM_NODES = 50;

    private volatile long counter;

    @Test
    public void cacheUpdate() throws Exception {
        Revision r = newRevision();
        List<UpdateOp> ops = Lists.newArrayList();
        List<String> ids = Lists.newArrayList();
        for (int i = 0; i < NUM_NODES; i++) {
            String id = Utils.getIdFromPath("/node-" + i);
            ids.add(id);
            UpdateOp op = new UpdateOp(id, true);
            NodeDocument.setLastRev(op, r);
            ops.add(op);
            removeMe.add(id);
        }
        ds.remove(NODES, ids);
        ds.create(NODES, ops);

        for (int i = 0; i < 1000; i++) {
            Thread q = new Thread(new Runnable() {
                @Override
                public void run() {
                    queryDocuments();
                }
            });
            Thread u = new Thread(new Runnable() {
                @Override
                public void run() {
                    updateDocuments();
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

    private void updateDocuments() {
        UpdateOp op = new UpdateOp("foo", false);
        NodeDocument.setLastRev(op, newRevision());
        List<UpdateOp> ops = Lists.newArrayList();
        for (int i = 0; i < NUM_NODES; i++) {
            ops.add(op.shallowCopy(getIdFromPath("/node-" + i)));
        }
        ds.createOrUpdate(NODES, ops);
    }
}

