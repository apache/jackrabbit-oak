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

import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.NO_BINARY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SplitOperationsTest extends BaseDocumentMKTest {

    private static final String ROOT_ID = Utils.getIdFromPath("/");

    @Override
    public void initDocumentMK() {
        // prevent automatic background operations
        mk = new DocumentMK.Builder().setAsyncDelay(0).open();
    }

    @Test
    public void malformedStalePrev() throws Exception {
        DocumentNodeStore ns = getDocumentMK().getNodeStore();
        DocumentStore ds = ns.getDocumentStore();
        for (int i = 0; i < 5; i++) {
            NodeBuilder nb = ns.getRoot().builder();
            nb.setProperty("p", "v" + i);
            ns.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }

        // add a dangling _stalePrev
        UpdateOp op = new UpdateOp(ROOT_ID, false);
        NodeDocument.setStalePrevious(op, ns.newRevision(), 0);
        assertNotNull(ds.findAndUpdate(NODES, op));

        NodeDocument doc = ds.find(NODES, ROOT_ID);
        assertNotNull(doc);
        for (UpdateOp updateOp : SplitOperations.forDocument(
                doc, ns, ns.getHeadRevision(), NO_BINARY, 3)) {
            ds.createOrUpdate(NODES, updateOp);
        }

        doc = ds.find(NODES, ROOT_ID);
        assertNotNull(doc);
        // now _stalePrev entry must be gone
        assertEquals(0, doc.getStalePrev().size());
    }
}
