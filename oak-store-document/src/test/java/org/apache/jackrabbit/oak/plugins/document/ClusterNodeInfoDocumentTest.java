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

import java.util.List;

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.RecoveryHandler.NOOP;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ClusterNodeInfoDocumentTest {

    private DocumentStore store = new MemoryDocumentStore();

    @Test
    public void invisibleTrue() {
        assertFalse(createInactive(true).isInvisible());
    }

    @Test
    public void invisibleFalse() {
        assertFalse(createInactive(false).isInvisible());
    }

    @Test
    public void compatibility1_18() {
        ClusterNodeInfoDocument doc = createInactive(false);
        // remove invisible field introduced after 1.18
        UpdateOp op = new UpdateOp(String.valueOf(doc.getClusterId()), false);
        op.remove(ClusterNodeInfo.INVISIBLE);
        assertNotNull(store.findAndUpdate(Collection.CLUSTER_NODES, op));
        List<ClusterNodeInfoDocument> docs = ClusterNodeInfoDocument.all(store);
        assertThat(docs, hasSize(1));
        assertFalse(docs.get(0).isInvisible());
    }

    private ClusterNodeInfoDocument createInactive(boolean invisible) {
        int clusterId = 1;
        ClusterNodeInfo.getInstance(store, NOOP, "machineId", "instanceId", clusterId, invisible).dispose();
        return store.find(Collection.CLUSTER_NODES, String.valueOf(clusterId));
    }
}
