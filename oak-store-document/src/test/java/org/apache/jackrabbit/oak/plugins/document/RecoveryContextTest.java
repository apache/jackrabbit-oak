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

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.Test;

import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class RecoveryContextTest {

    @Test
    public void recoveryContext() {
        DocumentStore store = new MemoryDocumentStore();
        NodeDocument doc = new NodeDocument(store);
        int clusterId = 1;

        RevisionContext context = new RecoveryContext(doc, Clock.SIMPLE,
                ClusterNodeInfo.DEFAULT_RECOVERY_DELAY_MILLIS,
                clusterId, (r, d) -> null);
        assertEquals(clusterId, context.getClusterId());
        assertEquals(ClusterNodeInfo.DEFAULT_RECOVERY_DELAY_MILLIS, context.getRecoveryDelayMillis());
        assertEquals(0, context.getBranches().size());
        assertThat(context.getPendingModifications().getPaths(), empty());
        assertEquals(clusterId, context.newRevision().getClusterId());
    }

    @Test
    public void recoveryDelay() {
        DocumentStore store = new MemoryDocumentStore();
        NodeDocument doc = new NodeDocument(store);
        int clusterId = 1;

        RevisionContext context = new RecoveryContext(doc, Clock.SIMPLE,
                42,
                clusterId, (r, d) -> null);
        assertEquals(42, context.getRecoveryDelayMillis());
    }
}
