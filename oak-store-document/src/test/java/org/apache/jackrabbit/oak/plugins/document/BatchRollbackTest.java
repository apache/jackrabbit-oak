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

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.isFinalCommitRootUpdate;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class BatchRollbackTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private TestStore store = new TestStore();

    @Test
    public void batchRollback() throws Exception {
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setDocumentStore(store)
                .setAsyncDelay(0).build();
        ns.setMaxBackOffMillis(0);

        // prepare some test nodes
        NodeBuilder builder = ns.getRoot().builder();
        for (int i = 0; i < 10; i++) {
            builder.child("node-" + i);
        }
        merge(ns, builder);

        // perform an merge that fails
        builder = ns.getRoot().builder();
        for (int i = 0; i < 10; i++) {
            builder.child("node-" + i).child("test");
        }
        store.resetCounters();
        store.failCommitOnce.set(true);
        try {
            merge(ns, builder);
            fail("must fail with CommitFailedException");
        } catch (CommitFailedException e) {
            // expected
        }
        assertThat(store.getNumCreateOrUpdateCalls(NODES), lessThan(4));
    }

    private static class TestStore extends CountingDocumentStore {

        final AtomicBoolean failCommitOnce = new AtomicBoolean();

        TestStore() {
            super(new MemoryDocumentStore());
        }

        @Override
        public <T extends Document> T findAndUpdate(Collection<T> collection,
                                                    UpdateOp update) {
            if (collection == Collection.NODES
                    && isFinalCommitRootUpdate(update)
                    && failCommitOnce.compareAndSet(true, false)) {
                throw new DocumentStoreException("commit failed");
            }
            return super.findAndUpdate(collection, update);
        }
    }
}
