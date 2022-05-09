/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.document;

import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation.Type.REMOVE_MAP_ENTRY;
import static org.junit.Assert.assertFalse;

public class OrphanedBranchCleanupTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    // OAK-8012 / OAK-8353
    @Test
    public void orphanedBranchRace() throws Exception {
        Semaphore branchCleanupInProgress = new Semaphore(0);
        Semaphore readHappened = new Semaphore(0);

        DocumentStore store = new MemoryDocumentStore() {
            @Override
            public <T extends Document> T findAndUpdate(Collection<T> collection,
                                                        UpdateOp update) {
                if (collection == NODES && isBranchCleanup(update)) {
                    branchCleanupInProgress.release();
                    readHappened.acquireUninterruptibly();
                }
                return super.findAndUpdate(collection, update);
            }

            private boolean isBranchCleanup(UpdateOp update) {
                for (Entry<Key, Operation> e : update.getChanges().entrySet()) {
                    if (e.getValue().type == REMOVE_MAP_ENTRY
                            && NodeDocument.isRevisionsEntry(e.getKey().getName())) {
                        return true;
                    }
                }
                return false;
            }
        };

        DocumentNodeStore ns = builderProvider.newBuilder()
                .setDocumentStore(store).setAsyncDelay(0).build();

        createOrphanedBranch(ns);

        // add a node below the root. this serves two purposes:
        // 1) the root state will have hasChildren set to true, otherwise
        //    a read will not even try to get children
        // 2) push the head revision after the orphaned branch commit revision
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo");
        merge(ns, builder);

        do {
            System.gc();
            executorService.submit(ns::runBackgroundOperations);
        } while (!branchCleanupInProgress.tryAcquire(100, TimeUnit.MILLISECONDS));
        ns.getNodeCache().invalidateAll();
        ns.getNodeChildrenCache().invalidateAll();
        boolean hasTestNode = ns.getRoot().hasChildNode("test");
        readHappened.release();
        new ExecutorCloser(executorService).close();
        assertFalse(hasTestNode);
    }

    private void createOrphanedBranch(DocumentNodeStore ns) {
        DocumentStore store = ns.getDocumentStore();
        String id = Utils.getIdFromPath("/test");
        NodeBuilder builder = ns.getRoot().builder();
        NodeBuilder test = builder.child("test");
        for (int i = 0; store.find(NODES, id) == null; i++) {
            test.child("n-" + i);
        }
    }
}
