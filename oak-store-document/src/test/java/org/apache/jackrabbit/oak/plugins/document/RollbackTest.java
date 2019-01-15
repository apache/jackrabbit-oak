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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.TestUtils.isFinalCommitRootUpdate;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class RollbackTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private ExecutorService executorService = Executors.newCachedThreadPool();

    private Clock clock = new Clock.Virtual();

    @Before
    public void before() throws Exception {
        clock.waitUntil(System.currentTimeMillis());
        Revision.setClock(clock);
    }

    @After
    public void after() throws Exception {
        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.SECONDS);
    }

    @AfterClass
    public static void resetClock() {
        Revision.resetClockToDefault();
    }

    @Test
    public void nonBlocking() throws Exception {
        TestStore store = new TestStore();
        DocumentNodeStore ns = builderProvider.newBuilder()
                .clock(clock).setDocumentStore(store).setAsyncDelay(0).build();
        ns.setMaxBackOffMillis(0); // do not retry commits

        // create initial node structure
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo");
        builder.child("bar");
        builder.child("baz");
        merge(ns, builder);

        store.failCommitOnce.set(true);
        // add a child to each of the nodes.
        // the commit root will be on the root document
        Future<CommitFailedException> f = executorService.submit(() -> {
            NodeBuilder nb = ns.getRoot().builder();
            nb.child("foo").child("n");
            nb.child("bar").child("n");
            nb.child("baz").child("n");
            try {
                merge(ns, nb);
                fail("must throw CommitFailedException");
                return null;
            } catch (CommitFailedException e) {
                // expected
                return e;
            }
        });

        store.commitFailed.await();
        builder = ns.getRoot().builder();
        builder.child("test");
        merge(ns, builder);
        long t1 = clock.getTime();

        assertNotNull(f.get());
        long t2 = clock.getTime();

        assertFalse(store.failCommitOnce.get());
        assertThat(t2 - t1,
                greaterThanOrEqualTo(1000L));
    }

    @Test(expected = DocumentStoreException.class)
    public void rollbackFailed() {
        Rollback.FAILED.perform(new MemoryDocumentStore());
    }

    @Test
    public void rollbackNone() {
        Rollback.NONE.perform(new MemoryDocumentStore());
    }

    private class TestStore extends MemoryDocumentStore {

        final AtomicBoolean failCommitOnce = new AtomicBoolean();

        final CountDownLatch commitFailed = new CountDownLatch(1);

        @Override
        public <T extends Document> T findAndUpdate(Collection<T> collection,
                                                    UpdateOp update) {
            if (collection == Collection.NODES
                    && isFinalCommitRootUpdate(update)
                    && failCommitOnce.compareAndSet(true, false)) {
                commitFailed.countDown();
                throw new DocumentStoreException("commit failed");
            }
            return super.findAndUpdate(collection, update);
        }

        @Override
        public <T extends Document> List<T> createOrUpdate(Collection<T> collection,
                                                           List<UpdateOp> updateOps) {
            for (UpdateOp op : updateOps) {
                if (isRollbackUpdate(op)) {
                    try {
                        // simulate a rollback operation that takes one second
                        Thread.sleep(10);
                        clock.waitUntil(clock.getTime() + 1000);
                        break;
                    } catch (InterruptedException e) {
                        throw new DocumentStoreException(e);
                    }
                }
            }
            return super.createOrUpdate(collection, updateOps);
        }

        private boolean isRollbackUpdate(UpdateOp update) {
            for (Map.Entry<Key, Operation> entry : update.getChanges().entrySet()) {
                if (entry.getValue().type == Operation.Type.REMOVE_MAP_ENTRY
                        && !entry.getKey().getName().equals(NodeDocument.COMMIT_ROOT)) {
                    return true;
                }
            }
            return false;
        }
    }
}
