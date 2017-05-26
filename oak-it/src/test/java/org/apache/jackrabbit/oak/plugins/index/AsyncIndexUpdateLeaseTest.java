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
package org.apache.jackrabbit.oak.plugins.index;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.createIndexDefinition;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.jackrabbit.oak.OakBaseTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate.AsyncIndexStats;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate.AsyncUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class AsyncIndexUpdateLeaseTest extends OakBaseTest {

    private final String name = "async";
    private IndexEditorProvider provider;

    private final AtomicBoolean executed = new AtomicBoolean(false);

    public AsyncIndexUpdateLeaseTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Before
    public void setup() throws Exception {
        provider = new PropertyIndexEditorProvider();
        NodeBuilder builder = store.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, name);
        builder.child("testRoot").setProperty("foo", "abc");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        executed.set(false);
    }

    @After
    public void cleanup() throws Exception {
        assertTrue("Test method was not executed", executed.get());
        String referenced = getReferenceCp(store, name);
        assertNotNull("Reference checkpoint doesn't exist", referenced);
        assertNotNull(
                "Failed indexer must not clean successful indexer's checkpoint",
                store.retrieve(referenced));
    }

    @Test
    public void testPrePrepare() throws Exception {
        // take care of initial reindex before
        new AsyncIndexUpdate(name, store, provider).run();

        final IndexStatusListener l1 = new IndexStatusListener() {

            @Override
            protected void prePrepare() {
                executed.set(true);
                assertRunOk(new AsyncIndexUpdate(name, store, provider));
            }
        };
        assertRunKo(new SpecialAsyncIndexUpdate(name, store, provider, l1));
    }

    @Test
    public void testPostPrepare() {
        // take care of initial reindex before
        new AsyncIndexUpdate(name, store, provider).run();

        final IndexStatusListener l1 = new IndexStatusListener() {

            @Override
            protected void postPrepare() {
                executed.set(true);
                // lease must prevent this run
                assertRunKo(new AsyncIndexUpdate(name, store, provider));
            }
        };
        assertRunOk(new SpecialAsyncIndexUpdate(name, store, provider, l1));
    }

    @Test
    public void testPreIndexUpdate() throws Exception {
        // take care of initial reindex before
        new AsyncIndexUpdate(name, store, provider).run();

        testContent(store);
        final IndexStatusListener l1 = new IndexStatusListener() {

            @Override
            protected void preIndexUpdate() {
                executed.set(true);
                assertRunKo(new AsyncIndexUpdate(name, store, provider));
            }
        };
        assertRunOk(new SpecialAsyncIndexUpdate(name, store, provider, l1));
    }

    @Test
    public void testPostIndexUpdate() throws Exception {
        // take care of initial reindex before
        new AsyncIndexUpdate(name, store, provider).run();

        testContent(store);
        final IndexStatusListener l1 = new IndexStatusListener() {

            @Override
            protected void postIndexUpdate() {
                executed.set(true);
                assertRunKo(new AsyncIndexUpdate(name, store, provider));
            }
        };
        assertRunOk(new SpecialAsyncIndexUpdate(name, store, provider, l1));
    }

    @Test
    public void testPreClose() throws Exception {
        // take care of initial reindex before
        new AsyncIndexUpdate(name, store, provider).run();

        testContent(store);
        final IndexStatusListener l1 = new IndexStatusListener() {

            @Override
            protected void preClose() {
                executed.set(true);
                assertRunKo(new AsyncIndexUpdate(name, store, provider));
            }
        };
        assertRunOk(new SpecialAsyncIndexUpdate(name, store, provider, l1));
    }

    @Test
    public void testPostPrepareLeaseExpired() throws Exception {
        // take care of initial reindex before
        new AsyncIndexUpdate(name, store, provider).run();
        final long lease = 50;

        final IndexStatusListener l1 = new IndexStatusListener() {

            @Override
            protected void postPrepare() {
                executed.set(true);
                try {
                    TimeUnit.MILLISECONDS.sleep(lease * 3);
                } catch (InterruptedException e) {
                    //
                }
                assertRunOk(new AsyncIndexUpdate(name, store, provider));
            }
        };
        assertRunKo(new SpecialAsyncIndexUpdate(name, store, provider, l1)
                .setLeaseTimeOut(lease));
    }

    @Test
    public void testPreIndexUpdateLeaseExpired() throws Exception {
        // take care of initial reindex before
        new AsyncIndexUpdate(name, store, provider).run();

        // add extra indexed content
        testContent(store);

        final long lease = 50;
        final IndexStatusListener l1 = new IndexStatusListener() {

            @Override
            protected void preIndexUpdate() {
                executed.set(true);
                try {
                    TimeUnit.MILLISECONDS.sleep(lease * 3);
                } catch (InterruptedException e) {
                    //
                }
                assertRunOk(new AsyncIndexUpdate(name, store, provider));
            }
        };
        assertRunKo(new SpecialAsyncIndexUpdate(name, store, provider, l1)
                .setLeaseTimeOut(lease));
    }

    @Test
    public void testPostIndexUpdateLeaseExpired() throws Exception {
        // take care of initial reindex before
        new AsyncIndexUpdate(name, store, provider).run();

        // add extra indexed content
        testContent(store);

        final long lease = 50;
        final IndexStatusListener l1 = new IndexStatusListener() {

            @Override
            protected void postIndexUpdate() {
                executed.set(true);
                try {
                    TimeUnit.MILLISECONDS.sleep(lease * 3);
                } catch (InterruptedException e) {
                    //
                }
                assertRunOk(new AsyncIndexUpdate(name, store, provider));
            }
        };
        assertRunKo(new SpecialAsyncIndexUpdate(name, store, provider, l1)
                .setLeaseTimeOut(lease));
    }

    @Test
    public void testPrePrepareRexindex() throws Exception {
        final IndexStatusListener l1 = new IndexStatusListener() {

            @Override
            protected void prePrepare() {
                executed.set(true);
                assertRunOk(new AsyncIndexUpdate(name, store, provider));
            }
        };
        assertRunOk(new SpecialAsyncIndexUpdate(name, store, provider, l1));
    }

    @Test
    public void testPostPrepareReindex() {
        final IndexStatusListener l1 = new IndexStatusListener() {

            @Override
            protected void postPrepare() {
                executed.set(true);
                // lease must prevent this run
                assertRunKo(new AsyncIndexUpdate(name, store, provider));
            }
        };
        assertRunOk(new SpecialAsyncIndexUpdate(name, store, provider, l1));
    }

    @Test
    public void testPreIndexUpdateReindex() throws Exception {
        testContent(store);
        final IndexStatusListener l1 = new IndexStatusListener() {

            @Override
            protected void preIndexUpdate() {
                executed.set(true);
                assertRunKo(new AsyncIndexUpdate(name, store, provider));
            }
        };
        assertRunOk(new SpecialAsyncIndexUpdate(name, store, provider, l1));
    }

    @Test
    public void testPostIndexUpdateReindex() throws Exception {
        testContent(store);
        final IndexStatusListener l1 = new IndexStatusListener() {

            @Override
            protected void postIndexUpdate() {
                executed.set(true);
                assertRunKo(new AsyncIndexUpdate(name, store, provider));
            }
        };
        assertRunOk(new SpecialAsyncIndexUpdate(name, store, provider, l1));
    }

    @Test
    public void testPostPrepareReindexLeaseExpired() throws Exception {
        final long lease = 50;
        final IndexStatusListener l1 = new IndexStatusListener() {

            @Override
            protected void postPrepare() {
                executed.set(true);
                try {
                    TimeUnit.MILLISECONDS.sleep(lease * 3);
                } catch (InterruptedException e) {
                    //
                }
                assertRunOk(new AsyncIndexUpdate(name, store, provider));
            }
        };
        assertRunKo(new SpecialAsyncIndexUpdate(name, store, provider, l1)
                .setLeaseTimeOut(lease));
    }

    @Test
    public void testPreIndexUpdateReindexLeaseExpired() throws Exception {
        final long lease = 50;
        final IndexStatusListener l1 = new IndexStatusListener() {

            @Override
            protected void preIndexUpdate() {
                executed.set(true);
                try {
                    TimeUnit.MILLISECONDS.sleep(lease * 3);
                } catch (InterruptedException e) {
                    //
                }
                assertRunOk(new AsyncIndexUpdate(name, store, provider));
            }
        };
        assertRunKo(new SpecialAsyncIndexUpdate(name, store, provider, l1)
                .setLeaseTimeOut(lease));
    }

    @Test
    public void testPostIndexUpdateReindexLeaseExpired() throws Exception {
        final long lease = 50;
        final IndexStatusListener l1 = new IndexStatusListener() {

            @Override
            protected void postIndexUpdate() {
                executed.set(true);
                try {
                    TimeUnit.MILLISECONDS.sleep(lease * 3);
                } catch (InterruptedException e) {
                    //
                }
                assertRunOk(new AsyncIndexUpdate(name, store, provider));
            }
        };
        assertRunKo(new SpecialAsyncIndexUpdate(name, store, provider, l1)
                .setLeaseTimeOut(lease));
    }

    // -------------------------------------------------------------------

    private static String getReferenceCp(NodeStore store, String name) {
        return store.getRoot().getChildNode(AsyncIndexUpdate.ASYNC)
                .getString(name);
    }

    private void assertRunOk(AsyncIndexUpdate a) {
        assertRun(a, false);
    }

    private void assertRunKo(AsyncIndexUpdate a) {
        assertRun(a, true);
        assertConcurrentUpdate(a.getIndexStats());
    }

    private void assertRun(AsyncIndexUpdate a, boolean failing) {
        a.run();
        assertEquals("Unexpected failiure flag", failing, a.isFailing());
    }

    private void assertConcurrentUpdate(AsyncIndexStats stats) {
        assertTrue("Error must be of type 'Concurrent update'", stats
                .getLatestError().contains("Concurrent update detected"));
    }

    private static void testContent(NodeStore store) throws Exception {
        NodeBuilder builder = store.getRoot().builder();
        builder.child("testRoot").setProperty("foo",
                "abc " + System.currentTimeMillis());
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private static class SpecialAsyncIndexUpdate extends AsyncIndexUpdate {

        private final IndexStatusListener listener;

        public SpecialAsyncIndexUpdate(String name, NodeStore store,
                IndexEditorProvider provider, IndexStatusListener listener) {
            super(name, store, provider);
            this.listener = listener;
        }

        @Override
        public synchronized void run() {
            super.run();
        }

        @Override
        protected AsyncUpdateCallback newAsyncUpdateCallback(NodeStore store,
                                                             String name, long leaseTimeOut, String checkpoint,
                                                             AsyncIndexStats indexStats,
                                                             AtomicBoolean stopFlag) {
            return new SpecialAsyncUpdateCallback(store, name, leaseTimeOut,
                    checkpoint, indexStats, stopFlag, listener);
        }
    }

    private static class SpecialAsyncUpdateCallback extends AsyncUpdateCallback {

        private IndexStatusListener listener;

        public SpecialAsyncUpdateCallback(NodeStore store, String name,
                                          long leaseTimeOut, String checkpoint,
                                          AsyncIndexStats indexStats, AtomicBoolean stopFlag, IndexStatusListener listener) {
            super(store, name, leaseTimeOut, checkpoint, indexStats, stopFlag);
            this.listener = listener;
        }

        @Override
        protected void prepare(String afterCheckpoint) throws CommitFailedException {
            listener.prePrepare();
            super.prepare(afterCheckpoint);
            listener.postPrepare();
        }

        @Override
        public void indexUpdate() throws CommitFailedException {
            listener.preIndexUpdate();
            super.indexUpdate();
            listener.postIndexUpdate();
        }

        @Override
        void close() throws CommitFailedException {
            listener.preClose();
            super.close();
            listener.postClose();
        }

    }

    private abstract static class IndexStatusListener {

        protected void prePrepare() {
        }

        protected void postPrepare() {
        }

        protected void preIndexUpdate() {
        }

        protected void postIndexUpdate() {
        }

        protected void preClose() {
        }

        protected void postClose() {
        }
    }

}
