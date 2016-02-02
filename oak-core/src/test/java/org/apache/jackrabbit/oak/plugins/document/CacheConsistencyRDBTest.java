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

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.sql.DataSource;

import org.apache.jackrabbit.oak.plugins.document.DocumentMK.Builder;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDataSourceFactory;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBOptions;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBRow;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;

public class CacheConsistencyRDBTest extends AbstractRDBConnectionTest {

    private TestStore store;

    @Before
    @Override
    public void setUpConnection() throws Exception {
        dataSource = RDBDataSourceFactory.forJdbcUrl(URL, USERNAME, PASSWD);
        DocumentMK.Builder builder = new DocumentMK.Builder().clock(getTestClock()).setAsyncDelay(0);
        RDBOptions opt = new RDBOptions().tablePrefix("T" + Long.toHexString(System.currentTimeMillis())).dropTablesOnClose(true);
        store = new TestStore(dataSource, builder, opt);
        mk = builder.setDocumentStore(store).setLeaseCheck(false).open();
    }

    @Test
    public void cacheConsistency() throws Exception {
        mk.commit("/", "+\"node\":{}", null, null);
        // add a child node. this will require an update
        // of _lastRev on /node
        mk.commit("/node", "+\"child\":{}", null, null);
        // make sure the document is not cached
        store.invalidateNodeDocument(Utils.getIdFromPath("/node"));

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                store.query(NODES, Utils.getKeyLowerLimit("/"), Utils.getKeyUpperLimit("/"), 10);
            }
        }, "query");
        // block thread when it tries to convert db objects
        store.semaphores.put(t, new Semaphore(0));
        t.start();

        while (!store.semaphores.get(t).hasQueuedThreads()) {
            Thread.sleep(10);
        }

        final Semaphore done = new Semaphore(0);
        new Thread(new Runnable() {
            @Override
            public void run() {
                // trigger write back of _lastRevs
                mk.runBackgroundOperations();
                done.release();
            }
        }, "mkbg").start();

        // wait at most one second for background thread
        done.tryAcquire(1, TimeUnit.SECONDS);
        store.invalidateNodeDocument(Utils.getIdFromPath("/node"));

        // release thread
        store.semaphores.get(t).release();
        t.join();

        NodeState root = mk.getNodeStore().getRoot();
        assertTrue(root.getChildNode("node").getChildNode("child").exists());
    }

    private static final class TestStore extends RDBDocumentStore {

        final Map<Thread, Semaphore> semaphores = Maps.newConcurrentMap();

        public TestStore(DataSource dataSource, Builder builder, RDBOptions opt) {
            super(dataSource, builder, opt);
        }

        @Override
        protected <T extends Document> T convertFromDBObject(@Nonnull Collection<T> collection, @Nullable RDBRow row) {
            Semaphore s = semaphores.get(Thread.currentThread());
            if (s != null) {
                s.acquireUninterruptibly();
            }
            try {
                return super.convertFromDBObject(collection, row);
            } finally {
                if (s != null) {
                    s.release();
                }
            }
        }

        public void invalidateNodeDocument(String key) {
            getNodeDocumentCache().invalidate(key);
        }
    }
}
