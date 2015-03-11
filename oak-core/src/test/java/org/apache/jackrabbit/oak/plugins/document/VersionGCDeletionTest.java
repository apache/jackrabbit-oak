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

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.HOURS;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class VersionGCDeletionTest {
    private Clock clock;

    private DocumentNodeStore store;

    @Before
    public void setUp() throws InterruptedException {
        clock = new Clock.Virtual();
    }

    @After
    public void tearDown() throws Exception {
        if (store != null) {
            store.dispose();
        }
        Revision.resetClockToDefault();
    }

    @Test
    public void deleteParentLast() throws Exception{
        TestDocumentStore ts = new TestDocumentStore();
        store = new DocumentMK.Builder()
                .clock(clock)
                .setDocumentStore(ts)
                .setAsyncDelay(0)
                .getNodeStore();

        //Baseline the clock
        clock.waitUntil(Revision.getCurrentTimestamp());

        NodeBuilder b1 = store.getRoot().builder();
        b1.child("x").child("y");
        store.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        long maxAge = 1; //hours
        long delta = TimeUnit.MINUTES.toMillis(10);

        //Remove x/y
        NodeBuilder b2 = store.getRoot().builder();
        b2.child("x").remove();
        store.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        store.runBackgroundOperations();

        //3. Check that deleted doc does get collected post maxAge
        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);
        VersionGarbageCollector gc = store.getVersionGarbageCollector();

        //4. Ensure that while GC is being run /x gets removed but failure occurs
        //for /x/y. At least attempt that! Once issue is fixed the list would be
        //sorted again by VersionGC and then /x would always come after /x/y
        try {
            ts.throwException = true;
            gc.gc(maxAge * 2, HOURS);
            fail("Exception should be thrown");
        } catch (AssertionError ignore) {

        }

        ts.throwException = false;
        gc.gc(maxAge * 2, HOURS);
        assertNull(ts.find(Collection.NODES, "2:/x/y"));
        assertNull(ts.find(Collection.NODES, "1:/x"));
    }

    private static class TestDocumentStore extends MemoryDocumentStore {
        boolean throwException;
        @Override
        public <T extends Document> void remove(Collection<T> collection, String path) {
            if (throwException && "2:/x/y".equals(path)){
                throw new AssertionError();
            }
            super.remove(collection, path);
        }

        @SuppressWarnings("unchecked")
        @Nonnull
        @Override
        public <T extends Document> List<T> query(Collection<T> collection, String fromKey,
                                                  String toKey, String indexedProperty, long startValue, int limit) {
            List<T> result = super.query(collection, fromKey, toKey, indexedProperty, startValue, limit);

            //Ensure that /x comes before /x/y
            if (NodeDocument.DELETED_ONCE.equals(indexedProperty)){
                Collections.sort((List<NodeDocument>)result, new NodeDocComparator());
            }
            return result;
        }
    }

    /**
     * Ensures that NodeDocument with path  /x/y /x/y/z /x get sorted to
     * /x /x/y /x/y/z
     */
    private static class NodeDocComparator implements Comparator<NodeDocument> {
        private static Comparator<String> reverse = Collections.reverseOrder(PathComparator.INSTANCE);

        @Override
        public int compare(NodeDocument o1, NodeDocument o2) {
            return reverse.compare(o1.getPath(), o2.getPath());
        }
    }
}
