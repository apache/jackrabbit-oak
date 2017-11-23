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

import org.apache.jackrabbit.oak.json.JsopDiff;
import org.apache.jackrabbit.oak.plugins.document.util.TimingDocumentStoreWrapper;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.MODIFIED_IN_SECS_RESOLUTION;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getIdFromPath;
import static org.junit.Assert.assertEquals;

/**
 * Tests DocumentNodeStore on various DocumentStore back-ends.
 */
public class DocumentNodeStoreIT extends AbstractDocumentStoreTest {


    public DocumentNodeStoreIT(DocumentStoreFixture dsf) {
        super(dsf);
    }

    @After
    public void tearDown() {
        Revision.resetClockToDefault();
    }


    @Test
    public void modifiedResetWithDiff() throws Exception {
        Clock clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        Revision.setClock(clock);
        DocumentStore docStore = new TimingDocumentStoreWrapper(ds) {
            @Override
            public void dispose() {
                // do not dispose yet
            }
        };
        DocumentNodeStore ns1 = new DocumentMK.Builder()
                .setDocumentStore(docStore).setClusterId(1)
                .setAsyncDelay(0).clock(clock)
                        // use a no-op diff cache to simulate a cache miss
                        // when the diff is made later in the test
                .setDiffCache(AmnesiaDiffCache.INSTANCE)
                .getNodeStore();
        NodeBuilder builder1 = ns1.getRoot().builder();
        builder1.child("node");
        removeMe.add(getIdFromPath("/node"));
        for (int i = 0; i < DocumentMK.MANY_CHILDREN_THRESHOLD; i++) {
            builder1.child("node-" + i);
            removeMe.add(getIdFromPath("/node/node-" + i));
        }
        ns1.merge(builder1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        // make sure commit is visible to other node store instance
        ns1.runBackgroundOperations();

        DocumentNodeStore ns2 = new DocumentMK.Builder()
                .setDocumentStore(docStore).setClusterId(2)
                .setAsyncDelay(0).clock(clock).getNodeStore();

        NodeBuilder builder2 = ns2.getRoot().builder();
        builder2.child("node").child("child-a");
        removeMe.add(getIdFromPath("/node/child-a"));
        ns2.merge(builder2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // wait at least _modified resolution. in reality the wait may
        // not be necessary. e.g. when the clock passes the resolution boundary
        // exactly at this time
        clock.waitUntil(System.currentTimeMillis() +
                SECONDS.toMillis(MODIFIED_IN_SECS_RESOLUTION + 1));

        builder1 = ns1.getRoot().builder();
        builder1.child("node").child("child-b");
        removeMe.add(getIdFromPath("/node/child-b"));
        ns1.merge(builder1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        // remember root for diff
        DocumentNodeState root1 = ns1.getRoot();

        builder1 = root1.builder();
        builder1.child("node").child("child-c");
        removeMe.add(getIdFromPath("/node/child-c"));
        ns1.merge(builder1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        // remember root for diff
        DocumentNodeState root2 = ns1.getRoot();

        ns1.runBackgroundOperations();
        ns2.runBackgroundOperations();

        JsopDiff diff = new JsopDiff("", 0);
        ns1.compare(root2, root1, diff);
        // must report /node as changed
        assertEquals("^\"node\":{}", diff.toString());

        ns1.dispose();
        ns2.dispose();
    }
}
