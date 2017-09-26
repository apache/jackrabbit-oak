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

import java.io.IOException;

import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.concurrent.TimeUnit.HOURS;
import static org.junit.Assert.assertTrue;

/**
 * OAK-3929
 */
@RunWith(Parameterized.class)
public class RevisionGCTest {

    private DocumentStoreFixture fixture;

    private Clock clock;

    private DocumentNodeStore store;

    private VersionGarbageCollector gc;

    public RevisionGCTest(DocumentStoreFixture fixture) {
        this.fixture = fixture;
    }

    @Parameterized.Parameters(name="{0}")
    public static java.util.Collection<Object[]> fixtures() throws IOException {
        return DocumentStoreFixture.getFixtures();
    }

    @Before
    public void setUp() throws InterruptedException {
        clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        Revision.setClock(clock);
        store = new DocumentMK.Builder()
                .clock(clock)
                .setDocumentStore(fixture.createDocumentStore())
                .setAsyncDelay(0)
                .getNodeStore();
        gc = store.getVersionGarbageCollector();
    }

    @After
    public void tearDown() throws Exception {
        if (store != null) {
            store.dispose();
        }
        fixture.dispose();
        Revision.resetClockToDefault();
    }

    @Test
    public void recreateRemovedNodeAfterGC() throws Exception {
        NodeBuilder b = newNodeBuilder();
        b.child("child");
        merge(b);
        b = newNodeBuilder();
        b.child("child").remove();
        merge(b);
        store.runBackgroundOperations();

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hours
        gc.gc(1, HOURS);

        b = newNodeBuilder();
        b.child("child");
        merge(b);

        assertTrue(store.getRoot().hasChildNode("child"));
    }

    private NodeBuilder newNodeBuilder() {
        return store.getRoot().builder();
    }

    private void merge(NodeBuilder builder)
            throws Exception {
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

}
