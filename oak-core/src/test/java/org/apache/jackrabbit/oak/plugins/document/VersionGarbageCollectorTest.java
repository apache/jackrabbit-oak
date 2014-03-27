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

import java.io.IOException;
import java.util.*;
import java.util.Collection;

import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class VersionGarbageCollectorTest {

    private DocumentStoreFixture fixture;

    private Clock clock;

    private DocumentNodeStore store;

    private VersionGarbageCollector gc;

    public VersionGarbageCollectorTest(DocumentStoreFixture fixture) {
        this.fixture = fixture;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> fixtures() throws IOException {
        List<Object[]> fixtures = Lists.newArrayList();
        fixtures.add(new Object[] {new DocumentStoreFixture.MemoryFixture()});

        DocumentStoreFixture mongo = new DocumentStoreFixture.MongoFixture();
        if(mongo.isAvailable()){
           fixtures.add(new Object[] {mongo});
        }
        return fixtures;
    }

    @Before
    public void setUp() throws InterruptedException {
        clock = new Clock.Virtual();
        store = new DocumentMK.Builder()
                .clock(clock)
                .setDocumentStore(fixture.getDocumentStore())
                .getNodeStore();
        gc = store.getVersionGarbageCollector();

        //Baseline the clock
        clock.waitUntil(Revision.getCurrentTimestamp());
    }

    @After
    public void tearDown() throws Exception {
        fixture.dispose();
    }

    @Test
    public void gcIgnoredForCheckpoint() throws Exception {
        long expiryTime = 100, maxAge = 20;

        Revision cp = Revision.fromString(store.checkpoint(expiryTime));
        gc.setMaxRevisionAge(maxAge);

        //Fast forward time to future but before expiry of checkpoint
        clock.waitUntil(cp.getTimestamp() + expiryTime - maxAge);
        VersionGCStats stats = gc.gc();
        assertTrue(stats.ignoredGCDueToCheckPoint);

        //Fast forward time to future such that checkpoint get expired
        clock.waitUntil(clock.getTime() + expiryTime + 1);
        stats = gc.gc();
        assertFalse("GC should be performed", stats.ignoredGCDueToCheckPoint);
    }

    @Test
    public void testGCDeletedDocument() throws Exception{
        //1. Create nodes
        NodeBuilder b1 = store.getRoot().builder();
        b1.child("x").child("y");
        b1.child("z");
        store.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        int maxAge = 10000, delta = maxAge;
        gc.setMaxRevisionAge(maxAge);
        //Go past GC age and check no GC done as nothing deleted
        clock.waitUntil(Revision.getCurrentTimestamp() + maxAge);
        VersionGCStats stats = gc.gc();
        assertEquals(0, stats.deletedDocCount);

        //Remove x/y
        NodeBuilder b2 = store.getRoot().builder();
        b2.child("x").child("y").remove();
        store.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        clock.waitUntil(Revision.getCurrentTimestamp() + maxAge + delta);

        stats = gc.gc();
        assertEquals(1, stats.deletedDocCount);

        //TODO Add test scenario for deletion along with previous docs
    }


    @Test
    public void sample() throws Exception{

    }
}