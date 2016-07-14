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

import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.Collection.JOURNAL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class JournalGCTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    @Test
    public void gcWithCheckpoint() throws Exception {
        Clock c = new Clock.Virtual();
        c.waitUntil(System.currentTimeMillis());
        DocumentNodeStore ns = builderProvider.newBuilder()
                .clock(c).setAsyncDelay(0).getNodeStore();

        String cp = ns.checkpoint(TimeUnit.DAYS.toMillis(1));
        // perform some change
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo");
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        Revision head = ns.getHeadRevision().getRevision(ns.getClusterId());
        assertNotNull(head);

        // trigger creation of journal entry
        ns.runBackgroundOperations();

        JournalEntry entry = ns.getDocumentStore().find(JOURNAL, JournalEntry.asId(head));
        assertNotNull(entry);

        // wait two hours
        c.waitUntil(c.getTime() + TimeUnit.HOURS.toMillis(2));

        // instruct journal collector to remove entries older than one hour
        ns.getJournalGarbageCollector().gc(1, 10, TimeUnit.HOURS);

        // must not remove existing entry, because checkpoint is still valid
        entry = ns.getDocumentStore().find(JOURNAL, JournalEntry.asId(head));
        assertNotNull(entry);

        ns.release(cp);

        ns.getJournalGarbageCollector().gc(1, 10, TimeUnit.HOURS);
        // now journal GC can remove the entry
        entry = ns.getDocumentStore().find(JOURNAL, JournalEntry.asId(head));
        assertNull(entry);
    }

    @Test
    public void getTailRevision() throws Exception {
        Clock c = new Clock.Virtual();
        c.waitUntil(System.currentTimeMillis());
        DocumentNodeStore ns = builderProvider.newBuilder()
                .clock(c).setAsyncDelay(0).getNodeStore();

        JournalGarbageCollector jgc = ns.getJournalGarbageCollector();
        assertEquals(new Revision(0, 0, ns.getClusterId()), jgc.getTailRevision());

        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo");
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        ns.runBackgroundOperations();

        assertEquals(0, jgc.gc(1, 10, TimeUnit.HOURS));

        // current time, but without the increment done by getTime()
        long now = c.getTime() - 1;
        Revision tail = new Revision(now - TimeUnit.HOURS.toMillis(1), 0, ns.getClusterId());

        c.waitUntil(c.getTime() + TimeUnit.MINUTES.toMillis(1));
        assertEquals(tail, jgc.getTailRevision());

        c.waitUntil(c.getTime() + TimeUnit.HOURS.toMillis(1));

        // must collect all journal entries. the first created when
        // DocumentNodeStore was initialized and the second created
        // by the background update
        assertEquals(2, jgc.gc(1, 10, TimeUnit.HOURS));

        // current time, but without the increment done by getTime()
        now = c.getTime() - 1;
        tail = new Revision(now - TimeUnit.HOURS.toMillis(1), 0, ns.getClusterId());
        assertEquals(tail, jgc.getTailRevision());
    }
}
