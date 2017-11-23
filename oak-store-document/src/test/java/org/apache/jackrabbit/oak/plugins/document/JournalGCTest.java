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

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterables;

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.document.Collection.JOURNAL;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JournalGCTest {

    private static final Logger LOG = LoggerFactory.getLogger(JournalGCTest.class);

    private final ThreadLocal<Boolean> shouldWait = new ThreadLocal<Boolean>();

    @AfterClass
    public static void resetClock() {
        Revision.resetClockToDefault();
    }

    @Before
    public void setup() {
        Revision.resetClockToDefault();
        shouldWait.remove();
    }
    
    @After
    public void tearDown() {
        shouldWait.remove();
    }
    
    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    @Test
    public void gcWithCheckpoint() throws Exception {
        Clock c = new Clock.Virtual();
        c.waitUntil(System.currentTimeMillis());
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setJournalGCMaxAge(TimeUnit.HOURS.toMillis(1))
                .clock(c).setAsyncDelay(0).getNodeStore();

        // perform some change
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo");
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        Revision head = ns.getHeadRevision().getRevision(ns.getClusterId());
        assertNotNull(head);

        // trigger creation of journal entry
        ns.runBackgroundOperations();
        String cp = ns.checkpoint(TimeUnit.DAYS.toMillis(1));

        JournalEntry entry = ns.getDocumentStore().find(JOURNAL, JournalEntry.asId(head));
        assertNotNull(entry);

        // wait two hours
        c.waitUntil(c.getTime() + TimeUnit.HOURS.toMillis(2));

        // instruct journal collector to remove entries older than one hour
        ns.getJournalGarbageCollector().gc();

        // must not remove existing entry, because checkpoint is still valid
        entry = ns.getDocumentStore().find(JOURNAL, JournalEntry.asId(head));
        assertNotNull(entry);

        ns.release(cp);

        ns.getJournalGarbageCollector().gc();
        // now journal GC can remove the entry
        entry = ns.getDocumentStore().find(JOURNAL, JournalEntry.asId(head));
        assertNull(entry);
    }

    // OAK-5602
    @Test
    public void gcWithCheckpoint2() throws Exception {
        Clock c = new Clock.Virtual();
        c.waitUntil(System.currentTimeMillis());
        Revision.setClock(c);
        MemoryDocumentStore docStore = new MemoryDocumentStore();
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setDocumentStore(docStore).setUpdateLimit(100)
                .setJournalGCMaxAge(TimeUnit.HOURS.toMillis(1))
                .clock(c).setAsyncDelay(0).getNodeStore();

        NodeBuilder builder = ns.getRoot().builder();
        NodeBuilder test = builder.child("test");
        String testId = Utils.getIdFromPath("/test");
        for (int i = 0; ; i++) {
            NodeBuilder child = test.child("child-" + i);
            for (int j = 0; j < 10; j++) {
                child.setProperty("p-" + j, "value");
            }
            if (docStore.find(NODES, testId) != null) {
                // branch was created
                break;
            }
        }
        // simulate a long running commit taking 20 minutes
        c.waitUntil(c.getTime() + TimeUnit.MINUTES.toMillis(20));
        merge(ns, builder);
        ns.runBackgroundOperations();

        Revision head = ns.getHeadRevision().getRevision(ns.getClusterId());
        assertNotNull(head);
        ns.checkpoint(TimeUnit.DAYS.toMillis(1));

        JournalEntry entry = ns.getDocumentStore().find(JOURNAL, JournalEntry.asId(head));
        assertNotNull(entry);

        // wait two hours
        c.waitUntil(c.getTime() + TimeUnit.HOURS.toMillis(2));

        // instruct journal collector to remove entries older than one hour
        ns.getJournalGarbageCollector().gc();

        // must not remove existing entry, because checkpoint is still valid
        entry = ns.getDocumentStore().find(JOURNAL, JournalEntry.asId(head));
        assertNotNull(entry);
        // referenced branch commits must also be available
        assertThat(Iterables.size(entry.getBranchCommits()), greaterThan(0));
    }

    @Test
    public void getTailRevision() throws Exception {
        Clock c = new Clock.Virtual();
        c.waitUntil(System.currentTimeMillis());
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setJournalGCMaxAge(TimeUnit.HOURS.toMillis(1))
                .clock(c).setAsyncDelay(0).getNodeStore();

        JournalGarbageCollector jgc = ns.getJournalGarbageCollector();
        assertEquals(new Revision(0, 0, ns.getClusterId()), jgc.getTailRevision());

        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo");
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        ns.runBackgroundOperations();

        assertEquals(0, jgc.gc());

        // current time, but without the increment done by getTime()
        long now = c.getTime() - 1;
        Revision tail = new Revision(now - TimeUnit.HOURS.toMillis(1), 0, ns.getClusterId());

        c.waitUntil(c.getTime() + TimeUnit.MINUTES.toMillis(1));
        assertEquals(tail, jgc.getTailRevision());

        c.waitUntil(c.getTime() + TimeUnit.HOURS.toMillis(1));

        // must collect the journal entry created by the background update
        assertEquals(1, jgc.gc());

        // current time, but without the increment done by getTime()
        now = c.getTime() - 1;
        tail = new Revision(now - TimeUnit.HOURS.toMillis(1), 0, ns.getClusterId());
        assertEquals(tail, jgc.getTailRevision());
    }

    /**
     * reproducing OAK-5601:
     * <ul>
     *  <li>have two documentMk's, one to make changes, one does only read</li>
     *  <li>make a commit, let 1.2 seconds pass, run gc, then read it from the other documentMk</li>
     *  <li>the gc (1sec timeout) will have cleaned up that 1.2sec old journal entry, resulting in
     *      a missing journal entry exception when reading from the 2nd documentMk</li>
     * </ul>
     * What the test has to ensure is that the JournalEntry does the query, then blocks that
     * thread to let the GC happen, then continues on with find(). This results in those
     * revisions that the JournalEntry got back from the query to be removed and
     * thus end up missing by later on in addTo.
     */
    @Test
    public void gcCausingMissingJournalEntries() throws Exception {
        // cluster setup
        final Semaphore enteringFind = new Semaphore(0);
        final Semaphore continuingFind = new Semaphore(100);
        DocumentStore sharedDocStore = new MemoryDocumentStore() {
            @Override
            public <T extends Document> T find(Collection<T> collection, String key) {
                if (collection == JOURNAL && (shouldWait.get() == null || shouldWait.get())) {
                    LOG.info("find(JOURNAL,..): entered... releasing enteringFind semaphore");
                    enteringFind.release();
                    try {
                        LOG.info("find(JOURNAL,..): waiting for OK to continue");
                        if (!continuingFind.tryAcquire(5, TimeUnit.SECONDS)) {
                            fail("could not continue within 5 sec");
                        }
                        LOG.info("find(JOURNAL,..): continuing");
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                }
                return super.find(collection, key);
            }
        };
        final DocumentNodeStore writingNs = builderProvider.newBuilder()
                .setDocumentStore(sharedDocStore)
                .setClusterId(1)
                .setAsyncDelay(0).getNodeStore();
        DocumentNodeStore readingNs = builderProvider.newBuilder()
                .setDocumentStore(sharedDocStore)
                .setClusterId(2)
                .setAsyncDelay(0).getNodeStore();
        
        // 'proper cluster sync': do it a bit too many times
        readingNs.runBackgroundOperations();
        writingNs.runBackgroundOperations();
        readingNs.runBackgroundOperations();
        writingNs.runBackgroundOperations();
        
        // perform some change in writingNs - not yet seen by readingNs
        NodeBuilder builder = writingNs.getRoot().builder();
        NodeBuilder foo = builder.child("foo");
        // cause a branch commit
        for(int i=0; i<DocumentMK.UPDATE_LIMIT + 1; i++) {
            foo.setProperty(String.valueOf(i), "foobar");
        }
        writingNs.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        final Revision head = writingNs.getHeadRevision().getRevision(writingNs.getClusterId());
        assertNotNull(head);

        // trigger creation of journal entry - still not yet seen by readingNs
        writingNs.runBackgroundOperations();
        JournalEntry entry = writingNs.getDocumentStore().find(JOURNAL, JournalEntry.asId(head));
        assertNotNull(entry);

        // wait slightly more than 1 sec - readingNs does nothing during this time
        Thread.sleep(1200);

        // clear up the semaphore
        enteringFind.drainPermits();
        continuingFind.drainPermits();
        
        final StringBuffer errorMsg = new StringBuffer();
        Runnable r = new Runnable() {
            @Override
            public void run() {
                // wait for find(JOURNAL,..) to be entered
                LOG.info("waiting for find(JOURNAL,... to be called...");
                try {
                    if (!enteringFind.tryAcquire(5, TimeUnit.SECONDS)) {
                        errorMsg.append("find(JOURNAL,..) did not get called within 5sec");
                        return;
                    }
                } catch (InterruptedException e) {
                    errorMsg.append("Got interrupted: "+e);
                    return;
                }
                LOG.info("find(JOURNAL,..) got called, running GC.");
                
                // avoid find to block in this thread - via a ThreadLocal
                shouldWait.set(false);
                
                // instruct journal GC to remove entries older than one hour - readingNs hasn't seen it
                new JournalGarbageCollector(writingNs, TimeUnit.SECONDS.toMillis(1)).gc();

                // entry should be removed
                JournalEntry entry = writingNs.getDocumentStore().find(JOURNAL, JournalEntry.asId(head));
                assertNull(entry);
                
                // now release the waiting find(JOURNAL,..) thread
                continuingFind.release(100);
            }
        };
        Thread th = new Thread(r);
        th.start();
        
        // verify that readingNs doesn't have /foo yet
        assertFalse(readingNs.getRoot().hasChildNode("foo"));
        
        // now run background ops on readingNs - it should be able to see 'foo'
        for(int i=0; i<5; i++) {
            readingNs.runBackgroundOperations();
        }
        assertTrue(readingNs.getRoot().hasChildNode("foo"));
    }

}
