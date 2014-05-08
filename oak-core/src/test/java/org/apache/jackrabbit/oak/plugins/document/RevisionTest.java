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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.jackrabbit.oak.plugins.document.Revision.RevisionComparator;
import org.junit.Test;

/**
 * Tests the revision class
 */
public class RevisionTest {

    @Test
    public void fromStringToString() {
        for (int i = 0; i < 10000; i++) {
            Revision r = Revision.newRevision(i);
            // System.out.println(r);
            Revision r2 = Revision.fromString(r.toString());
            assertEquals(r.toString(), r2.toString());
            assertEquals(r.hashCode(), r2.hashCode());
            assertTrue(r.equals(r2));
        }
    }

    @Test
    public void difference() throws InterruptedException {
        long t0 = Revision.getCurrentTimestamp();
        Revision r0 = Revision.newRevision(0);
        Revision r1 = Revision.newRevision(0);
        long t1 = Revision.getCurrentTimestamp();
        // the difference must not be more than t1 - t0
        assertTrue(Revision.getTimestampDifference(r1, r0) <= (t1 - t0));
        // busy wait until we have a timestamp different from t1
        long t2;
        do {
            t2 = Revision.getCurrentTimestamp();
        } while (t1 == t2);

        Revision r2 = Revision.newRevision(0);
        assertTrue(Revision.getTimestampDifference(r2, r1) > 0);
    }

    @Test
    public void equalsHashCode() {
        Revision a = Revision.newRevision(0);
        Revision b = Revision.newRevision(0);
        assertTrue(a.equals(a));
        assertFalse(a.equals(b));
        assertFalse(b.equals(a));
        assertFalse(a.hashCode() == b.hashCode());
        Revision a1 = Revision.fromString(a.toString());
        assertTrue(a.equals(a1));
        assertTrue(a1.equals(a));
        Revision a2 = new Revision(a.getTimestamp(), a.getCounter(), a.getClusterId());
        assertTrue(a.equals(a2));
        assertTrue(a2.equals(a));
        assertEquals(a.hashCode(), a1.hashCode());
        assertEquals(a.hashCode(), a2.hashCode());
        Revision x1 = new Revision(a.getTimestamp() + 1, a.getCounter(), a.getClusterId());
        assertFalse(a.equals(x1));
        assertFalse(x1.equals(a));
        assertFalse(a.hashCode() == x1.hashCode());
        Revision x2 = new Revision(a.getTimestamp(), a.getCounter() + 1, a.getClusterId());
        assertFalse(a.equals(x2));
        assertFalse(x2.equals(a));
        assertFalse(a.hashCode() == x2.hashCode());
        Revision x3 = new Revision(a.getTimestamp(), a.getCounter(), a.getClusterId() + 1);
        assertFalse(a.equals(x3));
        assertFalse(x3.equals(a));
        assertFalse(a.hashCode() == x3.hashCode());
    }

    @Test
    public void compare() throws InterruptedException {
        Revision last = Revision.newRevision(0);
        try {
            last.compareRevisionTime(null);
            fail();
        } catch (NullPointerException e) {
            // expected
        }
        for (int i = 0; i < 1000; i++) {
            Revision r = Revision.newRevision(0);
            assertTrue(r.compareRevisionTime(r) == 0);
            assertTrue(r.compareRevisionTime(last) > 0);
            assertTrue(last.compareRevisionTime(r) < 0);
            last = r;
            if (i % 100 == 0) {
                // ensure the timestamp part changes as well
                Thread.sleep(1);
            }
        }
    }

    @Test
    public void revisionComparatorSimple() {
        RevisionComparator comp = new RevisionComparator(0);
        Revision r1 = Revision.newRevision(0);
        Revision r2 = Revision.newRevision(0);
        assertEquals(r1.compareRevisionTime(r2), comp.compare(r1, r2));
        assertEquals(r2.compareRevisionTime(r1), comp.compare(r2, r1));
        assertEquals(r1.compareRevisionTime(r1), comp.compare(r1, r1));
    }

    @Test
    public void revisionComparatorCluster() {

        RevisionComparator comp = new RevisionComparator(0);

        Revision r0c1 = new Revision(0x010, 0, 1);
        Revision r0c2 = new Revision(0x010, 0, 2);

        Revision r1c1 = new Revision(0x110, 0, 1);
        Revision r2c1 = new Revision(0x120, 0, 1);
        Revision r3c1 = new Revision(0x130, 0, 1);
        Revision r1c2 = new Revision(0x100, 0, 2);
        Revision r2c2 = new Revision(0x200, 0, 2);
        Revision r3c2 = new Revision(0x300, 0, 2);

        // first, only timestamps are compared
        assertEquals(1, comp.compare(r1c1, r1c2));
        assertEquals(-1, comp.compare(r2c1, r2c2));
        assertEquals(-1, comp.compare(r3c1, r3c2));

        // now we declare r2+r3 of c1 to be after r2+r3 of c2
        comp.add(r2c1, new Revision(0x20, 0, 0));
        comp.add(r2c2, new Revision(0x10, 0, 0));

        assertEquals(
                "1:\n r120-0-1:r20-0-0\n" +
                "2:\n r200-0-2:r10-0-0\n", comp.toString());

        assertEquals(-1, comp.compare(r0c1, r0c2));

        assertEquals(1, comp.compare(r1c1, r1c2));
        assertEquals(1, comp.compare(r2c1, r2c2));
        // both r3cx are still "in the future"
        assertEquals(-1, comp.compare(r3c1, r3c2));

        // now we declare r3 of c1 to be before r3 of c2
        // (with the same range timestamp,
        // the revision timestamps are compared)
        comp.add(r3c1, new Revision(0x30, 0, 0));
        comp.add(r3c2, new Revision(0x30, 0, 0));

        assertEquals(
                "1:\n r120-0-1:r20-0-0 r130-0-1:r30-0-0\n" +
                "2:\n r200-0-2:r10-0-0 r300-0-2:r30-0-0\n", comp.toString());

        assertEquals(1, comp.compare(r1c1, r1c2));
        assertEquals(1, comp.compare(r2c1, r2c2));
        assertEquals(-1, comp.compare(r3c1, r3c2));
        // reverse
        assertEquals(-1, comp.compare(r1c2, r1c1));
        assertEquals(-1, comp.compare(r2c2, r2c1));
        assertEquals(1, comp.compare(r3c2, r3c1));

        // get rid of old timestamps
        comp.purge(0x10);
        assertEquals(
                "1:\n r120-0-1:r20-0-0 r130-0-1:r30-0-0\n" +
                "2:\n r300-0-2:r30-0-0\n", comp.toString());
        comp.purge(0x20);
        assertEquals(
                "1:\n r130-0-1:r30-0-0\n" +
                "2:\n r300-0-2:r30-0-0\n", comp.toString());

        // update an entry
        comp.add(new Revision(0x301, 1, 2), new Revision(0x30, 0, 0));
        assertEquals(
                "1:\n r130-0-1:r30-0-0\n" +
                "2:\n r301-1-2:r30-0-0\n", comp.toString());

        comp.purge(0x30);
        assertEquals("", comp.toString());

    }

    @Test
    public void clusterCompare() {
        RevisionComparator comp = new RevisionComparator(1);

        // sequence of revisions as added to comparator later
        Revision r1c1 = new Revision(0x10, 0, 1);
        Revision r1c2 = new Revision(0x20, 0, 2);
        Revision r2c1 = new Revision(0x30, 0, 1);
        Revision r2c2 = new Revision(0x40, 0, 2);

        comp.add(r1c1, new Revision(0x10, 0, 0));
        comp.add(r2c1, new Revision(0x20, 0, 0));

        // there's no range for c2, and therefore this
        // revision must be considered to be in the future
        assertTrue(comp.compare(r1c2, r2c1) > 0);

        // add a range for r2r2
        comp.add(r2c2, new Revision(0x30, 0, 0));

        // now there is a range for c2, but the revision is old,
        // so it must be considered to be in the past
        assertTrue(comp.compare(r1c2, r2c1) < 0);
    }

    // OAK-1727
    @Test
    public void clusterCompare2() {
        RevisionComparator comp = new RevisionComparator(1);

        comp.add(Revision.fromString("r3-0-1"), Revision.fromString("r1-1-0"));

        Revision r1 = Revision.fromString("r1-0-2");
        Revision r2 = Revision.fromString("r4-0-2");

        // cluster sync
        Revision c1sync = Revision.fromString("r5-0-1");
        comp.add(c1sync,  Revision.fromString("r2-0-0"));
        Revision c2sync = Revision.fromString("r4-1-2");
        comp.add(c2sync,  Revision.fromString("r2-1-0"));
        Revision c3sync = Revision.fromString("r2-0-3");
        comp.add(c3sync,  Revision.fromString("r2-1-0"));

        assertTrue(comp.compare(r1, r2) < 0);
        assertTrue(comp.compare(r2, c2sync) < 0);
        // same seen-at revision, but clusterId 2 < 3
        assertTrue(comp.compare(c2sync, c3sync) < 0);

        // this means, c3sync must be after r1 and r2
        // because: r1 < r2 < c2sync < c3sync
        assertTrue(comp.compare(r1, c3sync) < 0);
        assertTrue(comp.compare(r2, c3sync) < 0);
    }

    @Test
    public void revisionSeen() {
        RevisionComparator comp = new RevisionComparator(1);

        Revision r0 = new Revision(0x01, 0, 1);
        Revision r1 = new Revision(0x10, 0, 1);
        Revision r2 = new Revision(0x20, 0, 1);
        Revision r21 = new Revision(0x21, 0, 1);
        Revision r3 = new Revision(0x30, 0, 1);
        Revision r4 = new Revision(0x40, 0, 1);
        Revision r5 = new Revision(0x50, 0, 1);

        comp.add(r1, new Revision(0x10, 0, 0));
        comp.add(r2, new Revision(0x20, 0, 0));
        comp.add(r3, new Revision(0x30, 0, 0));
        comp.add(r4, new Revision(0x40, 0, 0));

        // older than first range -> must return null
        assertNull(comp.getRevisionSeen(r0));

        // exact range start matches
        assertEquals(new Revision(0x10, 0, 0), comp.getRevisionSeen(r1));
        assertEquals(new Revision(0x20, 0, 0), comp.getRevisionSeen(r2));
        assertEquals(new Revision(0x30, 0, 0), comp.getRevisionSeen(r3));
        assertEquals(new Revision(0x40, 0, 0), comp.getRevisionSeen(r4));

        // revision newer than most recent range -> NEWEST
        assertEquals(RevisionComparator.NEWEST, comp.getRevisionSeen(r5));

        // within a range -> must return lower bound of next higher range
        assertEquals(new Revision(0x30, 0, 0), comp.getRevisionSeen(r21));
    }

    @Test
    public void uniqueRevision2() throws Exception {
        List<Thread> threads = new ArrayList<Thread>();
        final AtomicBoolean stop = new AtomicBoolean();
        final Set<Revision> set = Collections
                .synchronizedSet(new HashSet<Revision>());
        final Revision[] duplicate = new Revision[1];
        for (int i = 0; i < 20; i++) {
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    Revision[] last = new Revision[1024];
                    while (!stop.get()) {
                        for (Revision r : last) {
                            set.remove(r);
                        }
                        for (int i = 0; i < last.length; i++) {
                            last[i] = Revision.newRevision(1);
                        }
                        for (Revision r : last) {
                            if (!set.add(r)) {
                                duplicate[0] = r;
                            }
                        }
                    }
                }
            });
            thread.start();
            threads.add(thread);
        }
        Thread.sleep(200);
        stop.set(true);
        for (Thread t : threads) {
            t.join();
        }
        assertNull("Duplicate revision", duplicate[0]);
    }

    @Test
    public void uniqueRevision() throws Exception {
        //Revision.setClock(new Clock.Virtual());
        final BlockingQueue<Revision> revisionQueue = Queues.newLinkedBlockingQueue();
        int noOfThreads = 60;
        final int noOfLoops = 1000;
        List<Thread> workers = new ArrayList<Thread>();
        final AtomicBoolean stop = new AtomicBoolean();
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch stopLatch = new CountDownLatch(noOfThreads);
        for (int i = 0; i < noOfThreads; i++) {
            workers.add(new Thread(new Runnable() {
                @Override
                public void run() {
                    Uninterruptibles.awaitUninterruptibly(startLatch);
                    for (int j = 0; j < noOfLoops && !stop.get(); j++) {
                        revisionQueue.add(Revision.newRevision(1));
                    }
                    stopLatch.countDown();
                }
            }));
        }

        final List<Revision> duplicates = Lists.newArrayList();
        final Set<Revision> seenRevs = Sets.newHashSet();
        workers.add(new Thread(new Runnable() {
            @Override
            public void run() {
                startLatch.countDown();

                while (!stop.get()) {
                    List<Revision> revs = Lists.newArrayList();
                    Queues.drainUninterruptibly(revisionQueue, revs, 5, 100, TimeUnit.MILLISECONDS);
                    record(revs);
                }

                List<Revision> revs = Lists.newArrayList();
                revisionQueue.drainTo(revs);
                record(revs);
            }

            private void record(List<Revision> revs) {
                for (Revision rev : revs) {
                    if (!seenRevs.add(rev)) {
                        duplicates.add(rev);
                    }
                }

                if (!duplicates.isEmpty()) {
                    stop.set(true);
                }
            }
        }));

        for (Thread t : workers) {
            t.start();
        }

        stopLatch.await();
        stop.set(true);

        for (Thread t : workers) {
            t.join();
        }
        assertTrue(String.format("Duplicate rev seen %s %n Seen %s", duplicates, seenRevs), duplicates.isEmpty());
    }

}
