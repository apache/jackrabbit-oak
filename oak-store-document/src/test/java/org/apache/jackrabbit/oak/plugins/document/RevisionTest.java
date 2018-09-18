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
import java.util.Random;
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

import org.junit.Test;

/**
 * Tests the revision class
 */
public class RevisionTest {
    
    @Test
    public void invalid() {
        // revisions need to start with "br" or "r"
        for(String s : "1234,b,bb,".split(",")) {
            try {
                Revision.fromString(s);
                fail("Expected: Invalid revision id exception for " + s);
            } catch (Exception expected) {
                // expected
            }
        }
    }
    
    @Test
    public void edgeCases() {
        assertEquals("br0-0-0", new Revision(0, 0, 0, true).toString());
        Random rand = new Random(0);
        for (int i = 0; i < 1000; i++) {
            Revision r = new Revision(rand.nextLong(), rand.nextInt(),
                    rand.nextInt(), rand.nextBoolean());
            assertEquals(r.toString(), Revision.fromString(r.toString()).toString());
        }
        for (int i = 0; i < 1000; i++) {
            Revision r = new Revision(rand.nextInt(10), rand.nextInt(10),
                    rand.nextInt(10), rand.nextBoolean());
            assertEquals(r.toString(), Revision.fromString(r.toString()).toString());
        }
    }

    @Test
    public void fromStringToString() {
        for (int i = 0; i < 10000; i++) {
            Revision r = Revision.newRevision(i);
            // System.out.println(r);
            String rs = r.toString();
            Revision r2 = Revision.fromString(rs);
            if(!rs.equals(r2.toString())) {
                r2 = Revision.fromString(rs);
                assertEquals(rs, r2.toString());
            }
            assertEquals(rs, r2.toString());
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
