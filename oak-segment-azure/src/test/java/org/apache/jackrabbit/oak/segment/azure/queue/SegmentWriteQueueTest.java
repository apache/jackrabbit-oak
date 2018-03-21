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
package org.apache.jackrabbit.oak.segment.azure.queue;

import org.apache.jackrabbit.oak.segment.azure.AzureSegmentArchiveEntry;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SegmentWriteQueueTest {

    private static final byte[] EMPTY_DATA = new byte[0];

    private SegmentWriteQueue queue;

    @After
    public void shutdown() throws IOException {
        if (queue != null) {
            queue.close();
        }
    }

    @Test
    public void testQueue() throws IOException, InterruptedException {
        Set<UUID> added = Collections.synchronizedSet(new HashSet<>());
        Semaphore semaphore = new Semaphore(0);
        queue = new SegmentWriteQueue((tarEntry, data, offset, size) -> {
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
            }
            added.add(new UUID(tarEntry.getMsb(), tarEntry.getLsb()));
        });

        for (int i = 0; i < 10; i++) {
            queue.addToQueue(tarEntry(i), EMPTY_DATA, 0, 0);
        }

        for (int i = 0; i < 10; i++) {
            assertNotNull("Segments should be available for read", queue.read(uuid(i)));
        }
        assertFalse("Queue shouldn't be empty", queue.isEmpty());

        semaphore.release(Integer.MAX_VALUE);
        while (!queue.isEmpty()) {
            Thread.sleep(10);
        }

        assertEquals("There should be 10 segments consumed",10, added.size());
        for (int i = 0; i < 10; i++) {
            assertTrue("Missing consumed segment", added.contains(uuid(i)));
        }
    }

    @Test(timeout = 1000)
    public void testFlush() throws IOException, InterruptedException {
        Set<UUID> added = Collections.synchronizedSet(new HashSet<>());
        Semaphore semaphore = new Semaphore(0);
        queue = new SegmentWriteQueue((tarEntry, data, offset, size) -> {
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
            }
            added.add(new UUID(tarEntry.getMsb(), tarEntry.getLsb()));
        });

        for (int i = 0; i < 3; i++) {
            queue.addToQueue(tarEntry(i), EMPTY_DATA, 0, 0);
        }

        AtomicBoolean flushFinished = new AtomicBoolean(false);
        Set<UUID> addedAfterFlush = new HashSet<>();
        new Thread(() -> {
            try {
                queue.flush();
                flushFinished.set(true);
                addedAfterFlush.addAll(added);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).start();

        Thread.sleep(100);
        assertFalse("Flush should be blocked", flushFinished.get());

        AtomicBoolean addFinished = new AtomicBoolean(false);
        new Thread(() -> {
            try {
                queue.addToQueue(tarEntry(10), EMPTY_DATA, 0, 0);
                addFinished.set(true);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).start();

        Thread.sleep(100);
        assertFalse("Adding segments should be blocked until the flush is finished", addFinished.get());

        semaphore.release(Integer.MAX_VALUE);

        while (!addFinished.get()) {
            Thread.sleep(10);
        }
        assertTrue("Flush should be finished once the ", flushFinished.get());
        assertTrue("Adding segments should be blocked until the flush is finished", addFinished.get());

        for (int i = 0; i < 3; i++) {
            assertTrue(addedAfterFlush.contains(uuid(i)));
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testClose() throws IOException, InterruptedException {
        queue = new SegmentWriteQueue((tarEntry, data, offset, size) -> {});
        queue.close();
        queue.addToQueue(tarEntry(10), EMPTY_DATA, 0, 0);
    }

    @Test
    public void testRecoveryMode() throws IOException, InterruptedException {
        Set<UUID> added = Collections.synchronizedSet(new HashSet<>());
        Semaphore semaphore = new Semaphore(0);
        AtomicBoolean doBreak = new AtomicBoolean(true);
        List<Long> writeAttempts = Collections.synchronizedList(new ArrayList<>());
        queue = new SegmentWriteQueue((tarEntry, data, offset, size) -> {
            writeAttempts.add(System.currentTimeMillis());
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
            }
            if (doBreak.get()) {
                throw new IOException();
            }
            added.add(new UUID(tarEntry.getMsb(), tarEntry.getLsb()));
        });

        for (int i = 0; i < 10; i++) {
            queue.addToQueue(tarEntry(i), EMPTY_DATA, 0, 0);
        }

        semaphore.release(Integer.MAX_VALUE);
        Thread.sleep(100);

        assertTrue(queue.isBroken());
        assertEquals(9, queue.getSize()); // the 10th segment is handled by the recovery thread

        writeAttempts.clear();
        while (writeAttempts.size() < 5) {
            Thread.sleep(100);
        }
        long lastAttempt = writeAttempts.get(0);
        for (int i = 1; i < 5; i++) {
            long delay = writeAttempts.get(i) - lastAttempt;
            assertTrue("The delay between attempts to persist segment should be larger than 1s. Actual: " + delay, delay >= 1000);
            lastAttempt = writeAttempts.get(i);
        }

        AtomicBoolean addFinished = new AtomicBoolean(false);
        new Thread(() -> {
            try {
                queue.addToQueue(tarEntry(10), EMPTY_DATA, 0, 0);
                addFinished.set(true);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).start();

        Thread.sleep(100);
        assertFalse("Adding segments should be blocked until the recovery mode is finished", addFinished.get());

        doBreak.set(false);
        while (queue.isBroken()) {
            Thread.sleep(10);
        }
        assertFalse("Queue shouldn't be broken anymore", queue.isBroken());

        while (added.size() < 11) {
            Thread.sleep(10);
        }
        assertEquals("All segments should be consumed",11, added.size());
        for (int i = 0; i < 11; i++) {
            assertTrue("All segments should be consumed", added.contains(uuid(i)));
        }

        int i = writeAttempts.size() - 10;
        lastAttempt = writeAttempts.get(i);
        for (; i < writeAttempts.size(); i++) {
            long delay = writeAttempts.get(i) - lastAttempt;
            assertTrue("Segments should be persisted immediately", delay < 1000);
            lastAttempt = writeAttempts.get(i);
        }
    }

    private static AzureSegmentArchiveEntry tarEntry(long i) {
        return new AzureSegmentArchiveEntry(0, i, 0, 0, 0, 0, false);
    }

    private static UUID uuid(long i) {
        return new UUID(0, i);
    }

}
