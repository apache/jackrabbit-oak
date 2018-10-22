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
package org.apache.jackrabbit.oak.plugins.document.persistentCache.async;

import static java.lang.String.valueOf;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.sleep;
import static org.apache.jackrabbit.oak.plugins.document.persistentCache.async.CacheActionDispatcher.MAX_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.jackrabbit.oak.commons.StringUtils;
import org.junit.Test;

public class CacheActionDispatcherTest {

    @Test
    public void testMaxQueueSize() {
        CacheActionDispatcher dispatcher = new CacheActionDispatcher();

        for (int i = 0; i < MAX_SIZE + 10; i++) {
            dispatcher.add(createWriteAction(valueOf(i)));
        }
        assertEquals(MAX_SIZE, dispatcher.queue.size());
        assertEquals("0", dispatcher.queue.peek().toString());
    }

    @Test
    public void testQueue() throws InterruptedException {
        int threads = 5;
        int actionsPerThread = 100;

        final CacheActionDispatcher dispatcher = new CacheActionDispatcher();
        Thread queueThread = new Thread(dispatcher);
        queueThread.start();

        List<DummyCacheWriteAction> allActions = new ArrayList<DummyCacheWriteAction>();
        List<Thread> producerThreads = new ArrayList<Thread>();
        for (int i = 0; i < threads; i++) {
            final List<DummyCacheWriteAction> threadActions = new ArrayList<DummyCacheWriteAction>();
            for (int j = 0; j < actionsPerThread; j++) {
                DummyCacheWriteAction action = new DummyCacheWriteAction(String.format("%d_%d", i, j));
                threadActions.add(action);
                allActions.add(action);
            }
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (DummyCacheWriteAction a : threadActions) {
                        dispatcher.add(a);
                    }
                }
            });
            producerThreads.add(t);
        }

        for (Thread t : producerThreads) {
            t.start();
        }
        for (Thread t : producerThreads) {
            t.join();
        }

        long start = currentTimeMillis();
        while (!allActions.isEmpty()) {
            Iterator<DummyCacheWriteAction> it = allActions.iterator();
            while (it.hasNext()) {
                if (it.next().finished) {
                    it.remove();
                }
            }
            if (currentTimeMillis() - start > 10000) {
                fail("Following actions hasn't been executed: " + allActions);
            }
        }

        dispatcher.stop();
        queueThread.join();
        assertFalse(queueThread.isAlive());
    }

    @Test
    public void maxMemory() throws Exception {
        // calculate memory for a few actions and use as memory maximum
        long maxMemory = 0;
        List<CacheAction> actions = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            CacheAction a = new DummyCacheWriteAction("id-" + i, 0);
            actions.add(a);
            maxMemory += a.getMemory();
        }
        CacheActionDispatcher dispatcher = new CacheActionDispatcher(maxMemory);

        // adding actions to the queue must all succeed
        for (CacheAction a : actions) {
            assertTrue(dispatcher.add(a));
        }

        // adding more must be rejected
        assertFalse(dispatcher.add(new DummyCacheWriteAction("foo", 0)));

        // drain the queue
        Thread t = new Thread(dispatcher);
        t.start();

        for (int i = 0; i < 100; i++) {
            if (dispatcher.getMemory() == 0) {
                break;
            }
            Thread.sleep(20);
        }
        assertEquals(0, dispatcher.getMemory());
        dispatcher.stop();
        t.join();

        // must be able to add again
        assertTrue(dispatcher.add(actions.get(0)));

        // but not if it exceeds the maximum memory
        String id = "abcdef";
        CacheAction big;
        do {
            big = new DummyCacheWriteAction(id, 0);
            id = id + id;
        } while (big.getMemory() < maxMemory);
        assertFalse(dispatcher.add(big));
    }

    private DummyCacheWriteAction createWriteAction(String id) {
        return new DummyCacheWriteAction(id);
    }

    private class DummyCacheWriteAction implements CacheAction {

        private final String id;

        private final long delay;

        private volatile boolean finished;

        private DummyCacheWriteAction(String id) {
            this(id, new Random().nextInt(10));
        }

        private DummyCacheWriteAction(String id, long delay) {
            this.id = id;
            this.delay = delay;
        }

        @Override
        public void execute() {
            try {
                sleep(delay);
            } catch (InterruptedException e) {
                fail("Interrupted");
            }
            finished = true;
        }

        @Override
        public int getMemory() {
            return StringUtils.estimateMemoryUsage(id);
        }

        @Override
        public String toString() {
            return id;
        }

    }
}
