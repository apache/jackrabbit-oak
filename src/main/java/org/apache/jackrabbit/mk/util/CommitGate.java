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
package org.apache.jackrabbit.mk.util;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A gate where listeners can wait for a new commit.
 */
public class CommitGate {

    private volatile String currentHead;
    private volatile AtomicReference<CountDownLatch> latchRef = new AtomicReference<CountDownLatch>();

    /**
     * Wait for a new commit to occur. In very few cases, this method may return
     * with the old head before the requested timeout.
     *
     * @param lastHead the last head
     * @param millis   the maximum number of milliseconds to wait (0 means don't wait)
     * @return the new head (or old head, if no new commit occurred)
     * @throws InterruptedException if the thread was interrupted
     */
    public String waitForCommit(String lastHead, long millis) throws InterruptedException {
        if (millis == 0 || (currentHead != null && !currentHead.equals(lastHead))) {
            return currentHead;
        }
        CountDownLatch latch = latchRef.get();
        if (latch == null) {
            latch = new CountDownLatch(1);
            CountDownLatch old = latchRef.getAndSet(latch);
            if (old != null) {
                // may cause a spurious release, but that's ok
                old.countDown();
            }
        }
        latch.await(millis, TimeUnit.MILLISECONDS);
        return currentHead;
    }

    /**
     * Commit a new head. Waiting threads are awoken.
     *
     * @param newHead the new head
     */
    public void commit(String newHead) {
        currentHead = newHead;
        CountDownLatch latch = latchRef.get();
        if (latch != null) {
            // may cause a spurious release, but that's ok
            latch.countDown();
            latch = latchRef.getAndSet(null);
            if (latch != null) {
                latch.countDown();
            }
        }
    }

}
