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
package org.apache.jackrabbit.oak.segment.consensus.queue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * A proposal added while the drainer empties and removes its wallet's list must not end up in a
 * list the buffer no longer references (it would never be released). Live: 1 of 8,284 proposals
 * stayed VERIFIED forever at 40 writes/s.
 */
public class AdaptivePackingBufferConcurrencyTest {

    private static final int PRODUCERS = 4;
    private static final int PER_PRODUCER = 10_000;

    @Test(timeout = 120_000)
    public void everyProposalAddedWhileDrainingIsEventuallyDrained() throws Exception {
        AdaptivePackingBuffer buffer = new AdaptivePackingBuffer();
        AdaptiveReleaseGovernor.Decision decision = AdaptiveReleaseGovernor.Decision.healthyDirect();
        long forcedNow = Long.MAX_VALUE / 2;
        AtomicLong drained = new AtomicLong();
        AtomicBoolean producing = new AtomicBoolean(true);
        CountDownLatch start = new CountDownLatch(1);

        List<Thread> producers = new ArrayList<>();
        for (int p = 0; p < PRODUCERS; p++) {
            int producer = p;
            Thread thread = new Thread(() -> {
                awaitQuietly(start);
                for (int i = 0; i < PER_PRODUCER; i++) {
                    buffer.addProposal(proposal("p" + producer + "-" + i), 0L);
                }
            });
            producers.add(thread);
            thread.start();
        }
        Thread drainer = new Thread(() -> {
            awaitQuietly(start);
            while (producing.get()) {
                drained.addAndGet(count(buffer.drainReadyBatches(forcedNow, decision)));
            }
        });
        drainer.start();

        start.countDown();
        for (Thread thread : producers) {
            thread.join();
        }
        producing.set(false);
        drainer.join();
        // Drain the backlog (at most a few batches per wallet per cycle) until the buffer reports nothing left.
        long released;
        do {
            released = count(buffer.drainReadyBatches(forcedNow, decision));
            drained.addAndGet(released);
        } while (released > 0);

        assertEquals("every added proposal is drained exactly once", (long) PRODUCERS * PER_PRODUCER, drained.get());
        assertEquals(0L, ((Number) buffer.getStatsMap().get("pendingProposals")).longValue());
    }

    private static long count(List<List<QueuedProposal>> batches) {
        long total = 0;
        for (List<QueuedProposal> batch : batches) {
            total += batch.size();
        }
        return total;
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static QueuedProposal proposal(String id) {
        QueuedProposal proposal = new QueuedProposal(id, "0xtx-" + id, null, 1L, 30_001L, ProposalState.VERIFIED);
        proposal.setWalletAddress("0xwallet");
        proposal.setPath("/oak-chain/aa/bb/cc/0xwallet/content/" + id);
        proposal.setEpoch(1L);
        return proposal;
    }
}
