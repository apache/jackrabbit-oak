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
package org.apache.jackrabbit.oak.segment.http.server.sse;

import org.junit.Test;

import jakarta.servlet.AsyncContext;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

public class EventBroadcasterTest {

    @Test
    public void testBroadcastSendsToMatchingClientsAndBuffersRecentEvents() throws Exception {
        EventBroadcaster broadcaster = new EventBroadcaster();
        try {
            StringWriter matchingBody = new StringWriter();
            StringWriter nonMatchingBody = new StringWriter();
            SSEClient matching = new SSEClient(
                mock(AsyncContext.class),
                new PrintWriter(matchingBody),
                new HashSet<>(Collections.singletonList("content")),
                new HashSet<>(),
                new HashSet<>(),
                null,
                null
            );
            SSEClient nonMatching = new SSEClient(
                mock(AsyncContext.class),
                new PrintWriter(nonMatchingBody),
                new HashSet<>(Collections.singletonList("binary")),
                new HashSet<>(),
                new HashSet<>(),
                null,
                null
            );

            assertTrue(broadcaster.addClient(matching));
            assertTrue(broadcaster.addClient(nonMatching));

            ContentEvent event = ContentEvent.builder().id("evt-1").timestamp(100L).build();
            broadcaster.broadcast(event);

            awaitContains(matchingBody, "id: evt-1");
            assertEquals("", nonMatchingBody.toString());
            List<ContentEvent> recent = broadcaster.getRecentEvents(10);
            assertEquals(1, recent.size());
            assertEquals("evt-1", recent.get(0).getId());
            assertEquals(1L, broadcaster.getTotalEventsBroadcast());
        } finally {
            broadcaster.shutdown();
        }
    }

    @Test
    public void testEmitHelpersPopulateExpectedEventTypes() {
        EventBroadcaster broadcaster = new EventBroadcaster();
        try {
            broadcaster.emitContentWrite("/content/doc-1", "0xwallet", "acme", "hello", "0xsig", "text/plain");
            broadcaster.emitBinaryUpload("/content/doc-2", "0xwallet", "acme", "img", "QmCid", 42L, "image/png");
            broadcaster.emitContentDelete("/content/doc-3", "0xwallet", "acme", "0xdead");
            broadcaster.emitWalletRegistration("0xwallet", "owner");
            broadcaster.emitConsensusEvent(ContentEvent.Action.COMMIT, "commit");

            List<ContentEvent> recent = broadcaster.getRecentEvents(10);
            assertEquals(5, recent.size());
            assertEquals("content", recent.get(0).getType());
            assertEquals("binary", recent.get(1).getType());
            assertEquals("delete", recent.get(2).getType());
            assertEquals("wallet", recent.get(3).getType());
            assertEquals("consensus", recent.get(4).getType());
            assertEquals(5L, broadcaster.getTotalEventsBroadcast());
        } finally {
            broadcaster.shutdown();
        }
    }

    @Test
    public void emitDoesNotWaitForASubscriberWhoseWriterBlocks() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        EventBroadcaster broadcaster = new EventBroadcaster();
        ExecutorService emitter = Executors.newSingleThreadExecutor();
        try {
            broadcaster.addClient(client(mock(AsyncContext.class), new PrintWriter(blockingWriter(new CountDownLatch(1), release))));

            Future<?> emits = emitter.submit(() -> {
                for (int i = 0; i < 5_000; i++) {
                    broadcaster.emitContentWrite("/content/doc-" + i, "0xwallet", "acme", "m", "0xsig", "page");
                }
            });

            emits.get(2, TimeUnit.SECONDS);
            assertEquals(5_000L, broadcaster.getTotalEventsBroadcast());
        } finally {
            release.countDown();
            emitter.shutdownNow();
            broadcaster.shutdown();
        }
    }

    @Test
    public void subscriberStuckInAWriteIsClosed() throws Exception {
        CountDownLatch writing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        EventBroadcaster broadcaster = new EventBroadcaster(16, 200L);
        try {
            AsyncContext asyncContext = mock(AsyncContext.class);
            SSEClient stuck = client(asyncContext, new PrintWriter(blockingWriter(writing, release)));
            broadcaster.addClient(stuck);

            broadcaster.emitContentWrite("/content/doc", "0xwallet", "acme", "m", "0xsig", "page");

            assertTrue(writing.await(2, TimeUnit.SECONDS));
            verify(asyncContext, timeout(2_000)).complete();
            assertTrue(stuck.isClosed());
            assertEquals(0, broadcaster.getClientCount());
        } finally {
            release.countDown();
            broadcaster.shutdown();
        }
    }

    private static SSEClient client(AsyncContext asyncContext, PrintWriter writer) {
        return new SSEClient(asyncContext, writer, new HashSet<>(), new HashSet<>(), new HashSet<>(), null, null);
    }

    /** A socket whose peer stopped reading: every write blocks until released. */
    private static Writer blockingWriter(CountDownLatch writing, CountDownLatch release) {
        return new Writer() {
            @Override
            public void write(char[] buffer, int offset, int length) {
                writing.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
    }

    private static void awaitContains(StringWriter body, String expected) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (!body.toString().contains(expected) && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertTrue(body.toString().contains(expected));
    }
}
