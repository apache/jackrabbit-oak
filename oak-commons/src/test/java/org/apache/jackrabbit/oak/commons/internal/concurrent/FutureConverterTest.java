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
package org.apache.jackrabbit.oak.commons.internal.concurrent;

import org.apache.jackrabbit.guava.common.util.concurrent.ListenableFuture;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Unit cases for {@link FutureConverter}
 */
public class FutureConverterTest {

    @Test
    public void toListenableFutureSuccessfulCompletion() throws Exception {
        CompletableFuture<String> completable = new CompletableFuture<>();
        ListenableFuture<String> listenable = FutureConverter.toListenableFuture(completable);

        completable.complete("hello");
        Assert.assertEquals("hello", listenable.get(1, TimeUnit.SECONDS));
        Assert.assertTrue(listenable.isDone());
    }

    @Test
    public void toListenableFutureExceptionalCompletion() {
        CompletableFuture<String> completable = new CompletableFuture<>();
        ListenableFuture<String> listenable = FutureConverter.toListenableFuture(completable);

        completable.completeExceptionally(new IllegalStateException("bad!"));
        try {
            listenable.get(1, TimeUnit.SECONDS);
            Assert.fail("Expected ExecutionException");
        } catch (ExecutionException e) {
            Assert.assertTrue(e.getCause() instanceof IllegalStateException);
            Assert.assertEquals("bad!", e.getCause().getMessage());
        } catch (Exception e) {
            Assert.fail("Unexpected exception: " + e);
        }
    }

    @Test
    public void toListenableFutureCancelCompletableFuture() {
        CompletableFuture<String> completable = new CompletableFuture<>();
        ListenableFuture<String> listenable = FutureConverter.toListenableFuture(completable);

        completable.cancel(true);
        try {
            listenable.get(1, TimeUnit.SECONDS);
            Assert.fail("Expected CancellationException");
        } catch (CancellationException e) {
            // expected
        } catch (Exception e) {
            Assert.fail("Unexpected exception: " + e);
        }
    }

    @Test
    public void toListenableFutureListenableCancelDoesNotAffectCompletable() {
        CompletableFuture<String> completable = new CompletableFuture<>();
        ListenableFuture<String> listenable = FutureConverter.toListenableFuture(completable);

        listenable.cancel(true);
        Assert.assertTrue(completable.isCancelled());
    }

    @Test
    public void toListenableFutureTestGetRestoresInterruptStatus() throws Exception {
        CompletableFuture<String> completable = new CompletableFuture<>();
        ListenableFuture<String> listenable = FutureConverter.toListenableFuture(completable);

        final AtomicBoolean interruptedInCatch = new AtomicBoolean(false);
        final AtomicBoolean caughtInterruptedException = new AtomicBoolean(false);
        final CountDownLatch threadStarted = new CountDownLatch(1);

        Thread testThread = new Thread(() -> {
            threadStarted.countDown();
            try {
                listenable.get(); // This will block, we interrupt
                Assert.fail("Expected InterruptedException");
            } catch (InterruptedException e) {
                // Expected interrupt
                caughtInterruptedException.set(true);
                interruptedInCatch.set(Thread.currentThread().isInterrupted());
            } catch (Exception e) {
                Assert.fail("Unexpected exception: " + e);
            }
        });

        testThread.start();
        // Wait for thread to start and then interrupt
        threadStarted.await();
        Thread.sleep(50); // Small delay to ensure get() is called
        testThread.interrupt();

        testThread.join();

        Assert.assertTrue("Should have caught InterruptedException", caughtInterruptedException.get());
        Assert.assertTrue("Thread should be interrupted when catching InterruptedException", interruptedInCatch.get());
    }

    @Test
    public void toListenableFutureTestGetTimeoutRestoresInterruptStatus() throws Exception {
        CompletableFuture<String> completable = new CompletableFuture<>();
        ListenableFuture<String> listenable = FutureConverter.toListenableFuture(completable);

        final AtomicBoolean interruptedInCatch = new AtomicBoolean(false);
        final AtomicBoolean caughtInterruptedException = new AtomicBoolean(false);
        final CountDownLatch threadStarted = new CountDownLatch(1);

        Thread testThread = new Thread(() -> {
            threadStarted.countDown();
            try {
                listenable.get(10, TimeUnit.SECONDS); // Will block and get interrupted
                Assert.fail("Expected InterruptedException");
            } catch (InterruptedException e) {
                // Expected interrupt
                caughtInterruptedException.set(true);
                interruptedInCatch.set(Thread.currentThread().isInterrupted());
            } catch (Exception e) {
                Assert.fail("Unexpected exception: " + e);
            }
        });

        testThread.start();
        // Wait for thread to start and then interrupt
        threadStarted.await();
        Thread.sleep(50); // Small delay to ensure get() is called
        testThread.interrupt();

        testThread.join();

        Assert.assertTrue("Should have caught InterruptedException", caughtInterruptedException.get());
        Assert.assertTrue("Thread should be interrupted when catching InterruptedException", interruptedInCatch.get());
    }

}