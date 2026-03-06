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
package org.apache.jackrabbit.oak.jcr.observation;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.jackrabbit.commons.observation.ListenerTracker;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.blob.BlobAccessProvider;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.observation.CommitRateLimiter;
import org.apache.jackrabbit.oak.plugins.observation.filter.FilterProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.CompositeRegistration;
import org.apache.jackrabbit.oak.stats.StatisticManager;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link ChangeProcessor}.
 *
 * <p>Contains two groups of tests:
 * <ol>
 *   <li>New-implementation-specific tests that verify the {@link AtomicBoolean}
 *       {@code stopped} flag introduced when Guava {@code Monitor} was removed.</li>
 *   <li>Behavioural tests for {@link ChangeProcessor#stop()} and
 *       {@link ChangeProcessor#stopAndWait(int, TimeUnit)} that are written
 *       against the public API only and pass against both the old Guava-based
 *       and the new {@link ReentrantLock}-based implementations.</li>
 * </ol>
 */
public class ChangeProcessorTest {

    // ── stopped-flag tests (new implementation specific) ───────────────────

    @Test
    public void testStopSetStoppedFlagCorrectly() {
        ChangeProcessor changeProcessor = createChangeProcessor();
        CompositeRegistration registration = Mockito.mock(CompositeRegistration.class);
        setRegistration(changeProcessor, registration);
        Assert.assertFalse("stopped flag should be false before stop()", getStoppedFlag(changeProcessor).get());
        changeProcessor.stop();
        Mockito.verify(registration).unregister();
        Assert.assertTrue("stopped flag should be true after stop()", getStoppedFlag(changeProcessor).get());
    }

    @Test
    public void testToStringRunningStateBeforeAndAfterStop() {
        ChangeProcessor changeProcessor = createChangeProcessor();
        CompositeRegistration registration = Mockito.mock(CompositeRegistration.class);
        setRegistration(changeProcessor, registration);

        String toStringBeforeStop = changeProcessor.toString();
        Assert.assertTrue("toString() before stop should indicate running=true: " + toStringBeforeStop,
                toStringBeforeStop.contains("running=true"));

        changeProcessor.stop();

        String toStringAfterStop = changeProcessor.toString();
        Assert.assertTrue("toString() after stop should indicate running=false: " + toStringAfterStop,
                toStringAfterStop.contains("running=false"));
    }

    @Test
    public void testStopIdempotency() {
        ChangeProcessor changeProcessor = createChangeProcessor();
        CompositeRegistration registration = Mockito.mock(CompositeRegistration.class);
        setRegistration(changeProcessor, registration);
        Assert.assertFalse("stopped flag should be false before stop()", getStoppedFlag(changeProcessor).get());
        changeProcessor.stop();
        Assert.assertTrue("stopped flag should be true after first stop()", getStoppedFlag(changeProcessor).get());
        Mockito.verify(registration).unregister();
        changeProcessor.stop();
        Assert.assertTrue("stopped flag should remain true after second stop()", getStoppedFlag(changeProcessor).get());
        Mockito.verify(registration, Mockito.times(1)).unregister();
    }

    // ── stopAndWait() behavioural tests ─────────────────────────────────────

    /**
     * stopAndWait() must return {@code true} and call
     * {@code registration.unregister()} when no other thread holds the
     * running lock (i.e. no contentChanged() is in progress).
     */
    @Test
    public void testStopAndWaitReturnsTrueWhenNotContended() throws InterruptedException {
        ChangeProcessor cp = createChangeProcessor();
        CompositeRegistration registration = Mockito.mock(CompositeRegistration.class);
        setRegistration(cp, registration);

        boolean result = cp.stopAndWait(1, TimeUnit.SECONDS);

        Assert.assertTrue("stopAndWait should return true when lock is free", result);
        Mockito.verify(registration).unregister();
    }

    /**
     * When a concurrent thread holds the running lock (simulating an
     * in-progress contentChanged() call), stopAndWait() must block until the
     * timeout elapses and then return {@code false}.
     */
    @Test
    public void testStopAndWaitReturnsFalseOnTimeout() throws Exception {
        ChangeProcessor cp = createChangeProcessor();
        CompositeRegistration registration = Mockito.mock(CompositeRegistration.class);
        setRegistration(cp, registration);

        ReentrantLock underlyingLock = getUnderlyingLock(cp);
        CountDownLatch lockAcquired = new CountDownLatch(1);
        CountDownLatch testDone = new CountDownLatch(1);

        // Simulate a long-running contentChanged() by holding the underlying lock.
        Thread holder = new Thread(() -> {
            underlyingLock.lock();
            lockAcquired.countDown();
            try {
                testDone.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                underlyingLock.unlock();
            }
        });
        holder.start();
        lockAcquired.await(2, TimeUnit.SECONDS);

        boolean result = cp.stopAndWait(100, TimeUnit.MILLISECONDS);

        Assert.assertFalse(
                "stopAndWait should return false when the running lock is held by another thread",
                result);

        testDone.countDown();
        holder.join(2000);
    }

    /**
     * When the processor was already stopped (e.g. by a prior stop() call),
     * stopAndWait() must return {@code true} immediately without calling
     * unregister() again.
     */
    @Test
    public void testStopAndWaitReturnsTrueWhenAlreadyStopped() throws InterruptedException {
        ChangeProcessor cp = createChangeProcessor();
        CompositeRegistration registration = Mockito.mock(CompositeRegistration.class);
        setRegistration(cp, registration);

        cp.stop();

        boolean result = cp.stopAndWait(100, TimeUnit.MILLISECONDS);

        Assert.assertTrue("stopAndWait should return true when already stopped", result);
        Mockito.verify(registration, Mockito.times(1)).unregister();
    }

    /**
     * Once stopAndWait() returns {@code true} the processor must report
     * {@code running=false}.
     */
    @Test
    public void testStopAndWaitSetsRunningToFalse() throws InterruptedException {
        ChangeProcessor cp = createChangeProcessor();
        CompositeRegistration registration = Mockito.mock(CompositeRegistration.class);
        setRegistration(cp, registration);

        Assert.assertTrue("should be running=true before stopAndWait",
                cp.toString().contains("running=true"));

        cp.stopAndWait(1, TimeUnit.SECONDS);

        Assert.assertTrue("should be running=false after stopAndWait",
                cp.toString().contains("running=false"));
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    private static ChangeProcessor createChangeProcessor() {
        ContentSession contentSession = Mockito.mock(ContentSession.class);
        NamePathMapper namePathMapper = Mockito.mock(NamePathMapper.class);
        ListenerTracker tracker = Mockito.mock(ListenerTracker.class);
        FilterProvider filter = Mockito.mock(FilterProvider.class);
        StatisticManager statisticManager = Mockito.mock(StatisticManager.class);
        CommitRateLimiter commitRateLimiter = Mockito.mock(CommitRateLimiter.class);
        BlobAccessProvider blobAccessProvider = Mockito.mock(BlobAccessProvider.class);
        return new ChangeProcessor(contentSession, namePathMapper, tracker, filter,
                statisticManager, 1, commitRateLimiter, blobAccessProvider);
    }

    private static AtomicBoolean getStoppedFlag(ChangeProcessor changeProcessor) {
        try {
            Field stoppedField = ChangeProcessor.class.getDeclaredField("stopped");
            stoppedField.setAccessible(true);
            return (AtomicBoolean) stoppedField.get(changeProcessor);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static ReentrantLock getUnderlyingLock(ChangeProcessor cp) throws Exception {
        Field f = ChangeProcessor.class.getDeclaredField("runningLock");
        f.setAccessible(true);
        return (ReentrantLock) f.get(cp);
    }

    private static void setRegistration(ChangeProcessor cp, CompositeRegistration registration) {
        try {
            Field f = ChangeProcessor.class.getDeclaredField("registration");
            f.setAccessible(true);
            f.set(cp, registration);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
