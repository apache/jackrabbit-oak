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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal;

import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncHandler;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl.PARAM_NAME;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Reproduces the contention behind OAK-12341: many session threads resolving a
 * {@code PermissionProvider} concurrently call {@code SyncConfigTracker.isEnabled()}. Before the
 * fix, that call delegated to {@code ServiceTracker.getServiceReferences()} on every invocation,
 * which internally synchronizes on the tracker's shared {@code Tracked} instance, so concurrent
 * readers serialize on that monitor exactly as seen in the reported thread dump:
 * <pre>
 * "...": BLOCKED (on object monitor)
 *     at org.osgi.util.tracker.ServiceTracker.getServiceReferences(ServiceTracker.java:531)
 *     - waiting to lock &lt;0x...&gt; (a org.osgi.util.tracker.ServiceTracker$Tracked)
 *     at ...SyncConfigTracker.getReferences(SyncConfigTracker.java:166)
 * </pre>
 * The two tests below drive {@link #THREAD_COUNT} threads against the same {@code SyncConfigTracker}
 * instance: one exercising the pre-fix code path (the raw, still publicly inherited
 * {@code getServiceReferences()}), the other exercising {@code isEnabled()} as it exists after the
 * OAK-12341 fix. Thread states are sampled via {@link ThreadMXBean} while both run.
 */
public class SyncConfigTrackerConcurrencyTest {

    private static final int THREAD_COUNT = Math.max(16, Runtime.getRuntime().availableProcessors() * 4);
    private static final long SAMPLE_MILLIS = 1500;
    private static final int MATCHING_SYNC_HANDLER_COUNT = 5;

    @Rule
    public final OsgiContext context = new OsgiContext();

    private SyncHandlerMappingTracker mappingTracker;
    private SyncConfigTracker tracker;

    @Before
    public void before() {
        mappingTracker = new SyncHandlerMappingTracker(context.bundleContext());
        mappingTracker.open();

        tracker = new SyncConfigTracker(context.bundleContext(), mappingTracker);
        tracker.open();

        // register several SyncHandlers with dynamic membership enabled, mirroring a deployment
        // with multiple IDPs, so the ServiceTracker actually has more than one reference to
        // fetch/copy on every getServiceReferences() call
        for (int i = 0; i < MATCHING_SYNC_HANDLER_COUNT; i++) {
            context.registerService(SyncHandler.class, mock(SyncHandler.class),
                    Map.of(PARAM_NAME, "sh" + i, PARAM_USER_DYNAMIC_MEMBERSHIP, true));
        }
        // registered but never tracked: doesn't match the tracker's dynamic-membership filter
        context.registerService(SyncHandler.class, mock(SyncHandler.class),
                Map.of(PARAM_NAME, "sh-disabled", PARAM_USER_DYNAMIC_MEMBERSHIP, false));

        assertTrue(tracker.isEnabled());
        assertEquals(MATCHING_SYNC_HANDLER_COUNT, tracker.getServiceReferences().length);
    }

    @After
    public void after() {
        mappingTracker.close();
        tracker.close();
    }

    /**
     * Root-cause reproduction: hammering the inherited, un-cached
     * {@code ServiceTracker.getServiceReferences()} from many threads reproduces the
     * "BLOCKED ... a ServiceTracker$Tracked" contention from the OAK-12341 thread dump, even
     * though every caller is only reading, not modifying, the tracked services.
     */
    @Test
    public void concurrentGetServiceReferencesBlocksReaders() throws Exception {
        boolean blockedOnTracked = sampleForBlockingOnTrackedMonitor(tracker::getServiceReferences);
        assertTrue("expected concurrent ServiceTracker.getServiceReferences() callers to contend on "
                + "the shared Tracked monitor, as observed in the OAK-12341 thread dump", blockedOnTracked);
    }

    /**
     * After the OAK-12341 fix, {@code isEnabled()} reads a {@code CopyOnWriteArrayList} that is
     * maintained from {@code addingService}/{@code removedService} instead of calling
     * {@code getServiceReferences()}, so concurrent readers never contend on the ServiceTracker's
     * {@code Tracked} monitor.
     */
    @Test
    public void concurrentIsEnabledDoesNotBlockReaders() throws Exception {
        boolean blockedOnTracked = sampleForBlockingOnTrackedMonitor(tracker::isEnabled);
        assertFalse("expected isEnabled() to no longer contend on the ServiceTracker$Tracked monitor "
                + "once service references are cached (OAK-12341)", blockedOnTracked);
    }

    /**
     * Runs {@code action} on {@link #THREAD_COUNT} threads in a tight loop for
     * {@link #SAMPLE_MILLIS} and polls {@link ThreadMXBean} throughout, returning {@code true} as
     * soon as any thread is observed {@code BLOCKED} while waiting to lock a
     * {@code ServiceTracker$Tracked} instance held by another thread.
     */
    @SuppressWarnings("deprecation") // Thread.threadId() replacement requires Java 19+; this module targets 17
    private boolean sampleForBlockingOnTrackedMonitor(Runnable action) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch started = new CountDownLatch(THREAD_COUNT);
        AtomicBoolean stop = new AtomicBoolean(false);
        List<Long> threadIds = new CopyOnWriteArrayList<>();
        List<Future<?>> futures = new ArrayList<>();

        try {
            for (int i = 0; i < THREAD_COUNT; i++) {
                futures.add(pool.submit(() -> {
                    threadIds.add(Thread.currentThread().getId());
                    started.countDown();
                    while (!stop.get()) {
                        action.run();
                    }
                }));
            }
            assertTrue("worker threads did not start in time", started.await(10, TimeUnit.SECONDS));

            ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(SAMPLE_MILLIS);
            boolean blocked = false;
            while (!blocked && System.nanoTime() < deadline) {
                for (Long tid : threadIds) {
                    ThreadInfo info = threadMXBean.getThreadInfo(tid);
                    if (info != null && info.getThreadState() == Thread.State.BLOCKED
                            && info.getLockName() != null && info.getLockName().contains("ServiceTracker$Tracked")) {
                        blocked = true;
                        break;
                    }
                }
            }
            return blocked;
        } finally {
            stop.set(true);
            for (Future<?> f : futures) {
                f.get(10, TimeUnit.SECONDS);
            }
            pool.shutdown();
        }
    }
}
