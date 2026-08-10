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

import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncContext;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncedIdentity;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import javax.jcr.ValueFactory;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl.PARAM_NAME;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

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

    private static final Logger LOG = LoggerFactory.getLogger(SyncConfigTrackerConcurrencyTest.class);

    private static final int THREAD_COUNT = Math.max(16, Runtime.getRuntime().availableProcessors() * 4);
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
        SyncHandler syncHandlerInstance = new MockSyncHandler();
        for (int i = 0; i < MATCHING_SYNC_HANDLER_COUNT; i++) {
            context.registerService(SyncHandler.class, syncHandlerInstance,
                    Map.of(PARAM_NAME, "sh" + i, PARAM_USER_DYNAMIC_MEMBERSHIP, true));
        }
        // registered but never tracked: doesn't match the tracker's dynamic-membership filter
        context.registerService(SyncHandler.class, syncHandlerInstance,
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
        long blockedMs = sampleForBlockingOnTrackedMonitor(tracker::getServiceReferences);
        LOG.info("Contended threads were blocked for {}ms", blockedMs);
        assertTrue("expected concurrent ServiceTracker.getServiceReferences() callers to contend on "
                + "the shared Tracked monitor, as observed in the OAK-12341 thread dump", blockedMs > 0);
    }

    /**
     * After the OAK-12341 fix, {@code isEnabled()} reads a {@code CopyOnWriteArrayList} that is
     * maintained from {@code addingService}/{@code removedService} instead of calling
     * {@code getServiceReferences()}, so concurrent readers never contend on the ServiceTracker's
     * {@code Tracked} monitor.
     */
    @Test
    public void concurrentIsEnabledDoesNotBlockReaders() throws Exception {
        long blockedMs = sampleForBlockingOnTrackedMonitor(tracker::isEnabled);
        LOG.info("Contended threads were blocked for {}ms", blockedMs);
        assertEquals("expected isEnabled() to no longer contend on the ServiceTracker$Tracked monitor "
                + "once service references are cached (OAK-12341)", 0, blockedMs);
    }

    /**
     * Runs {@code action} on {@link #THREAD_COUNT} threads in a tight loop
     * and polls the {@link ThreadMXBean} of all worker threads up to 1000
     * times
     * throughout, returning {@code true} as
     * soon as any thread is observed {@code BLOCKED} while waiting to lock a
     * {@code ServiceTracker$Tracked} instance held by another thread.
     */
    private long sampleForBlockingOnTrackedMonitor(Runnable action) throws Exception {
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        assumeTrue("JVM supporting thread contention monitoring required",
                threadMXBean.isThreadContentionMonitoringSupported());
        threadMXBean.setThreadContentionMonitoringEnabled(true);

        ExecutorService pool = new ThreadPoolExecutor(
                THREAD_COUNT, THREAD_COUNT,
                0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(THREAD_COUNT * 2),
                new ThreadPoolExecutor.CallerRunsPolicy());
        Set<Long> threadIds = ConcurrentHashMap.newKeySet();
        CountDownLatch allThreadsStarted = new CountDownLatch(THREAD_COUNT);
        AtomicBoolean stop = new AtomicBoolean(false);

        try {
            pool.execute(() -> {
                while (!stop.get()) {
                    pool.execute(() -> {
                        threadIds.add(Thread.currentThread().getId());
                        allThreadsStarted.countDown();
                        action.run();
                    });
                }
            });

            assertTrue("worker threads did not start in time", allThreadsStarted.await(10, TimeUnit.SECONDS));

            long[] threads = threadIds.stream().mapToLong(Long::longValue).toArray();
            long blockedMs = 0;
            for (int i = 0; i < 1000; i++) {
                ThreadInfo[] threadInfo = threadMXBean.getThreadInfo(threads);
                blockedMs += Stream.of(threadInfo)
                        .filter(Objects::nonNull)
                        .filter(info -> info.getThreadState() == Thread.State.BLOCKED)
                        .filter(info -> info.getLockName() != null && info.getLockName().contains("ServiceTracker$Tracked"))
                        .mapToLong(ThreadInfo::getBlockedTime)
                        .sum();
            }
            return blockedMs;
        } finally {
            stop.set(true);
            pool.shutdownNow();
        }
    }

    private static class MockSyncHandler implements SyncHandler {
        @Override
        public @NotNull String getName() {
            throw new UnsupportedOperationException();
        }

        @Override
        public @NotNull SyncContext createContext(@NotNull ExternalIdentityProvider idp, @NotNull UserManager userManager, @NotNull ValueFactory valueFactory) {
            throw new UnsupportedOperationException();
        }

        @Override
        public @Nullable SyncedIdentity findIdentity(@NotNull UserManager userManager, @NotNull String id) throws RepositoryException {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean requiresSync(@NotNull SyncedIdentity identity) {
            throw new UnsupportedOperationException();
        }

        @Override
        public @NotNull Iterator<SyncedIdentity> listIdentities(@NotNull UserManager userManager) throws RepositoryException {
            throw new UnsupportedOperationException();
        }
    }
}
