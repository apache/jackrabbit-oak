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
package org.apache.jackrabbit.oak.segment.consensus.aeron;

import io.aeron.Aeron;
import io.aeron.archive.Archive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.exceptions.ActiveDriverException;
import io.aeron.exceptions.DriverTimeoutException;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AeronClusterLauncherTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @After
    public void clearProperties() {
        System.clearProperty("oak.cluster.environment");
        System.clearProperty("oak.cluster.session.timeout.minutes");
        System.clearProperty("oak.cluster.session.timeout.seconds");
    }

    @Test
    public void secondsPropertyWinsOverMinutes() {
        System.setProperty("oak.cluster.session.timeout.seconds", "15");
        System.setProperty("oak.cluster.session.timeout.minutes", "7");

        AeronClusterLauncher.SessionTimeoutConfig config = AeronClusterLauncher.resolveSessionTimeoutConfig();

        assertEquals(TimeUnit.SECONDS.toNanos(15), config.timeoutNs);
        assertEquals("oak.cluster.session.timeout.seconds", config.source);
    }

    @Test
    public void minutesPropertyIsStillHonoured() {
        System.setProperty("oak.cluster.session.timeout.minutes", "7");

        AeronClusterLauncher.SessionTimeoutConfig config = AeronClusterLauncher.resolveSessionTimeoutConfig();

        assertEquals(TimeUnit.MINUTES.toNanos(7), config.timeoutNs);
        assertEquals("oak.cluster.session.timeout.minutes", config.source);
    }

    @Test
    public void defaultIsThirtySecondsWhateverTheEnvironment() {
        for (String environment : new String[] {"dev", "staging", "prod", "custom"}) {
            System.setProperty("oak.cluster.environment", environment);

            AeronClusterLauncher.SessionTimeoutConfig config = AeronClusterLauncher.resolveSessionTimeoutConfig();

            assertEquals(environment, TimeUnit.SECONDS.toNanos(30), config.timeoutNs);
            assertEquals("default", config.source);
        }
    }

    @Test
    public void invalidValuesFallThroughToTheNextSource() {
        System.setProperty("oak.cluster.session.timeout.seconds", "nope");
        System.setProperty("oak.cluster.session.timeout.minutes", "3");
        assertEquals(TimeUnit.MINUTES.toNanos(3), AeronClusterLauncher.resolveSessionTimeoutConfig().timeoutNs);

        System.setProperty("oak.cluster.session.timeout.minutes", "0");
        assertEquals(TimeUnit.SECONDS.toNanos(30), AeronClusterLauncher.resolveSessionTimeoutConfig().timeoutNs);
    }

    @Test
    public void resolvedTimeoutReachesTheConsensusModuleContext() {
        System.setProperty("oak.cluster.session.timeout.seconds", "12");

        AeronClusterContextFactory.LaunchContexts contexts = AeronClusterContextFactory.create(
            0, new File("target/aeron-session-timeout"), mock(ClusteredService.class), "aeron-test-dir",
            "127.0.0.1", List.of("127.0.0.1"), mock(org.agrona.concurrent.ShutdownSignalBarrier.class),
            component -> { }, 131072, 131072, 65536, 10_000, 65536,
            AeronClusterLauncher.resolveSessionTimeoutConfig(),
            throwable -> { }, throwable -> { }, throwable -> { });

        assertEquals(TimeUnit.SECONDS.toNanos(12), contexts.consensusModuleContext.sessionTimeoutNs());
        assertEquals(TimeUnit.SECONDS.toNanos(12), contexts.freshCopy().consensusModuleContext.sessionTimeoutNs());
    }

    @Test
    public void helperMethodsHandleBlankAndInvalidValues() throws Exception {
        assertNull(invokeParsePositiveInt(null));
        assertNull(invokeParsePositiveInt(" "));
        assertNull(invokeParsePositiveInt("-1"));
        assertNull(invokeParsePositiveInt("nope"));
        assertEquals(Integer.valueOf(3), invokeParsePositiveInt("3"));

        System.clearProperty("aeron.socket.so_sndbuf");
        assertEquals(16, invokeGetPositiveIntProperty("aeron.socket.so_sndbuf", 16));

        System.setProperty("aeron.socket.so_sndbuf", "bad");
        assertEquals(16, invokeGetPositiveIntProperty("aeron.socket.so_sndbuf", 16));

        System.setProperty("aeron.socket.so_sndbuf", "32");
        assertEquals(32, invokeGetPositiveIntProperty("aeron.socket.so_sndbuf", 16));
    }

    @Test
    public void accessorsAndShutdownBehaveWithoutLaunchingCluster() throws Exception {
        AeronClusterLauncher launcher = new AeronClusterLauncher(
            1,
            List.of("node-0", "node-1"),
            new File("."),
            mock(ClusteredService.class),
            mock(AeronClusterAddressResolver.class),
            mock(AeronClusterErrorPolicy.class)
        );

        assertEquals(AeronClusterLauncher.getPortBase(), launcher.getClusterBasePort());
        assertEquals(AeronClusterLauncher.calculatePort(1, 7), AeronClusterLauncher.calculatePort(
            AeronClusterLauncher.getPortBase(), 1, 7));
        assertEquals("node-1", invokeGetHostname(launcher));
        assertNull(launcher.getAeron());

        ClusteredServiceContainer container = mock(ClusteredServiceContainer.class);
        ClusteredServiceContainer.Context context = mock(ClusteredServiceContainer.Context.class);
        Aeron aeron = mock(Aeron.class);
        when(container.context()).thenReturn(context);
        when(context.aeron()).thenReturn(aeron);
        setField(launcher, "container", container);
        assertSame(aeron, launcher.getAeron());

        CrashHandler crashHandler = mock(CrashHandler.class);
        MediaDriverHealthMonitor healthMonitor = mock(MediaDriverHealthMonitor.class);
        setField(launcher, "crashHandler", crashHandler);
        setField(launcher, "healthMonitor", healthMonitor);
        assertSame(crashHandler, launcher.getCrashHandler());
        assertSame(healthMonitor, launcher.getHealthMonitor());

        org.agrona.concurrent.ShutdownSignalBarrier barrier = mock(org.agrona.concurrent.ShutdownSignalBarrier.class);
        setField(launcher, "barrier", barrier);
        launcher.awaitShutdown();
        verify(barrier).await();

        launcher.shutdown();
        assertTrue(((AtomicBoolean) getField(launcher, "shutdownScheduled")).get());
    }

    @Test
    public void launchMediaDriverRetriesWhenCncFileIsUninitialised() throws Exception {
        File aeronDir = tempFolder.newFolder("aeron-driver");
        File cncFile = new File(aeronDir, "cnc.dat");
        Files.write(cncFile.toPath(), "stale".getBytes(StandardCharsets.UTF_8));

        AtomicInteger attempts = new AtomicInteger();
        ClusteredMediaDriver expected = mock(ClusteredMediaDriver.class);
        AeronClusterLauncher launcher = newLauncher(contexts -> {
            if (attempts.getAndIncrement() == 0) {
                throw new DriverTimeoutException("FATAL - CnC file is created but not initialised");
            }
            return expected;
        });

        ClusteredMediaDriver resolved = launcher.launchMediaDriver(contexts(), aeronDir.getAbsolutePath());

        assertSame(expected, resolved);
        assertEquals(2, attempts.get());
        assertFalse(cncFile.exists());
    }

    @Test
    public void launchMediaDriverRefusesActiveDriverWithoutDeletingItsDirectory() throws Exception {
        File aeronDir = tempFolder.newFolder("active-driver");
        Files.write(new File(aeronDir, "driver.lock").toPath(), "lock".getBytes(StandardCharsets.UTF_8));

        AtomicInteger attempts = new AtomicInteger();
        AeronClusterLauncher launcher = newLauncher(contexts -> {
            attempts.incrementAndGet();
            throw new ActiveDriverException("Active media driver detected: " + aeronDir + "/cnc.dat");
        });

        try {
            launcher.launchMediaDriver(contexts(), aeronDir.getAbsolutePath());
            fail("Expected refusal");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains(aeronDir.getAbsolutePath()));
            assertTrue(expected.getCause() instanceof ActiveDriverException);
        }
        assertEquals(1, attempts.get());
        assertTrue(new File(aeronDir, "driver.lock").exists());
    }

    @Test
    public void launchMediaDriverPropagatesNonMatchingTimeouts() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        AeronClusterLauncher launcher = newLauncher(contexts -> {
            attempts.incrementAndGet();
            throw new DriverTimeoutException("Some other timeout");
        });

        try {
            launcher.launchMediaDriver(contexts(), tempFolder.getRoot().getAbsolutePath());
            fail("Expected DriverTimeoutException");
        } catch (DriverTimeoutException expected) {
            assertEquals(1, attempts.get());
        }
    }

    @Test
    public void launchMediaDriverRefusesActiveArchiveMarkFileWithoutDeletingIt() throws Exception {
        File archiveDir = tempFolder.newFolder("cluster-node", "archive");
        File archiveMark = new File(archiveDir, "archive-mark.dat");
        Files.write(archiveMark.toPath(), "stale".getBytes(StandardCharsets.UTF_8));

        AtomicInteger attempts = new AtomicInteger();
        AeronClusterLauncher launcher = newLauncher(contexts -> {
            attempts.incrementAndGet();
            throw new IllegalStateException("active mark file detected: " + archiveMark.getAbsolutePath());
        });

        try {
            launcher.launchMediaDriver(contexts(), tempFolder.getRoot().getAbsolutePath());
            fail("Expected refusal");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains(archiveDir.getAbsolutePath()));
        }
        assertEquals(1, attempts.get());
        assertTrue(archiveMark.exists());
    }

    @Test
    public void launchMediaDriverUsesFreshContextsForRetryAttempts() throws Exception {
        List<AeronClusterContextFactory.LaunchContexts> attempts = new ArrayList<>();
        ClusteredMediaDriver expected = mock(ClusteredMediaDriver.class);
        AeronClusterLauncher launcher = newLauncher(contexts -> {
            attempts.add(contexts);
            if (attempts.size() == 1) {
                throw new DriverTimeoutException("FATAL - CnC file is created but not initialised");
            }
            return expected;
        });

        AeronClusterContextFactory.LaunchContexts original = contexts();
        ClusteredMediaDriver resolved = launcher.launchMediaDriver(original, tempFolder.getRoot().getAbsolutePath());

        assertSame(expected, resolved);
        assertEquals(2, attempts.size());
        assertTrue(attempts.get(0) != attempts.get(1));
        assertTrue(attempts.get(0).mediaDriverContext != attempts.get(1).mediaDriverContext);
        assertTrue(attempts.get(0).archiveContext != attempts.get(1).archiveContext);
        assertTrue(attempts.get(0).consensusModuleContext != attempts.get(1).consensusModuleContext);
        assertTrue(attempts.get(0).clusteredServiceContext != attempts.get(1).clusteredServiceContext);
        assertTrue(original.mediaDriverContext != attempts.get(0).mediaDriverContext);
        assertTrue(original.archiveContext != attempts.get(0).archiveContext);
        assertTrue(original.consensusModuleContext != attempts.get(0).consensusModuleContext);
        assertTrue(original.clusteredServiceContext != attempts.get(0).clusteredServiceContext);
    }

    @Test
    public void launchMediaDriverStopsAtTheFirstActiveMarkFile() throws Exception {
        File archiveDir = tempFolder.newFolder("cluster-node-marks", "archive");
        File clusterDir = new File(tempFolder.getRoot(), "cluster-node-marks/cluster");
        assertTrue(clusterDir.mkdirs());
        File archiveMark = new File(archiveDir, "archive-mark.dat");
        File clusterMark = new File(clusterDir, "cluster-mark.dat");
        Files.write(archiveMark.toPath(), "stale".getBytes(StandardCharsets.UTF_8));
        Files.write(clusterMark.toPath(), "stale".getBytes(StandardCharsets.UTF_8));

        AtomicInteger attempts = new AtomicInteger();
        AeronClusterLauncher launcher = newLauncher(contexts -> {
            int attempt = attempts.getAndIncrement();
            if (attempt == 0) {
                throw new IllegalStateException("active mark file detected: " + archiveMark.getAbsolutePath());
            }
            throw new IllegalStateException("active mark file detected: " + clusterMark.getAbsolutePath());
        });

        try {
            launcher.launchMediaDriver(contexts(), tempFolder.getRoot().getAbsolutePath());
            fail("Expected refusal");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains(archiveMark.getAbsolutePath()));
        }
        assertEquals(1, attempts.get());
        assertTrue(archiveMark.exists());
        assertTrue(clusterMark.exists());
    }

    @Test
    public void launchClusteredServiceContainerRefusesActiveMarkFilesWithoutDeletingThem() throws Exception {
        File clusterDir = tempFolder.newFolder("service-container-cluster");
        File clusterMark = new File(clusterDir, "cluster-mark.dat");
        File serviceMark = new File(clusterDir, "cluster-mark-service-0.dat");
        Files.write(clusterMark.toPath(), "stale".getBytes(StandardCharsets.UTF_8));
        Files.write(serviceMark.toPath(), "stale".getBytes(StandardCharsets.UTF_8));

        AtomicInteger attempts = new AtomicInteger();
        AeronClusterLauncher launcher = newLauncher(
            contexts -> mock(ClusteredMediaDriver.class),
            context -> {
                int attempt = attempts.getAndIncrement();
                if (attempt == 0) {
                    throw new IllegalStateException("active mark file detected: " + serviceMark.getAbsolutePath());
                }
                throw new IllegalStateException("active mark file detected: " + clusterMark.getAbsolutePath());
            }
        );

        try {
            launcher.launchClusteredServiceContainer(contexts());
            fail("Expected refusal");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains(clusterDir.getAbsolutePath()));
        }
        assertEquals(1, attempts.get());
        assertTrue(clusterMark.exists());
        assertTrue(serviceMark.exists());
    }

    @Test
    public void launchClusteredServiceContainerUsesAFreshContext() throws Exception {
        List<ClusteredServiceContainer.Context> attempts = new ArrayList<>();
        ClusteredServiceContainer expected = mock(ClusteredServiceContainer.class);
        AeronClusterLauncher launcher = newLauncher(
            contexts -> mock(ClusteredMediaDriver.class),
            context -> {
                attempts.add(context);
                return expected;
            }
        );

        AeronClusterContextFactory.LaunchContexts original = contexts();
        ClusteredServiceContainer resolved = launcher.launchClusteredServiceContainer(original);

        assertSame(expected, resolved);
        assertEquals(1, attempts.size());
        assertTrue(original.clusteredServiceContext != attempts.get(0));
    }

    private static Integer invokeParsePositiveInt(String value) throws Exception {
        Method method = AeronClusterLauncher.class.getDeclaredMethod("parsePositiveInt", String.class);
        method.setAccessible(true);
        return (Integer) method.invoke(null, value);
    }

    private static AeronClusterLauncher newLauncher(AeronClusterLauncher.LaunchInvoker launchInvoker) {
        return newLauncher(launchInvoker, context -> mock(ClusteredServiceContainer.class));
    }

    private static AeronClusterLauncher newLauncher(
            AeronClusterLauncher.LaunchInvoker launchInvoker,
            AeronClusterLauncher.ContainerLaunchInvoker containerLaunchInvoker) {
        return new AeronClusterLauncher(
            0,
            List.of("node-0"),
            new File("."),
            mock(ClusteredService.class),
            mock(AeronClusterAddressResolver.class),
            mock(AeronClusterErrorPolicy.class),
            launchInvoker,
            containerLaunchInvoker
        );
    }

    private static AeronClusterContextFactory.LaunchContexts contexts() {
        return new AeronClusterContextFactory.LaunchContexts(
            new MediaDriver.Context(),
            new Archive.Context(),
            new ConsensusModule.Context(),
            new ClusteredServiceContainer.Context()
        );
    }

    private static int invokeGetPositiveIntProperty(String key, int defaultValue) throws Exception {
        Method method = AeronClusterLauncher.class.getDeclaredMethod("getPositiveIntProperty", String.class, int.class);
        method.setAccessible(true);
        return (Integer) method.invoke(null, key, defaultValue);
    }

    private static String invokeGetHostname(AeronClusterLauncher launcher) throws Exception {
        Method method = AeronClusterLauncher.class.getDeclaredMethod("getHostname");
        method.setAccessible(true);
        return (String) method.invoke(launcher);
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Object getField(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }
}
