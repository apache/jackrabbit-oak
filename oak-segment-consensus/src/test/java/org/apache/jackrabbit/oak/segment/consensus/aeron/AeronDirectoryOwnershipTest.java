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

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.codecs.mark.ClusterComponentType;
import io.aeron.cluster.service.ClusterMarkFile;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.SystemEpochClock;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/**
 * The media driver directory and the cluster/archive mark files belong to whichever process is alive in them.
 * Startup refuses to run over a live owner and never deletes a mark file; a dead media directory is cleared by
 * the media driver itself, which saves its error log first.
 */
public class AeronDirectoryOwnershipTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void liveMediaDriverDirectoryIsRefusedNotDeleted() throws Exception {
        File mediaDir = new File(tempFolder.getRoot(), "media");
        try (MediaDriver owner = MediaDriver.launch(mediaDriverContext(mediaDir))) {
            new AeronClusterStartupPreflight(0, mock(CrashHandler.class)).run(mediaDir.getAbsolutePath());
            assertTrue("preflight deleted a live driver's directory", new File(mediaDir, "cnc.dat").exists());

            AtomicInteger attempts = new AtomicInteger();
            AeronClusterLauncher launcher = launcher(contexts -> {
                attempts.incrementAndGet();
                MediaDriver.launch(contexts.mediaDriverContext).close();
                return mock(ClusteredMediaDriver.class);
            }, context -> mock(ClusteredServiceContainer.class));
            try {
                launcher.launchMediaDriver(contexts(mediaDir), mediaDir.getAbsolutePath());
                fail("started over a live media driver");
            } catch (IllegalStateException expected) {
                assertTrue(expected.getMessage(), expected.getMessage().contains(mediaDir.getAbsolutePath()));
                assertTrue(expected.getMessage(), expected.getMessage().contains("active"));
            }
            assertEquals(1, attempts.get());
            assertTrue(new File(mediaDir, "cnc.dat").exists());
        }
    }

    @Test
    public void deadMediaDriverDirectoryIsClearedAtLaunchAndItsErrorLogSaved() throws Exception {
        File mediaDir = new File(tempFolder.getRoot(), "media");
        try (MediaDriver previousRun = MediaDriver.launch(mediaDriverContext(mediaDir))) {
            previousRun.context().countedErrorHandler().onError(new IllegalStateException("previous-run-failure"));
        }
        File leftover = new File(mediaDir, "leftover.dat");
        Files.write(leftover.toPath(), "x".getBytes(StandardCharsets.UTF_8));

        new AeronClusterStartupPreflight(0, mock(CrashHandler.class)).run(mediaDir.getAbsolutePath());
        assertTrue("preflight must leave the decision to the media driver", leftover.exists());

        try (MediaDriver next = MediaDriver.launch(mediaDriverContext(mediaDir))) {
            assertFalse(leftover.exists());
            assertTrue(new File(mediaDir, "cnc.dat").exists());
        }
        File[] savedErrorLogs = tempFolder.getRoot().listFiles((dir, name) -> name.startsWith("media-")
            && name.endsWith("-error.log"));
        assertEquals(1, savedErrorLogs.length);
        assertTrue(new String(Files.readAllBytes(savedErrorLogs[0].toPath()), StandardCharsets.US_ASCII)
            .contains("previous-run-failure"));
    }

    @Test
    public void mediaDriverKeepsItsLivenessCheckEvenWhenDeleteOnStartIsConfigured() {
        System.setProperty("aeron.dir.delete.on.start", "true");
        try {
            assertFalse(mediaDriverContext(new File(tempFolder.getRoot(), "media")).dirDeleteOnStart());
        } finally {
            System.clearProperty("aeron.dir.delete.on.start");
        }
    }

    @Test
    public void activeClusterMarkFileIsRefusedAndNeverDeleted() throws Exception {
        File clusterDir = tempFolder.newFolder("node", "cluster");
        File markFile = new File(clusterDir, ClusterMarkFile.FILENAME);
        try (ClusterMarkFile owner = activeMarkFile(markFile)) {
            AtomicInteger attempts = new AtomicInteger();
            AeronClusterLauncher launcher = launcher(contexts -> {
                attempts.incrementAndGet();
                activeMarkFile(markFile).close();
                return mock(ClusteredMediaDriver.class);
            }, context -> {
                attempts.incrementAndGet();
                activeMarkFile(markFile).close();
                return mock(ClusteredServiceContainer.class);
            });

            assertRefused(() -> launcher.launchMediaDriver(contexts(tempFolder.getRoot()), "unused"), clusterDir);
            assertRefused(() -> launcher.launchClusteredServiceContainer(contexts(tempFolder.getRoot())), clusterDir);
            assertEquals("one attempt per launch, no retries", 2, attempts.get());
            assertTrue(markFile.exists());
        }
    }

    @Test
    public void staleMarkFileMessageNeverDeletesTheNamedFile() throws Exception {
        File archiveMark = new File(tempFolder.newFolder("node", "archive"), "archive-mark.dat");
        Files.write(archiveMark.toPath(), "owned".getBytes(StandardCharsets.UTF_8));
        AeronClusterLauncher launcher = launcher(contexts -> {
            throw new IllegalStateException("active mark file detected: " + archiveMark.getAbsolutePath());
        }, context -> {
            throw new IllegalStateException("active mark file detected: " + archiveMark.getAbsolutePath());
        });

        assertRefused(() -> launcher.launchMediaDriver(contexts(tempFolder.getRoot()), "unused"),
            archiveMark.getParentFile());
        assertRefused(() -> launcher.launchClusteredServiceContainer(contexts(tempFolder.getRoot())),
            archiveMark.getParentFile());
        assertTrue(archiveMark.exists());
    }

    private static void assertRefused(Runnable launch, File owningDir) {
        try {
            launch.run();
            fail("started over an active mark file");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains(owningDir.getAbsolutePath()));
            assertTrue(expected.getMessage(), expected.getMessage().contains("never deleted"));
        }
    }

    private static ClusterMarkFile activeMarkFile(File file) {
        ClusterMarkFile markFile = new ClusterMarkFile(file, ClusterComponentType.CONSENSUS_MODULE,
            ClusterMarkFile.ERROR_BUFFER_MIN_LENGTH, SystemEpochClock.INSTANCE,
            ClusteredServiceContainer.Configuration.LIVENESS_TIMEOUT_MS, 4096);
        markFile.signalReady();
        markFile.updateActivityTimestamp(SystemEpochClock.INSTANCE.time());
        return markFile;
    }

    private static MediaDriver.Context mediaDriverContext(File mediaDir) {
        return contexts(mediaDir).mediaDriverContext.clone();
    }

    private static AeronClusterContextFactory.LaunchContexts contexts(File mediaDir) {
        return AeronClusterContextFactory.create(
            0,
            mediaDir.getParentFile(),
            mock(ClusteredService.class),
            mediaDir.getAbsolutePath(),
            "127.0.0.1",
            Arrays.asList("127.0.0.1"),
            mock(ShutdownSignalBarrier.class),
            component -> { },
            131072,
            131072,
            65536,
            10_000,
            65536,
            new AeronClusterLauncher.SessionTimeoutConfig(TimeUnit.MINUTES.toNanos(1), "test"),
            throwable -> { },
            throwable -> { },
            throwable -> { }
        );
    }

    private static AeronClusterLauncher launcher(AeronClusterLauncher.LaunchInvoker driver,
                                                 AeronClusterLauncher.ContainerLaunchInvoker container) {
        return new AeronClusterLauncher(0, List.of("node-0"), new File("."), mock(ClusteredService.class),
            mock(AeronClusterAddressResolver.class), mock(AeronClusterErrorPolicy.class), driver, container);
    }
}
