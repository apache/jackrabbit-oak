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

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AeronClusterStartupPreflightTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @After
    public void clearProperties() {
        System.clearProperty("aeron.delete.dirs.on.startup");
        System.clearProperty("aeron.dir.name");
    }

    @Test
    public void runDeletesConfiguredAeronDirectoryWhenStartupCleanupEnabled() throws Exception {
        File configuredAeronDir = tempFolder.newFolder("configured-aeron-dir");
        Files.write(
            new File(configuredAeronDir, "cnc.dat").toPath(),
            "stale".getBytes(StandardCharsets.UTF_8)
        );

        System.setProperty("aeron.delete.dirs.on.startup", "true");
        System.setProperty("aeron.dir.name", configuredAeronDir.getAbsolutePath());

        CrashHandler crashHandler = mock(CrashHandler.class);
        AeronClusterStartupPreflight.PreflightResult result =
            new AeronClusterStartupPreflight(1, crashHandler)
                .run(new File(tempFolder.getRoot(), "driver").getAbsolutePath());

        assertFalse(configuredAeronDir.exists());
        assertFalse(result.hasCrashMarkers);
        assertFalse(result.staleDriverDirectoryDetected);
        assertFalse(result.forceBootstrap);
    }

    @Test
    public void runReportsCrashMarkersAndLeavesTheDriverDirectoryToTheMediaDriver() throws Exception {
        File staleDriverDir = tempFolder.newFolder("stale-driver");
        Files.write(
            new File(staleDriverDir, "driver.lock").toPath(),
            "lock".getBytes(StandardCharsets.UTF_8)
        );

        CrashHandler crashHandler = mock(CrashHandler.class);
        when(crashHandler.hasCrashed()).thenReturn(true);
        when(crashHandler.getState()).thenReturn("node-crash-2");
        when(crashHandler.shouldForceBootstrap()).thenReturn(true);

        AeronClusterStartupPreflight.PreflightResult result =
            new AeronClusterStartupPreflight(2, crashHandler).run(staleDriverDir.getAbsolutePath());

        assertTrue(result.hasCrashMarkers);
        assertTrue(result.staleDriverDirectoryDetected);
        assertTrue(result.forceBootstrap);
        assertTrue(new File(staleDriverDir, "driver.lock").exists());
    }

    @Test
    public void runLeavesCleanStateAloneWhenNoCrashMarkersOrDriverDirectory() throws Exception {
        CrashHandler crashHandler = mock(CrashHandler.class);
        when(crashHandler.hasCrashed()).thenReturn(false);
        when(crashHandler.shouldForceBootstrap()).thenReturn(false);

        AeronClusterStartupPreflight.PreflightResult result =
            new AeronClusterStartupPreflight(0, crashHandler)
                .run(new File(tempFolder.getRoot(), "missing-driver").getAbsolutePath());

        assertFalse(result.hasCrashMarkers);
        assertFalse(result.staleDriverDirectoryDetected);
        assertFalse(result.forceBootstrap);
    }

    @Test
    public void runIgnoresEmptyDriverDirectory() throws Exception {
        File emptyDriverDir = tempFolder.newFolder("empty-driver");

        CrashHandler crashHandler = mock(CrashHandler.class);
        when(crashHandler.hasCrashed()).thenReturn(false);
        when(crashHandler.shouldForceBootstrap()).thenReturn(false);

        AeronClusterStartupPreflight.PreflightResult result =
            new AeronClusterStartupPreflight(0, crashHandler).run(emptyDriverDir.getAbsolutePath());

        assertTrue(emptyDriverDir.exists());
        assertFalse(result.hasCrashMarkers);
        assertFalse(result.staleDriverDirectoryDetected);
        assertFalse(result.forceBootstrap);
    }
}
