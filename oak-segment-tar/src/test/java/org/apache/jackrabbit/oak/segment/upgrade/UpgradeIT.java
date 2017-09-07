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
 *
 */

package org.apache.jackrabbit.oak.segment.upgrade;

import static com.google.common.collect.Iterables.transform;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.jackrabbit.oak.segment.SegmentVersion.V_12;
import static org.apache.jackrabbit.oak.segment.SegmentVersion.V_13;
import static org.apache.jackrabbit.oak.segment.data.SegmentData.newSegmentData;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.apache.jackrabbit.oak.segment.file.ManifestChecker.newManifestChecker;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.segment.SegmentVersion;
import org.apache.jackrabbit.oak.segment.data.SegmentData;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.tar.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.file.tar.TarFiles;
import org.apache.jackrabbit.oak.segment.tool.Compact;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class UpgradeIT {

    private final File upgradeItHome = new File("target/upgrade-it");

    @Rule
    public TemporaryFolder fileStoreHome = new TemporaryFolder(upgradeItHome);

    /**
     * Launch a groovy script in an Oak 1.6. console to initialise the upgrade
     * source. See pom.xml for how these files are placed under target/upgrade-it.
     */
    @Before
    public void setup() throws IOException, InterruptedException {
        Process oakConsole = new ProcessBuilder(
                "java", "-jar", "oak-run.jar",
                "console", fileStoreHome.getRoot().getAbsolutePath(), "--read-write",
                ":load create16store.groovy")
                .directory(upgradeItHome)
                .redirectError(Redirect.INHERIT)
                .redirectOutput(Redirect.INHERIT)
                .redirectInput(Redirect.INHERIT)
                .start();

        assertTrue(
                "Timeout while creating the source repository",
                oakConsole.waitFor(2, MINUTES));
    }

    @Test
    public void openUpgradesStore() throws IOException, InvalidFileStoreVersionException {
        checkStoreVersion(1);
        fileStoreBuilder(fileStoreHome.getRoot())
                .build()
                .close();
        checkStoreVersion(2);
    }

    @Test
    public void openReadonlyDoesNotUpgradeStore() throws IOException, InvalidFileStoreVersionException {
        checkStoreVersion(1);
        fileStoreBuilder(fileStoreHome.getRoot())
                .buildReadOnly()
                .close();
        checkStoreVersion(1);
    }

    @Test
    public void offRCUpgradesSegments() throws IOException, InvalidFileStoreVersionException {
        checkSegmentVersion(V_12);
        checkStoreVersion(1);
        Compact.builder()
                .withPath(fileStoreHome.getRoot())
                .withMmap(true)
                .withForce(true)
                .build()
                .run();
        checkStoreVersion(2);
        checkSegmentVersion(V_13);
    }

    private void checkStoreVersion(int version) throws IOException, InvalidFileStoreVersionException {
        newManifestChecker(new File(fileStoreHome.getRoot(), "/manifest"),
                true, version, version).checkManifest();
    }

    private void checkSegmentVersion(@Nonnull SegmentVersion version) throws IOException {
        try (TarFiles tarFiles = TarFiles.builder()
                .withDirectory(fileStoreHome.getRoot())
                .withTarRecovery((_1, _2, _3) -> fail("Unexpected recovery"))
                .withIOMonitor(new IOMonitorAdapter())
                .withReadOnly()
                .build()) {

            for (SegmentData segmentData : getSegments(tarFiles)) {
                SegmentVersion actualVersion = SegmentVersion.fromByte(segmentData.getVersion());
                assertEquals(
                        format("Segment version mismatch. Expected %s, found %s", version, actualVersion),
                        version, actualVersion);
            }
        }
    }

    private static Iterable<SegmentData> getSegments(@Nonnull TarFiles tarFiles) {
        return transform(
                tarFiles.getSegmentIds(),
                uuid -> newSegmentData(tarFiles.readSegment(
                    uuid.getMostSignificantBits(),
                    uuid.getLeastSignificantBits())));
    }
}
