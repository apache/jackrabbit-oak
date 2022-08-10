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
package org.apache.jackrabbit.oak.segment.spi.persistence.split;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.tar.TarPersistence;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.testutils.NodeStoreTestHarness;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SplitPersistenceTest {

    private static final Logger LOG = LoggerFactory.getLogger(SplitPersistenceTest.class);

    @Rule
    public NodeStoreTestHarness.Rule harnesses = new NodeStoreTestHarness.Rule();

    private NodeStoreTestHarness splitHarness;

    private SegmentArchiveManager splitArchiveManager;

    private TarPersistence rwPersistence;

    private @NotNull List<String> roArchives;

    @Before
    public void setUp() throws IOException, InvalidFileStoreVersionException, CommitFailedException {
        final NodeStoreTestHarness roHarness = harnesses.createHarnessWithFolder(TarPersistence::new);
        initializeBaseSetup(roHarness, "1");
        roHarness.startNewTarFile(); // data00000a.tar
        modifyNodeStore(roHarness, "2");
        roHarness.setReadOnly(); // data00001a.tar
        roArchives = roHarness.createArchiveManager().listArchives();

        splitHarness = harnesses.createHarnessWithFolder(folder -> {
            try {
                rwPersistence = new TarPersistence(folder);
                return new SplitPersistence(roHarness.getPersistence(), rwPersistence);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        splitArchiveManager = splitHarness.createArchiveManager();

        modifyNodeStore(splitHarness, "2");
        splitHarness.startNewTarFile(); // data00002a.tar
        modifyNodeStore(splitHarness, "3");
        splitHarness.startNewTarFile(); // data00003a.tar
    }

    @Test
    public void archiveManager_exists() {
        assertTrue(splitArchiveManager.exists("data00000a.tar"));
        assertTrue(splitArchiveManager.exists("data00001a.tar"));
        assertTrue(splitArchiveManager.exists("data00002a.tar"));
        assertTrue(splitArchiveManager.exists("data00003a.tar"));
        assertFalse(splitArchiveManager.exists("data00004a.tar"));
    }

    @Test
    public void archiveManager_listArchives() throws IOException {
        assertEquals(
                splitArchiveManager.listArchives(),
                asList("data00000a.tar", "data00001a.tar", "data00002a.tar", "data00003a.tar"));
    }

    @Test
    public void segmentFilesExist() {
        assertTrue(splitHarness.getPersistence().segmentFilesExist());
    }

    @Test
    public void getJournalFile() throws IOException {
        try (final JournalFileReader rwJournalFileReader = rwPersistence.getJournalFile().openJournalReader();
             final JournalFileReader splitJournalFileReader = splitHarness.getPersistence().getJournalFile().openJournalReader()) {
            assertEquals(rwJournalFileReader.readLine(), splitJournalFileReader.readLine());
        }
    }

    @Test
    public void getGCJournalFile() throws IOException {
        assertEquals(rwPersistence.getGCJournalFile().readLines(), splitHarness.getPersistence().getGCJournalFile().readLines());
    }

    @Test
    public void getManifestFile() throws IOException {
        assertEquals(rwPersistence.getManifestFile().load(), splitHarness.getPersistence().getManifestFile().load());
    }

    @Test
    public void gcOnlyCompactsRWStore() throws IOException, CommitFailedException, InvalidFileStoreVersionException, InterruptedException {
        for (int i = 0; i < 3; i++) {
            createGarbage();
            final List<String> archivesBeforeGC = splitArchiveManager.listArchives();
            LOG.info("archives before gc: {}", archivesBeforeGC);
            assertTrue("GC should run successfully", splitHarness.runGC());
            final List<String> archivesAfterGC = splitArchiveManager.listArchives();
            LOG.info("archives after gc: {}", archivesAfterGC);
            MatcherAssert.assertThat(archivesAfterGC, CoreMatchers.hasItems(roArchives.toArray(new String[0])));
        }
    }

    private static void initializeBaseSetup(NodeStoreTestHarness harness, String version) throws CommitFailedException, IOException, InvalidFileStoreVersionException {
        harness.writeAndCommit(builder -> {
            builder.child("foo").child("bar").setProperty("version", "v" + version);
            builder.child("foo").child("bar").setProperty("fullVersion", version + ".0.0");
            builder.child("foo").setProperty("fooVersion", "version_" + version);
            builder.child("foo").setProperty("fooOverwriteVersion", "version_" + version);
            builder.child("foo").child("to_be_deleted").setProperty("version", "v" + version);
        });
        harness.startNewTarFile();
    }

    private static void modifyNodeStore(NodeStoreTestHarness harness, String version) throws CommitFailedException {
        harness.writeAndCommit(builder -> {
            builder.child("foo").child("bar").setProperty("version", "v" + version);
            builder.child("foo").setProperty("fooOverwriteVersion", "version_" + version);
            builder.child("foo").setProperty("splitVersion", "version_" + version);
            builder.child("foo").child("to_be_deleted").remove();
        });
    }

    private void createGarbage() throws CommitFailedException, IOException, InvalidFileStoreVersionException {
        for (int i = 0; i < 100; i++) {
            modifyNodeStore(splitHarness, "" + i);
            if (i % 50 == 0) {
                splitHarness.startNewTarFile();
            }
        }
    }
}