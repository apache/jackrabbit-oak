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

package org.apache.jackrabbit.oak.segment.file;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.segment.file.tar.GCGeneration.newGCGeneration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.file.GCJournal.GCJournalEntry;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class GcJournalTest {

    @Rule
    public final TemporaryFolder segmentFolder = new TemporaryFolder(new File("target"));

    @Test
    public void tarGcJournal() throws Exception {
        File directory = segmentFolder.newFolder();
        GCJournal gc = new GCJournal(directory);

        gc.persist(0, 100, newGCGeneration(1, 0, false), 50, RecordId.NULL.toString10());
        GCJournalEntry e0 = gc.read();
        assertEquals(100, e0.getRepoSize());
        assertEquals(0, e0.getReclaimedSize());
        assertEquals(50, e0.getNodes());
        assertEquals(RecordId.NULL.toString10(), e0.getRoot());

        gc.persist(0, 250, newGCGeneration(2, 0, false), 75, RecordId.NULL.toString());
        GCJournalEntry e1 = gc.read();
        assertEquals(250, e1.getRepoSize());
        assertEquals(0, e1.getReclaimedSize());
        assertEquals(75, e1.getNodes());
        assertEquals(RecordId.NULL.toString(), e1.getRoot());

        gc.persist(50, 200, newGCGeneration(3, 0, false), 90, "foo");
        GCJournalEntry e2 = gc.read();
        assertEquals(200, e2.getRepoSize());
        assertEquals(50, e2.getReclaimedSize());
        assertEquals(90, e2.getNodes());
        assertEquals("foo", e2.getRoot());

        // same gen
        gc.persist(75, 300, newGCGeneration(3, 0, false), 125, "bar");
        GCJournalEntry e3 = gc.read();
        assertEquals(200, e3.getRepoSize());
        assertEquals(50, e3.getReclaimedSize());
        assertEquals(90, e3.getNodes());
        assertEquals("foo", e2.getRoot());

        Collection<GCJournalEntry> all = gc.readAll();
        assertEquals(all.size(), 3);

        File file = new File(directory, GCJournal.GC_JOURNAL);
        assertTrue(file.exists());
        List<String> allLines = Files.readAllLines(file.toPath(), UTF_8);
        assertEquals(allLines.size(), 3);
    }

    @Test
    public void testGCGeneration() throws Exception {
        GCJournal out = new GCJournal(segmentFolder.getRoot());
        out.persist(1, 100, newGCGeneration(1, 2, false), 50, RecordId.NULL.toString());
        GCJournal in = new GCJournal(segmentFolder.getRoot());
        assertEquals(newGCGeneration(1, 2, false), in.read().getGcGeneration());
    }

    @Test
    public void testGCGenerationCompactedFlagCleared() throws Exception {
        GCJournal out = new GCJournal(segmentFolder.getRoot());
        out.persist(1, 100, newGCGeneration(1, 2, true), 50, RecordId.NULL.toString());
        GCJournal in = new GCJournal(segmentFolder.getRoot());
        assertEquals(newGCGeneration(1, 2, false), in.read().getGcGeneration());
    }

    @Test
    public void testReadOak16GCLog() throws IOException {
        createOak16GCLog();
        GCJournal gcJournal = new GCJournal(segmentFolder.getRoot());
        GCJournalEntry entry = gcJournal.read();
        assertEquals(45919825920L, entry.getRepoSize());
        assertEquals(41394306048L, entry.getReclaimedSize());
        assertEquals(1493819563098L, entry.getTs());
        assertEquals(newGCGeneration(1, 1, false), entry.getGcGeneration());
        assertEquals(42, entry.getNodes());
        assertEquals(RecordId.NULL.toString10(), entry.getRoot());
    }

    @Test
    public void testUpdateOak16GCLog() throws IOException {
        createOak16GCLog();
        GCJournal gcJournal = new GCJournal(segmentFolder.getRoot());
        gcJournal.persist(75, 300, newGCGeneration(3, 0, false), 125, "bar");

        ArrayList<GCJournalEntry> entries = newArrayList(gcJournal.readAll());
        assertEquals(2, entries.size());

        GCJournalEntry entry = entries.get(0);
        assertEquals(45919825920L, entry.getRepoSize());
        assertEquals(41394306048L, entry.getReclaimedSize());
        assertEquals(1493819563098L, entry.getTs());
        assertEquals(newGCGeneration(1, 1, false), entry.getGcGeneration());
        assertEquals(42, entry.getNodes());
        assertEquals(RecordId.NULL.toString10(), entry.getRoot());

        entry = entries.get(1);
        assertEquals(300, entry.getRepoSize());
        assertEquals(75, entry.getReclaimedSize());
        assertEquals(newGCGeneration(3, 0, false), entry.getGcGeneration());
        assertEquals(125, entry.getNodes());
        assertEquals("bar", entry.getRoot());
    }

    private void createOak16GCLog() throws IOException {
        try (InputStream source = GcJournalTest.class.getResourceAsStream("oak-1.6-gc.log")) {
            try (FileOutputStream target = new FileOutputStream(segmentFolder.newFile("gc.log"))) {
                IOUtils.copy(source, target);
            }
        }
    }

}
