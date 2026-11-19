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

package org.apache.jackrabbit.oak.segment.tool;

import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.defaultGCOptions;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.UUID;

import org.apache.jackrabbit.oak.segment.SegmentNodeBuilder;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.tar.TarPersistence;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileWriter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class GcJournalCheckTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private File storeDir;

    private final StringWriter out = new StringWriter();

    private final StringWriter err = new StringWriter();

    @Before
    public void setUp() {
        storeDir = folder.getRoot();
    }

    @Test
    public void neverCompactedStoreHasNothingToCheck() throws Exception {
        writeAndCompact(false, false);

        assertEquals(GcJournalCheck.CONSISTENT, check(storeDir));
        assertTrue(out.toString().contains("nothing to check"));
    }

    @Test
    public void compactedStoreWithJournalIsConsistent() throws Exception {
        writeAndCompact(true, true);

        assertEquals(GcJournalCheck.CONSISTENT, check(storeDir));
        String output = out.toString();
        assertTrue(output.contains("Checking journal.log"));
        assertTrue(output.contains("Checking gc.log"));
        assertTrue(output.contains("gc.log is consistent with the latest journal.log revision"));
        assertEquals("", err.toString());
    }

    @Test
    public void compactedStoreWithoutJournalEntriesIsInconsistent() throws Exception {
        writeAndCompact(true, false);

        assertEquals(GcJournalCheck.INCONSISTENT, check(storeDir));
        assertTrue(err.toString().contains("gc.log has no entries"));
    }

    @Test
    public void journalBehindHeadIsStale() throws Exception {
        writeAndCompact(true, true);
        writeAndCompact(true, false);

        assertEquals(GcJournalCheck.INCONSISTENT, check(storeDir));
        assertTrue(err.toString().contains("gc.log is stale"));
    }

    @Test
    public void malformedLatestEntryIsInconsistent() throws Exception {
        writeAndCompact(true, true);
        appendToJournal("garbage");

        assertEquals(GcJournalCheck.INCONSISTENT, check(storeDir));
        assertTrue(err.toString().contains("malformed"));
    }

    @Test
    public void invalidRootIsInconsistent() throws Exception {
        writeAndCompact(true, true);
        appendToJournal("1,0,0,1,1,0,not-a-record-id");

        assertEquals(GcJournalCheck.INCONSISTENT, check(storeDir));
        assertTrue(err.toString().contains("invalid record id"));
    }

    @Test
    public void missingRootSegmentIsInconsistent() throws Exception {
        writeAndCompact(true, true);
        appendToJournal("1,0,0,1,1,0," + UUID.fromString("11111111-2222-3333-a444-555555555555") + ":0");

        assertEquals(GcJournalCheck.INCONSISTENT, check(storeDir));
        assertTrue(err.toString().contains("missing segment"));
    }

    @Test
    public void invalidStoreDirectoryIsInconsistent() {
        assertEquals(GcJournalCheck.INCONSISTENT, check(new File(storeDir, "does-not-exist")));
        assertTrue(err.toString().contains("Unable to check gc.log"));
    }

    @Test
    public void missingLatestJournalRevisionIsInconsistent() throws Exception {
        writeAndCompact(true, true);
        appendToJournalLog("11111111-2222-3333-a444-555555555555:0 root " + System.currentTimeMillis());

        assertEquals(GcJournalCheck.INCONSISTENT, check(storeDir));
        assertTrue(err.toString().contains("Latest journal.log revision 11111111-2222-3333-a444-555555555555:0 is not accessible: missing segment"));
    }

    @Test
    public void invalidLatestJournalRevisionIsInconsistent() throws Exception {
        writeAndCompact(true, true);
        appendToJournalLog("not-a-record-id root " + System.currentTimeMillis());

        assertEquals(GcJournalCheck.INCONSISTENT, check(storeDir));
        assertTrue(err.toString().contains("Latest journal.log revision not-a-record-id is not accessible: invalid record id"));
    }

    private int check(File path) {
        return GcJournalCheck.builder()
            .withPath(path)
            .withOutWriter(new PrintWriter(out, true))
            .withErrWriter(new PrintWriter(err, true))
            .build()
            .run();
    }

    private void writeAndCompact(boolean compact, boolean cleanup) throws Exception {
        try (FileStore store = fileStoreBuilder(storeDir)
            .withGCOptions(defaultGCOptions().setOffline())
            .build()) {
            SegmentNodeState base = store.getHead();
            SegmentNodeBuilder builder = base.builder();
            builder.setProperty("key", UUID.randomUUID().toString());
            store.getRevisions().setHead(base.getRecordId(), builder.getNodeState().getRecordId());
            store.flush();

            if (compact) {
                assertTrue("compactFull() should succeed", store.compactFull());
            }
            if (cleanup) {
                store.cleanup();
            }
        }
    }

    private void appendToJournal(String line) throws IOException {
        new TarPersistence(storeDir).getGCJournalFile().writeLine(line);
    }

    private void appendToJournalLog(String line) throws IOException {
        try (JournalFileWriter writer = new TarPersistence(storeDir).getJournalFile().openJournalWriter()) {
            writer.writeLine(line);
        }
    }

}
