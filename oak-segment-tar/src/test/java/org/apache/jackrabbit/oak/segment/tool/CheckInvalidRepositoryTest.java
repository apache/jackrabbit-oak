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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.List;

import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.JournalEntry;
import org.apache.jackrabbit.oak.segment.file.JournalReader;
import org.apache.jackrabbit.oak.segment.file.MockReadOnlyFileStore;
import org.apache.jackrabbit.oak.segment.file.tar.LocalJournalFile;
import org.apache.jackrabbit.oak.segment.file.tooling.ConsistencyChecker;
import org.apache.jackrabbit.oak.segment.file.tooling.ConsistencyChecker.ConsistencyCheckResult;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link Check} assuming an invalid repository.
 */
public class CheckInvalidRepositoryTest extends CheckRepositoryTestBase {

    private Output log;

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        super.addInvalidRevision();
        log = new Output();
    }

    @Test
    public void testInvalidRevisionFallbackOnValid() {
        int checkResult = check(b -> b
            .withFilterPaths(Set.of("/"))
        );

        assertCheckSucceeded(checkResult);
        assertExpectedOutput(log.outString(), List.of("Checked 7 nodes and 21 properties", "Path / is consistent",
            "Searched through 2 revisions"));

        // not sure whether first traversal will fail because of "/a" or "/z"
        assertExpectedOutput(log.errString(), List.of("Error while traversing /"));
    }

    @Test
    public void testPartialBrokenPathWithoutValidRevision() {
        int checkResult = check(b -> b
            .withFilterPaths(Set.of("/z"))
        );

        assertCheckFailed(checkResult);
        assertExpectedOutput(log.outString(), List.of("Checking head", "Checking checkpoints", "No good revision found"));
        assertExpectedOutput(log.errString(),
            List.of(
                "Error while traversing /z: java.lang.IllegalArgumentException: Segment reference out of bounds",
                "Path /z not found"));
    }

    @Test
    public void testPartialBrokenPathWithValidRevision() {
        int checkResult = check(b -> b
            .withFilterPaths(Set.of("/a"))
            .withCheckpoints(new HashSet<>())
        );

        assertCheckSucceeded(checkResult);
        assertExpectedOutput(log.outString(), List.of("Checked 1 nodes and 1 properties", "Path /a is consistent",
            "Searched through 2 revisions"));
        assertExpectedOutput(log.errString(), List.of(
            "Error while traversing /a: java.lang.IllegalArgumentException: Segment reference out of bounds"));
    }

    @Test
    public void testCorruptHeadWithValidCheckpoints() {
        int checkResult = check(b -> b
            .withFilterPaths(Set.of("/"))
        );

        assertCheckSucceeded(checkResult);
        assertExpectedOutput(log.outString(), List.of("Checking head", "Checking checkpoints",
            "Checked 7 nodes and 21 properties", "Path / is consistent", "Searched through 2 revisions and 2 checkpoints"));
        assertExpectedOutput(log.errString(), List.of(
            "Error while traversing /a: java.lang.IllegalArgumentException: Segment reference out of bounds"));
    }

    @Test
    public void testCorruptPathInCp1NoValidRevision() throws Exception {
        corruptPathFromCheckpoint();

        int checkResult = check(b -> b
            .withFilterPaths(Set.of("/b"))
            .withCheckpoints(Set.of(checkpoints.iterator().next()))
        );

        assertCheckFailed(checkResult);
        assertExpectedOutput(log.outString(), List.of("Searched through 2 revisions and 1 checkpoints", "No good revision found"));
        assertExpectedOutput(log.errString(), List.of(
            "Error while traversing /b: java.lang.IllegalArgumentException: Segment reference out of bounds"));
    }

    @Test
    public void testLargeJournal() throws IOException {
        File segmentStoreFolder = new File(temporaryFolder.getRoot().getAbsolutePath());
        File journalFile = new File(segmentStoreFolder, "journal.log");
        File largeJournalFile = temporaryFolder.newFile("journal.log.large");

        JournalReader journalReader = new JournalReader(new LocalJournalFile(journalFile));
        JournalEntry journalEntry = journalReader.next();

        String journalLine = journalEntry.getRevision() + " root " + journalEntry.getTimestamp() + "\n";

        for (int k = 0; k < 10000; k++) {
            FileUtils.writeStringToFile(largeJournalFile, journalLine, true);
        }

        int checkResult = check(b -> b
            .withPath(segmentStoreFolder)
            .withJournal(largeJournalFile)
            .withFilterPaths(Set.of("/"))
        );

        assertCheckFailed(checkResult);
        assertExpectedOutput(log.outString(), List.of("No good revision found"));
    }

    @Test
    public void testFailFast_withInvalidHead() {
        int checkResult = check(b -> b
            .withFilterPaths(Set.of("/"))
            .withFailFast(true)
        );

        assertCheckFailed(checkResult);
        assertExpectedOutput(log.outString(), List.of("Searched through 1 revisions and 2 checkpoints", "No good revision found"));
    }

    @Test
    public void testFailFast_withInvalidCheckpoints() {
        int checkResult = check(b -> b
            .withFilterPaths(Set.of("/b"))
            .withCheckpoints(Set.of("invalid-checkpoint-id"))
            .withFailFast(true)
        );

        assertCheckFailed(checkResult);
        assertExpectedOutput(log.outString(), List.of("Path /b is consistent", "No good revision found"));
        assertExpectedOutput(log.errString(), List.of("Checkpoint invalid-checkpoint-id not found in this revision!"));
    }

    @Test
    public void testFailFast_withSegmentNotFoundException() throws Exception {
        addMoreSegments(); // so that some segments are not loaded at initialization, but only when the check is performed

        File path = new File(temporaryFolder.getRoot().getAbsolutePath());
        File journalFile = new File(path, "journal.log");

        MockReadOnlyFileStore mockStore = MockReadOnlyFileStore.buildMock(path, journalFile);
        mockStore.failAfterReadSegmentCount(2);

        ConsistencyCheckResult result = checkConsistencyFailFast(mockStore, journalFile);

        assertNull(result.getOverallRevision());
        assertTrue(hasAnyHeadRevision(result));
        assertFalse(hasAnyCheckpointRevision(result));
    }

    @Test
    public void testFallbackToAnotherRevision_withSegmentNotFoundException() throws Exception {
        addMoreSegments(); // so that some segments are not loaded at initialization, but only when the check is performed

        File path = new File(temporaryFolder.getRoot().getAbsolutePath());
        File journalFile = new File(path, "journal.log");
        MockReadOnlyFileStore mockStore = MockReadOnlyFileStore.buildMock(path, journalFile);
        mockStore.failAfterReadSegmentCount(2);

        ConsistencyCheckResult result = checkConsistencyLenient(mockStore, journalFile);

        assertTrue(hasAnyHeadRevision(result));
        assertTrue(hasAnyCheckpointRevision(result));
    }

    @NotNull
    private ConsistencyCheckResult checkConsistencyFailFast(MockReadOnlyFileStore mockStore, File journalFile) throws IOException {
        return checkConsistency(mockStore, journalFile, true);
    }

    @NotNull
    private ConsistencyCheckResult checkConsistencyLenient(MockReadOnlyFileStore mockStore, File journalFile) throws IOException {
        return checkConsistency(mockStore, journalFile, false);
    }

    @NotNull
    private ConsistencyCheckResult checkConsistency(MockReadOnlyFileStore store, File journalFile, boolean failFast) throws IOException {
        return new ConsistencyChecker().checkConsistency(
            store, new JournalReader(new LocalJournalFile(journalFile)),
            true, checkpoints, Set.of("/b"), true, Integer.MAX_VALUE,
            failFast);
    }

    private void addMoreSegments() throws InvalidFileStoreVersionException, IOException, CommitFailedException {
        FileStore fileStore = FileStoreBuilder.fileStoreBuilder(temporaryFolder.getRoot()).withMaxFileSize(256)
            .withSegmentCacheSize(64).build();

        SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
        NodeBuilder builder = nodeStore.getRoot().builder();

        // add a new property value to existing child "b"
        addChildWithBlobProperties(nodeStore, builder, "y", 5);
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        fileStore.close();
    }

    private static boolean hasAnyHeadRevision(ConsistencyCheckResult result) {
        return result.getHeadRevisions()
            .values()
            .stream()
            .anyMatch(Objects::nonNull);
    }

    private static boolean hasAnyCheckpointRevision(ConsistencyCheckResult result) {
        return result.getCheckpointRevisions()
            .values()
            .stream()
            .flatMap(m -> m.values().stream())
            .anyMatch(Objects::nonNull);
    }

    private int check(Consumer<Check.Builder> builderConsumer) {
        Check.Builder builder = Check.builder()
            .withPath(new File(temporaryFolder.getRoot().getAbsolutePath()))
            .withDebugInterval(Long.MAX_VALUE)
            .withCheckBinaries(true)
            .withCheckHead(true)
            .withCheckpoints(checkpoints)
            .withOutWriter(log.outWriter)
            .withErrWriter(log.errWriter);
        builderConsumer.accept(builder);
        return builder.build().run();
    }

    private static void assertCheckFailed(int checkResult) {
        assertEquals("Check should have failed", 1, checkResult);
    }

    private static void assertCheckSucceeded(int checkResult) {
        assertEquals("Check should have succeeded", 0, checkResult);
    }

    private static class Output {
        private final StringWriter strOut;
        private final StringWriter strErr;
        private final PrintWriter outWriter;
        private final PrintWriter errWriter;

        public Output() {
            strOut = new StringWriter();
            strErr = new StringWriter();
            outWriter = new PrintWriter(strOut, true);
            errWriter = new PrintWriter(strErr, true);
        }

        public void close() {
            outWriter.close();
            errWriter.close();
        }

        public String outString() {
            return strOut.toString();
        }

        public String errString() {
            return strErr.toString();
        }
    }
}
