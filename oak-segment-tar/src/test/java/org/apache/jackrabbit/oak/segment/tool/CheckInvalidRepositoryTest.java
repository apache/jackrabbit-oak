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
import java.util.LinkedHashSet;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.segment.file.JournalEntry;
import org.apache.jackrabbit.oak.segment.file.JournalReader;
import org.apache.jackrabbit.oak.segment.file.tar.LocalJournalFile;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link Check} assuming an invalid repository.
 */
public class CheckInvalidRepositoryTest extends CheckRepositoryTestBase {

    @Before
    public void setup() throws Exception {
        super.setup();
        super.addInvalidRevision();
    }

    @Test
    public void testInvalidRevisionFallbackOnValid() {
        StringWriter strOut = new StringWriter();
        StringWriter strErr = new StringWriter();

        PrintWriter outWriter = new PrintWriter(strOut, true);
        PrintWriter errWriter = new PrintWriter(strErr, true);

        Set<String> filterPaths = new LinkedHashSet<>();
        filterPaths.add("/");

        Check.builder()
            .withPath(new File(temporaryFolder.getRoot().getAbsolutePath()))
            .withDebugInterval(Long.MAX_VALUE)
            .withCheckHead(true)
            .withCheckpoints(checkpoints)
            .withCheckBinaries(true)
            .withFilterPaths(filterPaths)
            .withOutWriter(outWriter)
            .withErrWriter(errWriter)
            .build()
            .run();

        outWriter.close();
        errWriter.close();

        assertExpectedOutput(strOut.toString(), Lists.newArrayList("Checked 7 nodes and 21 properties", "Path / is consistent",
            "Searched through 2 revisions"));

        // not sure whether first traversal will fail because of "/a" or "/z" 
        assertExpectedOutput(strErr.toString(), Lists.newArrayList("Error while traversing /"));
    }

    @Test
    public void testPartialBrokenPathWithoutValidRevision() {
        StringWriter strOut = new StringWriter();
        StringWriter strErr = new StringWriter();

        PrintWriter outWriter = new PrintWriter(strOut, true);
        PrintWriter errWriter = new PrintWriter(strErr, true);

        Set<String> filterPaths = new LinkedHashSet<>();
        filterPaths.add("/z");

        Check.builder()
            .withPath(new File(temporaryFolder.getRoot().getAbsolutePath()))
            .withDebugInterval(Long.MAX_VALUE)
            .withCheckBinaries(true)
            .withCheckHead(true)
            .withCheckpoints(checkpoints)
            .withFilterPaths(filterPaths)
            .withOutWriter(outWriter)
            .withErrWriter(errWriter)
            .build()
            .run();

        outWriter.close();
        errWriter.close();

        assertExpectedOutput(strOut.toString(), Lists.newArrayList("Checking head", "Checking checkpoints", "No good revision found"));
        assertExpectedOutput(strErr.toString(),
            Lists.newArrayList(
                "Error while traversing /z: java.lang.IllegalArgumentException: Segment reference out of bounds",
                "Path /z not found"));
    }

    @Test
    public void testPartialBrokenPathWithValidRevision() {
        StringWriter strOut = new StringWriter();
        StringWriter strErr = new StringWriter();

        PrintWriter outWriter = new PrintWriter(strOut, true);
        PrintWriter errWriter = new PrintWriter(strErr, true);

        Set<String> filterPaths = new LinkedHashSet<>();
        filterPaths.add("/a");

        Check.builder()
            .withPath(new File(temporaryFolder.getRoot().getAbsolutePath()))
            .withDebugInterval(Long.MAX_VALUE)
            .withCheckBinaries(true)
            .withCheckHead(true)
            .withCheckpoints(new HashSet<String>())
            .withFilterPaths(filterPaths)
            .withOutWriter(outWriter)
            .withErrWriter(errWriter)
            .build()
            .run();

        outWriter.close();
        errWriter.close();

        assertExpectedOutput(strOut.toString(), Lists.newArrayList("Checked 1 nodes and 1 properties", "Path /a is consistent",
            "Searched through 2 revisions"));
        assertExpectedOutput(strErr.toString(), Lists.newArrayList(
            "Error while traversing /a: java.lang.IllegalArgumentException: Segment reference out of bounds"));
    }

    @Test
    public void testCorruptHeadWithValidCheckpoints() {
        StringWriter strOut = new StringWriter();
        StringWriter strErr = new StringWriter();

        PrintWriter outWriter = new PrintWriter(strOut, true);
        PrintWriter errWriter = new PrintWriter(strErr, true);

        Set<String> filterPaths = new LinkedHashSet<>();
        filterPaths.add("/");

        Check.builder()
            .withPath(new File(temporaryFolder.getRoot().getAbsolutePath()))
            .withDebugInterval(Long.MAX_VALUE)
            .withCheckBinaries(true)
            .withCheckHead(true)
            .withCheckpoints(checkpoints)
            .withFilterPaths(filterPaths)
            .withOutWriter(outWriter)
            .withErrWriter(errWriter)
            .build()
            .run();

        outWriter.close();
        errWriter.close();

        assertExpectedOutput(strOut.toString(), Lists.newArrayList("Checking head", "Checking checkpoints",
            "Checked 7 nodes and 21 properties", "Path / is consistent", "Searched through 2 revisions and 2 checkpoints"));
        assertExpectedOutput(strErr.toString(), Lists.newArrayList(
            "Error while traversing /a: java.lang.IllegalArgumentException: Segment reference out of bounds"));
    }

    @Test
    public void testCorruptPathInCp1NoValidRevision() throws Exception {
        corruptPathFromCheckpoint();

        StringWriter strOut = new StringWriter();
        StringWriter strErr = new StringWriter();

        PrintWriter outWriter = new PrintWriter(strOut, true);
        PrintWriter errWriter = new PrintWriter(strErr, true);

        Set<String> filterPaths = new LinkedHashSet<>();
        filterPaths.add("/b");

        Set<String> cps = new HashSet<>();
        cps.add(checkpoints.iterator().next());

        Check.builder()
            .withPath(new File(temporaryFolder.getRoot().getAbsolutePath()))
            .withDebugInterval(Long.MAX_VALUE)
            .withCheckBinaries(true)
            .withCheckpoints(cps)
            .withFilterPaths(filterPaths)
            .withOutWriter(outWriter)
            .withErrWriter(errWriter)
            .build()
            .run();

        outWriter.close();
        errWriter.close();

        assertExpectedOutput(strOut.toString(), Lists.newArrayList("Searched through 2 revisions and 1 checkpoints", "No good revision found"));
        assertExpectedOutput(strErr.toString(), Lists.newArrayList(
            "Error while traversing /b: java.lang.IllegalArgumentException: Segment reference out of bounds"));
    }

    @Test
    public void testLargeJournal() throws IOException {
        StringWriter strOut = new StringWriter();
        StringWriter strErr = new StringWriter();

        PrintWriter outWriter = new PrintWriter(strOut, true);
        PrintWriter errWriter = new PrintWriter(strErr, true);

        File segmentStoreFolder = new File(temporaryFolder.getRoot().getAbsolutePath());
        File journalFile = new File(segmentStoreFolder, "journal.log");
        File largeJournalFile = temporaryFolder.newFile("journal.log.large");

        JournalReader journalReader = new JournalReader(new LocalJournalFile(journalFile));
        JournalEntry journalEntry = journalReader.next();

        String journalLine = journalEntry.getRevision() + " root " + journalEntry.getTimestamp() + "\n";

        for (int k = 0; k < 10000; k++) {
            FileUtils.writeStringToFile(largeJournalFile, journalLine, true);
        }

        Check.builder()
            .withPath(segmentStoreFolder)
            .withJournal(largeJournalFile)
            .withDebugInterval(Long.MAX_VALUE)
            .withCheckBinaries(true)
            .withCheckHead(true)
            .withFilterPaths(ImmutableSet.of("/"))
            .withCheckpoints(checkpoints)
            .withOutWriter(outWriter)
            .withErrWriter(errWriter)
            .build()
            .run();

        outWriter.close();
        errWriter.close();

        assertExpectedOutput(strOut.toString(), Lists.newArrayList("No good revision found"));
    }

}
