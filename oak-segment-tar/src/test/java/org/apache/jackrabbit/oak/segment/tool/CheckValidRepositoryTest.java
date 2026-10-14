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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link Check} assuming a consistent repository.
 */
public class CheckValidRepositoryTest extends CheckRepositoryTestBase {

    @Test
    public void testSuccessfulFullCheckWithBinaryTraversal() throws Exception {
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
            .withCheckpoints(new HashSet<String>())
            .withFilterPaths(filterPaths)
            .withIOStatistics(true)
            .withOutWriter(outWriter)
            .withErrWriter(errWriter)
            .build()
            .run();

        outWriter.close();
        errWriter.close();

        assertExpectedOutput(strOut.toString(), List.of("Checking head", "Searched through 1 revisions and 0 checkpoints",
            "Checked 7 nodes and 21 properties", "Path / is consistent"));
        assertExpectedOutput(strErr.toString(), List.of(""));
    }

    @Test
    public void testSuccessfulOnlyRootKidsCheckWithBinaryTraversalAndFilterPaths() throws Exception {
        StringWriter strOut = new StringWriter();
        StringWriter strErr = new StringWriter();

        PrintWriter outWriter = new PrintWriter(strOut, true);
        PrintWriter errWriter = new PrintWriter(strErr, true);

        Set<String> filterPaths = new LinkedHashSet<>();
        filterPaths.add("/a");
        filterPaths.add("/b");
        filterPaths.add("/c");
        filterPaths.add("/d");
        filterPaths.add("/e");
        filterPaths.add("/f");

        Check.builder()
            .withPath(new File(temporaryFolder.getRoot().getAbsolutePath()))
            .withDebugInterval(Long.MAX_VALUE)
            .withCheckBinaries(true)
            .withCheckHead(true)
            .withCheckpoints(new HashSet<String>())
            .withFilterPaths(filterPaths)
            .withIOStatistics(true)
            .withOutWriter(outWriter)
            .withErrWriter(errWriter)
            .build()
            .run();

        outWriter.close();
        errWriter.close();

        assertExpectedOutput(strOut.toString(), List.of("Checking head", "Searched through 1 revisions and 0 checkpoints",
            "Checked 1 nodes and 1 properties", "Checked 1 nodes and 2 properties", "Checked 1 nodes and 3 properties",
            "Path /a is consistent", "Path /b is consistent", "Path /c is consistent", "Path /d is consistent", "Path /e is consistent",
            "Path /f is consistent"));
        assertExpectedOutput(strErr.toString(), List.of(""));
    }

    @Test
    public void testSuccessfulFullCheckWithoutBinaryTraversal() throws Exception {
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
            .withCheckpoints(new HashSet<String>())
            .withFilterPaths(filterPaths)
            .withIOStatistics(true)
            .withOutWriter(outWriter)
            .withErrWriter(errWriter)
            .build()
            .run();

        outWriter.close();
        errWriter.close();

        assertExpectedOutput(strOut.toString(), List.of("Checking head", "Searched through 1 revisions and 0 checkpoints",
            "Checked 7 nodes and 15 properties", "Path / is consistent"));
        assertExpectedOutput(strErr.toString(), List.of(""));
    }

    @Test
    public void testSuccessfulPartialCheckWithoutBinaryTraversal() throws Exception {
        StringWriter strOut = new StringWriter();
        StringWriter strErr = new StringWriter();

        PrintWriter outWriter = new PrintWriter(strOut, true);
        PrintWriter errWriter = new PrintWriter(strErr, true);

        Set<String> filterPaths = new LinkedHashSet<>();
        filterPaths.add("/a");
        filterPaths.add("/b");
        filterPaths.add("/d");
        filterPaths.add("/e");

        Check.builder()
            .withPath(new File(temporaryFolder.getRoot().getAbsolutePath()))
            .withDebugInterval(Long.MAX_VALUE)
            .withCheckHead(true)
            .withCheckpoints(new HashSet<String>())
            .withFilterPaths(filterPaths)
            .withIOStatistics(true)
            .withOutWriter(outWriter)
            .withErrWriter(errWriter)
            .build()
            .run();

        outWriter.close();
        errWriter.close();

        assertExpectedOutput(strOut.toString(), List.of("Checking head", "Searched through 1 revisions and 0 checkpoints",
            "Checked 1 nodes and 0 properties", "Checked 1 nodes and 4 properties", "Checked 1 nodes and 5 properties",
            "Path /a is consistent", "Path /b is consistent", "Path /d is consistent", "Path /e is consistent"));
        assertExpectedOutput(strErr.toString(), List.of(""));
    }

    @Test
    public void testUnsuccessfulPartialCheckWithoutBinaryTraversal() throws Exception {
        StringWriter strOut = new StringWriter();
        StringWriter strErr = new StringWriter();

        PrintWriter outWriter = new PrintWriter(strOut, true);
        PrintWriter errWriter = new PrintWriter(strErr, true);

        Set<String> filterPaths = new LinkedHashSet<>();
        filterPaths.add("/g");

        Check.builder()
            .withPath(new File(temporaryFolder.getRoot().getAbsolutePath()))
            .withDebugInterval(Long.MAX_VALUE)
            .withCheckHead(true)
            .withCheckpoints(new HashSet<String>())
            .withFilterPaths(filterPaths)
            .withIOStatistics(true)
            .withOutWriter(outWriter)
            .withErrWriter(errWriter)
            .build()
            .run();

        outWriter.close();
        errWriter.close();

        assertExpectedOutput(strOut.toString(), List.of("Checking head", "Searched through 1 revisions and 0 checkpoints",
            "No good revision found"));
        assertExpectedOutput(strErr.toString(), List.of("Path /g not found"));
    }

    @Test
    public void testUnsuccessfulPartialCheckWithBinaryTraversal() throws Exception {
        StringWriter strOut = new StringWriter();
        StringWriter strErr = new StringWriter();

        PrintWriter outWriter = new PrintWriter(strOut, true);
        PrintWriter errWriter = new PrintWriter(strErr, true);

        Set<String> filterPaths = new LinkedHashSet<>();
        filterPaths.add("/a");
        filterPaths.add("/f");
        filterPaths.add("/g");
        filterPaths.add("/d");
        filterPaths.add("/e");

        Check.builder()
            .withPath(new File(temporaryFolder.getRoot().getAbsolutePath()))
            .withDebugInterval(Long.MAX_VALUE)
            .withFilterPaths(filterPaths)
            .withCheckBinaries(true)
            .withCheckHead(true)
            .withCheckpoints(new HashSet<String>())
            .withIOStatistics(true)
            .withOutWriter(outWriter)
            .withErrWriter(errWriter)
            .build()
            .run();

        outWriter.close();
        errWriter.close();

        assertExpectedOutput(strOut.toString(), List.of("Checking head", "Searched through 1 revisions and 0 checkpoints",
            "Checked 1 nodes and 1 properties", "Checked 1 nodes and 6 properties", "Checked 1 nodes and 4 properties",
            "Checked 1 nodes and 5 properties",
            "Path /a is consistent", "Path /f is consistent", "Path /d is consistent", "Path /e is consistent"));
        assertExpectedOutput(strErr.toString(), List.of("Path /g not found"));
    }

    @Test
    public void testSuccessfulCheckOfHeadAndCheckpointsWithoutFilterPaths() throws Exception {
        StringWriter strOut = new StringWriter();
        StringWriter strErr = new StringWriter();

        PrintWriter outWriter = new PrintWriter(strOut, true);
        PrintWriter errWriter = new PrintWriter(strErr, true);

        Set<String> filterPaths = new LinkedHashSet<>();
        filterPaths.add("/");

        Check.builder()
            .withPath(new File(temporaryFolder.getRoot().getAbsolutePath()))
            .withDebugInterval(Long.MAX_VALUE)
            .withFilterPaths(filterPaths)
            .withCheckBinaries(true)
            .withCheckHead(true)
            .withCheckpoints(checkpoints)
            .withIOStatistics(true)
            .withOutWriter(outWriter)
            .withErrWriter(errWriter)
            .build()
            .run();

        outWriter.close();
        errWriter.close();

        assertExpectedOutput(strOut.toString(), List.of("Checking head", "Checking checkpoints",
            "Searched through 1 revisions and 2 checkpoints", "Checked 7 nodes and 21 properties", "Path / is consistent"));
        assertExpectedOutput(strErr.toString(), List.of(""));
    }

    @Test
    public void testSuccessfulCheckOfHeadAndCheckpointsWithFilterPaths() throws Exception {
        StringWriter strOut = new StringWriter();
        StringWriter strErr = new StringWriter();

        PrintWriter outWriter = new PrintWriter(strOut, true);
        PrintWriter errWriter = new PrintWriter(strErr, true);

        Set<String> filterPaths = new LinkedHashSet<>();
        filterPaths.add("/f");

        Check.builder()
            .withPath(new File(temporaryFolder.getRoot().getAbsolutePath()))
            .withDebugInterval(Long.MAX_VALUE)
            .withFilterPaths(filterPaths)
            .withCheckBinaries(true)
            .withCheckHead(true)
            .withCheckpoints(checkpoints)
            .withIOStatistics(true)
            .withOutWriter(outWriter)
            .withErrWriter(errWriter)
            .build()
            .run();

        outWriter.close();
        errWriter.close();

        assertExpectedOutput(strOut.toString(), List.of("Checking head", "Checking checkpoints",
            "Searched through 1 revisions and 2 checkpoints", "Checked 1 nodes and 6 properties", "Path /f is consistent"));
        assertExpectedOutput(strErr.toString(), List.of(""));
    }

    @Test
    public void testMissingCheckpointCheck() throws Exception {
        StringWriter strOut = new StringWriter();
        StringWriter strErr = new StringWriter();

        PrintWriter outWriter = new PrintWriter(strOut, true);
        PrintWriter errWriter = new PrintWriter(strErr, true);

        Set<String> filterPaths = new LinkedHashSet<>();
        filterPaths.add("/");

        HashSet<String> checkpoints = new HashSet<String>();
        checkpoints.add("bogus-checkpoint");

        Check.builder()
            .withPath(new File(temporaryFolder.getRoot().getAbsolutePath()))
            .withDebugInterval(Long.MAX_VALUE)
            .withFilterPaths(filterPaths)
            .withCheckBinaries(true)
            .withCheckpoints(checkpoints)
            .withIOStatistics(true)
            .withOutWriter(outWriter)
            .withErrWriter(errWriter)
            .build()
            .run();

        outWriter.close();
        errWriter.close();

        assertExpectedOutput(strOut.toString(), List.of("Checking checkpoints", "Searched through 1 revisions and 1 checkpoints",
            "No good revision found"));
        assertExpectedOutput(strErr.toString(), List.of("Checkpoint bogus-checkpoint not found in this revision!"));
    }

    @Test
    public void testInvalidRevisionFallbackOnValidWhenOnlyNewNodeIsCorrupt() throws Exception {
        addInvalidRevisionWithCorruptNewNodeOnly();

        StringWriter strOut = new StringWriter();
        StringWriter strErr = new StringWriter();

        PrintWriter outWriter = new PrintWriter(strOut, true);
        PrintWriter errWriter = new PrintWriter(strErr, true);

        Check.builder()
            .withPath(new File(temporaryFolder.getRoot().getAbsolutePath()))
            .withDebugInterval(Long.MAX_VALUE)
            .withCheckBinaries(true)
            .withCheckHead(true)
            .withCheckpoints(checkpoints)
            .withFilterPaths(Set.of("/"))
            .withOutWriter(outWriter)
            .withErrWriter(errWriter)
            .build()
            .run();

        outWriter.close();
        errWriter.close();

        assertExpectedOutput(strOut.toString(), List.of("Checked 7 nodes and 21 properties", "Path / is consistent",
            "Searched through 2 revisions"));
        assertExpectedOutput(strErr.toString(),
            List.of("Error while traversing /z: java.lang.IllegalArgumentException: Segment reference out of bounds"));
    }

    @Test
    public void testNewNodeFilterWithoutValidRevision() throws Exception {
        addInvalidRevisionWithCorruptNewNodeOnly();

        StringWriter strOut = new StringWriter();
        StringWriter strErr = new StringWriter();

        PrintWriter outWriter = new PrintWriter(strOut, true);
        PrintWriter errWriter = new PrintWriter(strErr, true);

        Check.builder()
            .withPath(new File(temporaryFolder.getRoot().getAbsolutePath()))
            .withDebugInterval(Long.MAX_VALUE)
            .withCheckBinaries(true)
            .withCheckHead(true)
            .withCheckpoints(new HashSet<String>())
            .withFilterPaths(Set.of("/z"))
            .withOutWriter(outWriter)
            .withErrWriter(errWriter)
            .build()
            .run();

        outWriter.close();
        errWriter.close();

        assertExpectedOutput(strOut.toString(), List.of("Searched through 2 revisions", "No good revision found"));
        assertExpectedOutput(strErr.toString(), List.of("Error while traversing /z", "Path /z not found"));
    }

    @Test
    public void testSuccessfulFullCheckWithContinueOnError() throws Exception {
        StringWriter strOut = new StringWriter();
        StringWriter strErr = new StringWriter();

        PrintWriter outWriter = new PrintWriter(strOut, true);
        PrintWriter errWriter = new PrintWriter(strErr, true);

        int result = Check.builder()
            .withPath(new File(temporaryFolder.getRoot().getAbsolutePath()))
            .withDebugInterval(Long.MAX_VALUE)
            .withCheckBinaries(true)
            .withCheckHead(true)
            .withCheckpoints(new HashSet<String>())
            .withFilterPaths(Set.of("/"))
            .withContinueOnError(true)
            .withOutWriter(outWriter)
            .withErrWriter(errWriter)
            .build()
            .run();

        outWriter.close();
        errWriter.close();

        assertEquals(0, result);
        assertExpectedOutput(strOut.toString(), List.of("Checked 7 nodes and 21 properties", "Path / is consistent"));
        assertEquals("", strErr.toString());
    }
}
