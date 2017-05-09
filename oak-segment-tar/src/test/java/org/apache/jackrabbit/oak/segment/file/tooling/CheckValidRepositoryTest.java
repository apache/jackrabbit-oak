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
package org.apache.jackrabbit.oak.segment.file.tooling;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.jackrabbit.oak.segment.tool.Check;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Tests for {@link CheckCommand} assuming a consistent repository.
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
        .withJournal("journal.log")
        .withDebugInterval(Long.MAX_VALUE)
        .withCheckBinaries(true)
        .withFilterPaths(filterPaths)
        .withIOStatistics(true)
        .withOutWriter(outWriter)
        .withErrWriter(errWriter)
        .build()
        .run();
        
        outWriter.close();
        errWriter.close();
        
        assertExpectedOutput(strOut.toString(), Lists.newArrayList("Searched through 1 revisions", "Checked 7 nodes and 21 properties",
                "Path / is consistent"));
        assertExpectedOutput(strErr.toString(), Lists.newArrayList(""));
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
        .withJournal("journal.log")
        .withDebugInterval(Long.MAX_VALUE)
        .withCheckBinaries(true)
        .withFilterPaths(filterPaths)
        .withIOStatistics(true)
        .withOutWriter(outWriter)
        .withErrWriter(errWriter)
        .build()
        .run();
        
        outWriter.close();
        errWriter.close();
        
        assertExpectedOutput(strOut.toString(), Lists.newArrayList("Searched through 1 revisions",
                "Checked 1 nodes and 1 properties", "Checked 1 nodes and 2 properties", "Checked 1 nodes and 3 properties",
                "Path /a is consistent", "Path /b is consistent", "Path /c is consistent", "Path /d is consistent", "Path /e is consistent",
                "Path /f is consistent"));
        assertExpectedOutput(strErr.toString(), Lists.newArrayList(""));
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
        .withJournal("journal.log")
        .withDebugInterval(Long.MAX_VALUE)
        .withFilterPaths(filterPaths)
        .withIOStatistics(true)
        .withOutWriter(outWriter)
        .withErrWriter(errWriter)
        .build()
        .run();
        
        outWriter.close();
        errWriter.close();
        
        assertExpectedOutput(strOut.toString(), Lists.newArrayList("Searched through 1 revisions", "Checked 7 nodes and 15 properties",
                "Path / is consistent"));
        assertExpectedOutput(strErr.toString(), Lists.newArrayList(""));
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
        .withJournal("journal.log")
        .withDebugInterval(Long.MAX_VALUE)
        .withFilterPaths(filterPaths)
        .withIOStatistics(true)
        .withOutWriter(outWriter)
        .withErrWriter(errWriter)
        .build()
        .run();
        
        outWriter.close();
        errWriter.close();
        
        assertExpectedOutput(strOut.toString(), Lists.newArrayList("Searched through 1 revisions", "Checked 1 nodes and 0 properties",
                "Checked 1 nodes and 0 properties", "Checked 1 nodes and 4 properties", "Checked 1 nodes and 5 properties",
                "Path /a is consistent", "Path /b is consistent", "Path /d is consistent", "Path /e is consistent"));
        assertExpectedOutput(strErr.toString(), Lists.newArrayList(""));
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
        .withJournal("journal.log")
        .withDebugInterval(Long.MAX_VALUE)
        .withFilterPaths(filterPaths)
        .withIOStatistics(true)
        .withOutWriter(outWriter)
        .withErrWriter(errWriter)
        .build()
        .run();
        
        outWriter.close();
        errWriter.close();
        
        assertExpectedOutput(strOut.toString(), Lists.newArrayList("Searched through 1 revisions", "No good revision found"));
        assertExpectedOutput(strErr.toString(), Lists.newArrayList("Path /g not found"));
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
        .withJournal("journal.log")
        .withDebugInterval(Long.MAX_VALUE)
        .withFilterPaths(filterPaths)
        .withCheckBinaries(true)
        .withIOStatistics(true)
        .withOutWriter(outWriter)
        .withErrWriter(errWriter)
        .build()
        .run();
        
        outWriter.close();
        errWriter.close();
        
        assertExpectedOutput(strOut.toString(), Lists.newArrayList("Searched through 1 revisions", "Checked 1 nodes and 1 properties",
                "Checked 1 nodes and 6 properties", "Checked 1 nodes and 4 properties", "Checked 1 nodes and 5 properties",
                "Path /a is consistent", "Path /f is consistent", "Path /d is consistent", "Path /e is consistent"));
        assertExpectedOutput(strErr.toString(), Lists.newArrayList("Path /g not found"));
    }
}
