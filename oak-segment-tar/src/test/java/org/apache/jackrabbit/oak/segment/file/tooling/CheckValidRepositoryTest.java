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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.tool.Check;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Tests for {@link CheckCommand}
 */
public class CheckValidRepositoryTest {
    private static final Logger log = LoggerFactory.getLogger(CheckValidRepositoryTest.class);
    
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Before
    public void setup() throws Exception {
        FileStore fileStore = FileStoreBuilder.fileStoreBuilder(temporaryFolder.getRoot())
                .withMaxFileSize(256)
                .withSegmentCacheSize(64)
                .build();
        
        SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
        NodeBuilder builder = nodeStore.getRoot().builder();
        
        addChildWithBlobProperties(nodeStore, builder, "a", 5);
        addChildWithBlobProperties(nodeStore, builder, "b", 10);
        addChildWithBlobProperties(nodeStore, builder, "c", 15);
        
        addChildWithProperties(nodeStore, builder, "d", 5);
        addChildWithProperties(nodeStore, builder, "e", 5);
        addChildWithProperties(nodeStore, builder, "f", 5);
        
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        fileStore.close();
    }
    
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
        
        assertExpectedOutput(strOut.toString(), Lists.newArrayList("Searched through 1 revisions", "Checked 7 nodes and 45 properties"));
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
        
        assertExpectedOutput(strOut.toString(), Lists.newArrayList("Searched through 1 revisions", "Checked 6 nodes and 45 properties"));
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
        
        assertExpectedOutput(strOut.toString(), Lists.newArrayList("Searched through 1 revisions", "Checked 7 nodes and 15 properties"));
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
        
        assertExpectedOutput(strOut.toString(), Lists.newArrayList("Searched through 1 revisions", "Checked 4 nodes and 10 properties"));
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
        
        assertExpectedOutput(strOut.toString(), Lists.newArrayList("Broken revision", "Checked 0 nodes and 0 properties", "No good revision found"));
        assertExpectedOutput(strErr.toString(), Lists.newArrayList("Invalid path: /g"));
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
        
        assertExpectedOutput(strOut.toString(), Lists.newArrayList("Broken revision", "Checked 2 nodes and 10 properties", "No good revision found"));
        assertExpectedOutput(strErr.toString(), Lists.newArrayList("Invalid path: /g"));
    }
    
    private static void assertExpectedOutput(String message, List<String> assertMessages) {
        log.info("Assert message: {}", assertMessages);
        log.info("Message logged: {}", message);

        
        for (String msg : assertMessages) {
            Assert.assertTrue(message.contains(msg));
        }
    }
    
    private static void addChildWithBlobProperties(SegmentNodeStore nodeStore, NodeBuilder builder, String childName,
            int propCount) throws IOException {
        NodeBuilder child = builder.child(childName);
        for (int i = 0; i < propCount; i++) {
            child.setProperty(childName + i, nodeStore.createBlob(randomStream(i, 2000)));
        }
    }

    private static void addChildWithProperties(SegmentNodeStore nodeStore, NodeBuilder builder, String childName,
            int propCount) throws IOException {
        NodeBuilder child = builder.child(childName);
        for (int i = 0; i < propCount; i++) {
            child.setProperty(childName + i, childName + i);
        }
    }
    
    private static InputStream randomStream(int seed, int size) {
        Random r = new Random(seed);
        byte[] data = new byte[size];
        r.nextBytes(data);
        return new ByteArrayInputStream(data);
    }
}
