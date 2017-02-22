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
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Tests for {@link CheckCommand} assuming an invalid repository.
 */
public class CheckInvalidRepositoryTest extends CheckRepositoryTestBase {

    @Before
    public void setup() throws Exception {
        super.setup();
        super.addInvalidRevision();
    }

    @Ignore
    @Test
    public void testInvalidRevision() {
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
        .withCheckBinaries(false)
        .withFilterPaths(filterPaths)
        .withOutWriter(outWriter)
        .withErrWriter(errWriter)
        .build()
        .run();
        
        outWriter.close();
        errWriter.close();
        
        assertExpectedOutput(strOut.toString(), Lists.newArrayList("Broken revision",
                "Checked 7 nodes and 15 properties", "Found latest good revision", "Searched through 2 revisions"));
        assertExpectedOutput(strErr.toString(), Lists.newArrayList("Error while traversing /z"));
    }
}
