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
package org.apache.jackrabbit.oak.index.indexer.document.incrementalstore;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedMergeSortTaskTest;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertArrayEquals;

public class IncrementalFlatFileStoreNodeStateEntryWriterTest {
    private static final Logger LOG = LoggerFactory.getLogger(PipelinedMergeSortTaskTest.class);

    /*
     There are many characters which can't be in revision and operand but
     for testing purpose just assuming | is the only restricted character
     nodeData can have anything
     */
    @Test
    public void testParts() throws Exception {
        int runTestMillis = 1000;
        long testEndTime = System.currentTimeMillis() + runTestMillis;
        long stringsWithPipesGreaterThanThree = 0;
        long totalRuns = 0;
        while (System.currentTimeMillis() < testEndTime) {
            String[] testStringParts = createStringParts();
            String testString = String.join("|", testStringParts);
            String[] evaluatedParts = IncrementalFlatFileStoreNodeStateEntryWriter.getParts(testString);
            assertArrayEquals(testStringParts, evaluatedParts);
            int initialLength = testString.length();
            String testStringWithPipeRemoved = testString.replace("|", "");
            int finalLengthAfterPipeRemoval = testStringWithPipeRemoved.length();
            totalRuns++;
            if (initialLength - finalLengthAfterPipeRemoval > 3) {
                stringsWithPipesGreaterThanThree++;
            }
        }
        LOG.info("total runs:{}", totalRuns);
        LOG.info("total Strings with more than 3 |'s :{}", stringsWithPipesGreaterThanThree);

        System.out.println("total Strings with mre than 3 |'s :"
                + stringsWithPipesGreaterThanThree + "  " + totalRuns);
    }
    private String[] createStringParts() {
        String path = RandomStringUtils.randomPrint(1000).replace("|", "/");
        String nodeData = RandomStringUtils.randomPrint(5000);
        String revision = RandomStringUtils.randomPrint(20).replace("|", "-");
        ;
        String operand = RandomStringUtils.randomPrint(1).replace("|", "A");
        return new String[]{path, nodeData, revision, operand};
    }

}