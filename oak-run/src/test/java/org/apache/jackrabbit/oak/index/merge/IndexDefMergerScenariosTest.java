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
package org.apache.jackrabbit.oak.index.merge;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test merging index definitions.
 */
@RunWith(Parameterized.class)
public class IndexDefMergerScenariosTest extends ParameterizedMergingTestBase {

    private static final Logger log = LoggerFactory.getLogger(IndexDefMergerConflictsTest.class);

    @Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                testCase("should merge custom into new base index", "basic.json"),
                testCase("should use the latest base version for the base in merges", "merges-base.json"),
                testCase(
                        "should use the latest base version for the base in merges when updating multiple version numbers",
                        "merges-multi-version.json"),
                testCase("should not remove child nodes from product index missing from custom index",
                        "missing-child.json"),
                testCase("should support removing adding changing properties from product index in custom index",
                        "removed-property.json")
        });
    }

    private final JsonObject expectedIndexes;

    public IndexDefMergerScenariosTest(String name, String testCaseFile)
            throws IOException {
        super(name, testCaseFile);
        this.expectedIndexes = super.getTestCaseChild("expected");
    }

    @Test
    public void verifyExpectedMergeResult() {
        IndexDefMergerUtils.merge(buildIndexes, runIndexes);
        File output = new File("target" + File.separator + "surefire-output" + File.separator
                + getClass().getCanonicalName().replace(".", "-") + File.separator + testCaseFile);
        try {
            if (!output.getParentFile().exists()) {
                output.getParentFile().mkdirs();
            }
            IOUtils.write(buildIndexes.toString(), new FileOutputStream(output),
                    StandardCharsets.UTF_8);
        } catch (IOException e) {
            log.warn("Failed to write merged index definition to: {}", output, e);
        }
        assertEquals("Failed to execute test: " + testCaseName, expectedIndexes.toString(), buildIndexes.toString());
    }

}
