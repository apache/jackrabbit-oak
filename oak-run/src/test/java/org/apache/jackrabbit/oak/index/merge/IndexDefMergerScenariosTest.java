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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test merging index definitions.
 */
@RunWith(Parameterized.class)
public class IndexDefMergerScenariosTest {
    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                testCase("should use the latest base version for the base in merges", "merges-base"),
                testCase("should use the latest base version for the base in merges when updating multiple version numbers", "merges-multi-version")
        });
    }

    public static Object[] testCase(String name, String baseName) {
        return new Object[] {
                name,
                baseName + "_build.json",
                baseName + "_run.json",
                baseName + "_expected.json"
        };
    }

    private final String name;
    private final JsonObject buildIndexes;
    private final JsonObject runIndexes;
    private final JsonObject expectedIndexes;

    public IndexDefMergerScenariosTest(String name, String buildFile, String runFile, String expectedFile)
            throws IOException {
        this.name = name;
        this.buildIndexes = JsonObject.fromJson(IndexDefMergerTest.readFromResource(buildFile), true);
        this.runIndexes = JsonObject.fromJson(IndexDefMergerTest.readFromResource(runFile), true);
        this.expectedIndexes = JsonObject.fromJson(IndexDefMergerTest.readFromResource(expectedFile), true);
    }

    @Test
    public void testMerge() {
        IndexDefMergerUtils.merge(buildIndexes, runIndexes);
        assertEquals("Failed to execute test: " + name, expectedIndexes.toString(), buildIndexes.toString());
    }

}
