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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.commons.json.JsonObject;

public class ParameterizedMergingTestBase {

    protected final String testCaseFile;
    protected final String testCaseName;
    protected final JsonObject buildIndexes;
    protected final JsonObject runIndexes;
    protected final JsonObject testCase;

    public static Object[] testCase(String name, String testCaseFile) {
        return new Object[] {
                name,
                testCaseFile
        };
    }

    public ParameterizedMergingTestBase(String name, String testCaseFile)
            throws IOException {
        this.testCaseName = name;
        this.testCaseFile = testCaseFile;
        this.testCase = readTestCaseFile(testCaseFile);
        this.buildIndexes = getTestCaseChild("build");
        this.runIndexes = getTestCaseChild("run");
    }

    private JsonObject readTestCaseFile(String testCaseFileName) {
        return Optional.ofNullable(IndexDefMergerConflictsTest.class.getResourceAsStream(testCaseFileName))
                .map(in -> {
                    try {
                        return IOUtils.toString(in, StandardCharsets.UTF_8.toString());
                    } catch (IOException e) {
                        throw new IllegalArgumentException(
                                "Unexpected IOException reading test case file: " + testCaseFileName, e);
                    }
                })
                .map(s -> JsonObject.fromJson(s, true))
                .orElseThrow(() -> new IllegalArgumentException("Unable to read test case file: " + testCaseFileName));
    }

    protected JsonObject getTestCaseChild(String fieldName) {
        return Optional.ofNullable(testCase.getChildren().get(fieldName))
                .orElseThrow(() -> new IllegalArgumentException(
                        "Unable to run test: " + testCaseName + ", Expected field " + fieldName + " not set"));
    }

}
