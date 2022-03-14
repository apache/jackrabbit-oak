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
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import groovy.json.StringEscapeUtils;

/**
 * Test merging conflict detection
 */
@RunWith(Parameterized.class)
public class IndexDefMergerConflictsTest extends ParameterizedMergingTestBase {

    @Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                testCase("product and custom properties must equal if not in ancestor",
                        "conflict-new-properties.json")
        });
    }

    private final String expectedMessage;

    public IndexDefMergerConflictsTest(String name, String testCaseFile)
            throws IOException {
        super(name, testCaseFile);
        this.expectedMessage = StringEscapeUtils.unescapeJavaScript(testCase.getProperties().get("expected"));
    }

    @Test
    public void verifyExpectedConflict() {
        UnsupportedOperationException exception = assertThrows(
                "Expected exception not thrown for test case:" + super.testCaseName,
                UnsupportedOperationException.class,
                () -> IndexDefMergerUtils.merge(buildIndexes, runIndexes));
        assertEquals("Unexpected exception message validating: " + super.testCaseName, "" + expectedMessage,
                "\"" + exception.getMessage() + "\"");
    }

}
