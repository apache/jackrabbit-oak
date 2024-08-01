/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.commons;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class LongUtilsTest {

    @Test
    public void tryParse() {
        assertParsingSuccess("-1");
        assertParsingSuccess("0");
        assertParsingSuccess("1");
        assertParsingSuccess(String.valueOf(Long.MAX_VALUE));
        assertParsingSuccess(String.valueOf(Long.MIN_VALUE));
        assertParsingSuccess(String.valueOf(Integer.MIN_VALUE));
        assertParsingSuccess(String.valueOf(Integer.MAX_VALUE));
        assertParsingSuccess("-0");
        for (long i = Long.MIN_VALUE; i < Long.MIN_VALUE + 100; i++) {
            assertParsingSuccess(String.valueOf(i));
        }
        for (long i = Long.MAX_VALUE; i > Long.MAX_VALUE - 100; i--) {
            assertParsingSuccess(String.valueOf(i));
        }

        assertParsingFailure("0.1");
        assertParsingFailure("1.1");
        assertParsingFailure("-1.1");
        assertParsingFailure("1.1e3");
        assertParsingFailure("foo");
        assertParsingFailure("");
        assertParsingFailure("-");
        assertParsingFailure("9223372036854775808"); // Long.MAX_VALUE + 1
        assertParsingFailure("9223372036854775809"); // Long.MAX_VALUE + 2
        assertParsingFailure("-9223372036854775809"); // Long.MIN_VALUE - 1
        assertParsingFailure("-9223372036854775810"); // Long.MIN_VALUE - 2
        assertParsingFailure("122229223372036854775809");
        assertParsingFailure("-122229223372036854775809");
    }

    private void assertParsingSuccess(String value) {
        assertEquals(Long.parseLong(value), LongUtils.tryParse(value).longValue());
    }

    private void assertParsingFailure(String value) {
        try {
            long parsed = Long.parseLong(value);
            fail("Expected NumberFormatException but instead the string: " + value + " was parsed as: " + parsed);
        } catch (NumberFormatException e) {
            // OK
        }
        assertNull(LongUtils.tryParse(value));
    }
}