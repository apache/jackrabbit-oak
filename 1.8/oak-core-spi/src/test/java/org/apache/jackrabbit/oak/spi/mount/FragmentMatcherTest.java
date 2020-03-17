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
package org.apache.jackrabbit.oak.spi.mount;

import org.junit.Test;

import static org.apache.jackrabbit.oak.spi.mount.FragmentMatcher.Result.FULL_MATCH;
import static org.apache.jackrabbit.oak.spi.mount.FragmentMatcher.Result.MISMATCH;
import static org.apache.jackrabbit.oak.spi.mount.FragmentMatcher.Result.PARTIAL_MATCH;
import static org.junit.Assert.assertEquals;

public class FragmentMatcherTest {

    @Test
    public void testMatcher() {
        assertResult(PARTIAL_MATCH, "/content", "/");
        assertResult(FULL_MATCH, "/content", "/content");
        assertResult(FULL_MATCH, "/content", "/content/acme");
        assertResult(FULL_MATCH, "/content", "/content/acme/en");

        assertResult(PARTIAL_MATCH, "/content/*", "/");
        assertResult(PARTIAL_MATCH, "/content/*", "/content");
        assertResult(FULL_MATCH, "/content/*", "/content/acme");
        assertResult(FULL_MATCH, "/content/*", "/content/acme/site");

        assertResult(PARTIAL_MATCH, "/content$", "/");
        assertResult(FULL_MATCH, "/content$", "/content");
        assertResult(MISMATCH, "/content$", "/content/acme");

        assertResult(PARTIAL_MATCH, "/content/*$", "/");
        assertResult(PARTIAL_MATCH, "/content/*$", "/content");
        assertResult(FULL_MATCH, "/content/*$", "/content/acme");
        assertResult(MISMATCH, "/content/*$", "/content/acme/site");

        assertResult(PARTIAL_MATCH, "/content/*/site", "/");
        assertResult(PARTIAL_MATCH, "/content/*/site", "/content");
        assertResult(PARTIAL_MATCH, "/content/*/site", "/content/acme");
        assertResult(FULL_MATCH, "/content/*/site", "/content/acme/site");
        assertResult(FULL_MATCH, "/content/*/site", "/content/acme/site/en");

        assertResult(PARTIAL_MATCH, "/content/*/site/", "/");
        assertResult(PARTIAL_MATCH, "/content/*/site/", "/content");
        assertResult(PARTIAL_MATCH, "/content/*/site/", "/content/acme");
        assertResult(FULL_MATCH, "/content/*/site/", "/content/acme/site/en");
        assertResult(FULL_MATCH, "/content/*/site/", "/content/acme/site/en/home");
    }

    private static void assertResult(FragmentMatcher.Result expectedResult, String pattern, String subject) {
        assertEquals(expectedResult, FragmentMatcher.startsWith(pattern, subject));
    }

}
