/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jackrabbit.oak.remote.http.matcher;

import org.junit.Test;

import static org.apache.jackrabbit.oak.remote.http.matcher.Matchers.matchesAll;
import static org.apache.jackrabbit.oak.remote.http.matcher.Matchers.matchesMethod;
import static org.apache.jackrabbit.oak.remote.http.matcher.Matchers.matchesPath;
import static org.apache.jackrabbit.oak.remote.http.matcher.Matchers.matchesRequest;
import static org.junit.Assert.assertNotNull;

public class MatchersTest {

    @Test
    public void testCreateMethodMatcher() {
        assertNotNull(matchesMethod("GET"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateMethodMatcherWithNullMethod() {
        matchesMethod(null);
    }

    @Test
    public void testCreatePathMatcher() {
        assertNotNull(matchesPath("/test"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreatePathMatcherWithNullPattern() {
        matchesPath(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreatePathMatcherWithInvalidPattern() {
        matchesPath("/test(");
    }

    @Test
    public void testCreateAllMatcher() {
        assertNotNull(matchesAll());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateAllMatchersWithNullMatcher() {
        matchesAll(null, null);
    }

    @Test
    public void testCreateRequestMatcher() {
        assertNotNull(matchesRequest("GET", "/test"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateRequestMatcherWithNullMethod() {
        matchesRequest(null, "/test");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateRequestMatcherWithNullPattern() {
        matchesRequest("GET", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateRequestMatcherWithInvalidPattern() {
        matchesRequest("GET", "/test(");
    }

}
