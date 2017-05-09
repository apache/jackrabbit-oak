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

import javax.servlet.http.HttpServletRequest;
import java.util.regex.Pattern;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class PathMatcherTest {

    private boolean matches(String requestPath, String matcherPattern) {
        HttpServletRequest request = mock(HttpServletRequest.class);
        doReturn(requestPath).when(request).getPathInfo();
        return new PathMatcher(Pattern.compile(matcherPattern)).match(request);
    }

    @Test
    public void testMatch() {
        assertTrue(matches("/test", "/test"));
    }

    @Test
    public void testCaseSensitiveMatch() {
        assertFalse(matches("/test", "/Test"));
    }

    @Test
    public void testPatternMatch() {
        assertTrue(matches("/test/something", "/test/.*"));
    }

    @Test
    public void testNoPathInfo() {
        assertFalse(matches(null, "/test"));
    }

}
