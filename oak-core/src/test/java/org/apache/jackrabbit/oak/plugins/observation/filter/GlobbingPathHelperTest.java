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
package org.apache.jackrabbit.oak.plugins.observation.filter;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import java.util.regex.Pattern;

import org.junit.Test;

public class GlobbingPathHelperTest {

    @Test
    public void globSubPath1() throws Exception {
        assertMatches("foo", "foo");
        assertMatches("**", "foo");
        assertMatches("**/bar", "bar");
        assertMatches("**/bar", "foo/bar");
        assertMatches("**/bar", "foo/zoo/bar");
        assertMatches("**/bar/**", "bar");
        assertMatches("**/bar/**", "/bar");
        assertMatches("**/bar/**", "foo/bar");
        assertMatches("**/bar/**", "/foo/bar");
        assertMatches("**/bar/**", "foo/bar/zoo");
        assertMatches("**/bar/**", "/foo/bar/zoo");
        assertMatches("**/bar/**", "bar/foo");
        assertMatches("**/bar/**", "/bar/foo");
        assertMatches("**/bar/**", "bar/foo/zoo");
        assertMatches("**/bar/**", "/bar/foo/zoo");
    }

    @Test
    public void globSubPath2() throws Exception {
        assertMatches("foo", "foo");
        assertDoesntMatch("foo", "/foo");
        assertMatches("**", "foo");
        assertMatches("**", "/foo");
        assertMatches("**", "foo/bar");
        assertMatches("**", "foo/bar/zoo");
        assertMatches("*.html", "foo.html");
        assertDoesntMatch("*.html", "/foo.html");
        assertMatches("**/*.html", "foo.html");
        assertMatches("**/*.html", "/foo.html");
        assertMatches("**/*.html", "bar/foo.html");
        assertMatches("**/*.html", "/bar/foo.html");
        assertMatches("**/*.html", "bar/zoo/foo.html");
        assertMatches("**/*.html", "/bar/zoo/foo.html");
    }
    
    @Test
    public void globPath() throws Exception {
        assertMatches("/**", "foo");
        assertMatches("/**", "/foo");
        assertMatches("/**", "foo/bar");
        assertMatches("/**", "/foo/bar");
        assertMatches("/**", "foo/bar/zoo");
        assertMatches("/**", "/foo/bar/zoo");
        assertMatches("/**/*.html", "foo.html");
        assertMatches("/**/*.html", "/foo.html");
        assertMatches("/**/*.html", "bar/foo.html");
        assertMatches("/**/*.html", "/bar/foo.html");
        assertMatches("/**/*.html", "bar/zoo/foo.html");
        assertMatches("/**/*.html", "/bar/zoo/foo.html");
        assertDoesntMatch("/*.html", "foo.html");
        assertMatches("/*.html", "/foo.html");
    }

    private void assertMatches(String globPath, String path) {
        Pattern p = Pattern.compile(GlobbingPathHelper.globAsRegex(globPath));
        assertTrue("'"+globPath+"' does not match '"+path+"'", p.matcher(path).matches());
    }

    private void assertDoesntMatch(String globPath, String path) {
        Pattern p = Pattern.compile(GlobbingPathHelper.globAsRegex(globPath));
        assertFalse("'"+globPath+"' does match '"+path+"'", p.matcher(path).matches());
    }
}
