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

        assertMatches("**/bar/**/foo", "bar/foo");
        assertMatches("**/bar/**/foo", "/bar/foo");
        
        assertMatches("**/bar/**/foo", "a/bar/foo");
        assertMatches("**/bar/**/foo", "/a/bar/foo");

        assertMatches("**/bar/**/foo", "bar/b/foo");
        assertMatches("**/bar/**/foo", "/bar/b/foo");

        assertMatches("**/bar/**/foo", "bar/a/b/c/foo");
        assertMatches("**/bar/**/foo", "/bar/a/b/c/foo");
        
        assertMatches("/a/b/**/foo/*", "/a/b/foo/xy");
        assertDoesntMatch("/a/b/**/foo/*", "/a/b/barfoo/xy");
        assertDoesntMatch("/a/b/**/foo/*", "/a/bbar/foo/xy");
        assertDoesntMatch("/a/b/**/foo/*", "/a/bbar/barfoo/xy");
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

        // '*' doesnt match /
        assertDoesntMatch("*.html", "/foo.html");
        
        assertMatches("**/*.html", "foo.html");
        assertMatches("**/*.*", "foo.html");
        assertDoesntMatch("**/*.*", "foohtml");
        
        assertMatches("**/*.html", "/foo.html");
        assertMatches("**/*.*", "/foo.html");

        assertMatches("**/*.html", "bar/foo.html");
        assertMatches("**/*.*", "bar/foo.html");
        assertMatches("**/*.html", "bar/a/b/foo.html");
        assertMatches("**/*.*", "bar/a/b/foo.html");

        // no '.':
        assertDoesntMatch("**/*.*", "bar/foohtml");

        assertMatches("**/*.html", "/bar/foo.html");
        assertMatches("**/*.*", "/bar/foo.html");

        // no '.':
        assertDoesntMatch("**/*.*", "/bar/foohtml");

        assertMatches("**/*.html", "bar/zoo/foo.html");
        assertMatches("**/*.*", "bar/zoo/foo.html");

        // no '.':
        assertDoesntMatch("**/*.*", "bar/zoo/foohtml");

        assertMatches("**/*.html", "/bar/zoo/foo.html");
        assertMatches("**/*.*", "/bar/zoo/foo.html");

        // no '.':
        assertDoesntMatch("**/*.*", "/bar/zoo/foohtml");
    }
    
    @Test
    public void globPath() throws Exception {
        // leading / only matches when path starts with / too
        assertDoesntMatch("/**", "foo");
        assertMatches("**", "foo");
        assertMatches("/**", "/foo");
        
        // leading / only matches when path starts with / too
        assertDoesntMatch("/**", "foo/bar");
        assertMatches("**", "foo/bar");
        assertMatches("/**", "/foo/bar");
        
        // leading / only matches when path starts with / too
        assertDoesntMatch("/**", "foo/bar/zoo");
        assertMatches("**", "foo/bar/zoo");
        assertMatches("/**", "/foo/bar/zoo");
        
        // leading / only matches when path starts with / too
        assertDoesntMatch("/**/*.html", "foo.html");
        assertMatches("/**/*.html", "/foo.html");
        
        // leading / only matches when path starts with / too
        assertDoesntMatch("/**/*.html", "bar/foo.html");
        assertMatches("/**/*.html", "/bar/foo.html");
        
        // leading / only matches when path starts with / too
        assertDoesntMatch("/**/*.html", "bar/zoo/foo.html");
        assertMatches("/**/*.html", "/bar/zoo/foo.html");
        
        // leading / only matches when path starts with / too
        assertDoesntMatch("/*.html", "foo.html");
        assertMatches("/*.html", "/foo.html");
        
        assertMatches("/foo/bar/**", "/foo/bar");
        
        // leading / only matches when path starts with / too
        assertDoesntMatch("/foo/bar/**", "foo/bar");
        
        // /foo/barbar should not match /foo/bar/**
        assertDoesntMatch("/foo/bar/**", "/foo/barbar");
        
        assertMatches("/foo/bar/**", "/foo/bar/zet");
    }

    private void assertMatches(String globPath, String path) {
        Pattern p = Pattern.compile(GlobbingPathHelper.globPathAsRegex(globPath));
        assertTrue("'"+globPath+"' does not match '"+path+"'", p.matcher(path).matches());
    }

    private void assertDoesntMatch(String globPath, String path) {
        Pattern p = Pattern.compile(GlobbingPathHelper.globPathAsRegex(globPath));
        assertFalse("'"+globPath+"' does match '"+path+"'", p.matcher(path).matches());
    }
}
