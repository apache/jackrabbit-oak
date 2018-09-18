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

package org.apache.jackrabbit.oak.security.authorization.restriction;

import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GlobPatternTest {

    private static void assertMatch(GlobPattern gp, String testPath, Boolean expectedResult) {
        Boolean match = Boolean.valueOf(gp.matches(testPath));
        assertEquals("Pattern : " + gp + "; TestPath : " + testPath, expectedResult, match);
    }

    @Test
    public void testMatchesWildcardAll() {

        Map<String,Boolean> tests = new HashMap<String,Boolean>();

        // restriction "*" matches /foo, all siblings of foo and foo's and the siblings' descendants
        GlobPattern gp = GlobPattern.create("/a/b/c", "*");
        // matching
        tests.put("/a/b/c", true);        // foo itself
        tests.put("/a/b/c/d", true);      // child of foo
        tests.put("/a/b/c/d/e", true);    // child of foo
        tests.put("/a/b/c/d/e/f", true);  // child of foo
        tests.put("/a/b/cde", true);      // sibling
        tests.put("/a/b/cde/e/f", true);  // child of the sibling
        // not-matching
        tests.put("/", false);
        tests.put("/a", false);
        tests.put("/b/c", false);

        for (String testPath : tests.keySet()) {
            assertMatch(gp, testPath, tests.get(testPath));
        }

        // restriction "*cat" matches all siblings and descendants of /foo that have a name ending with cat
        gp = GlobPattern.create("/a/b/c", "*e");
        tests = new HashMap<String,Boolean>();
        // matching
        tests.put("/a/b/c/e", true);      // descendant with name segment 'e'
        tests.put("/a/b/c/d/e", true);    // descendant with name segment 'e'
        tests.put("/a/b/c/gge", true);    // descendant with name segment ending with 'e'
        tests.put("/a/b/c/d/gge", true);  // descendant with name segment ending with 'e'
        tests.put("/a/b/ce", true);       // sibling whose name ends with 'e'
        tests.put("/a/b/chee", true);     // sibling whose name ends with 'e'
        tests.put("/a/b/cd/e", true);     // descendant of sibling named 'e'
        tests.put("/a/b/cd/f/e", true);   // descendant of sibling named 'e'
        tests.put("/a/b/cd/e", true);     // descendant of sibling with name ending with 'e'
        tests.put("/a/b/cd/f/e", true);   // descendant of sibling with name ending with 'e'
        // not-matching
        tests.put("/", false);
        tests.put("/a", false);
        tests.put("/b/c", false);
        tests.put("/a/b/c", false);
        tests.put("/a/b/c/d", false);
        tests.put("/a/b/c/d/e/f", false);
        tests.put("/a/b/c/d/f/e/f", false);
        tests.put("/a/b/c/d/f/efg", false);
        tests.put("/a/b/c/d/f/f", false);
        tests.put("/a/b/c/e/f", false);
        tests.put("/a/b/ce/", false);
        tests.put("/a/b/ceg", false);

        for (String testPath : tests.keySet()) {
            assertMatch(gp, testPath, tests.get(testPath));
        }

        // restriction "*/cat" matches all descendants of /foo and foo's siblings that have a name segment "cat"
        gp = GlobPattern.create("/a/b/c", "*/e");
        tests = new HashMap<String,Boolean>();
        // matching
        tests.put("/a/b/c/e", true);      // descendant with name segment 'e'
        tests.put("/a/b/c/d/e", true);    // descendant with name segment 'e'
        tests.put("/a/b/cd/e", true);     // descendant of sibling named 'e'
        tests.put("/a/b/cd/f/e", true);   // descendant of sibling named 'e'
        // not-matching
        tests.put("/", false);
        tests.put("/a", false);
        tests.put("/b/c", false);
        tests.put("/a/b/c", false);
        tests.put("/a/b/c/d", false);
        tests.put("/a/b/c/d/e/f", false);
        tests.put("/a/b/c/d/f/e/f", false);
        tests.put("/a/b/c/d/f/efg", false);
        tests.put("/a/b/c/d/f/f", false);
        tests.put("/a/b/c/e/f", false);
        tests.put("/a/b/ce/", false);

        for (String testPath : tests.keySet()) {
            assertMatch(gp, testPath, tests.get(testPath));
        }

        // matches target path '/a/b/c/e', all siblings whose name starts with e
        // and child nodes of either.
        gp = GlobPattern.create("/a/b/c/e", "*");
        tests = new HashMap<String,Boolean>();
        // matching
        tests.put("/a/b/c/e/f/g/h", true);
        tests.put("/a/b/c/e/d/e/f", true);
        tests.put("/a/b/c/e/d/e/f", true);
        tests.put("/a/b/c/e", true);
        tests.put("/a/b/c/e/", true);
        tests.put("/a/b/c/ef", true);
        tests.put("/a/b/c/ef/g", true);
        // not-matching
        tests.put("/a/b/ce/f/g/h", false);
        tests.put("/a/b/ce/d/e/f", false);
        tests.put("/a/b/c", false);
        tests.put("/a/b/c/d", false);
        tests.put("/a/b/c/d/e/f", false);
        tests.put("/a/b/c/d/f/f", false);
        tests.put("/a/b/c/d/f/e/f", false);
        tests.put("/a/b/cee/d/e/f", false);

        for (String testPath : tests.keySet()) {
            assertMatch(gp, testPath, tests.get(testPath));
        }

        // all descendants of '/a/b/c/e'
        gp = GlobPattern.create("/a/b/c/e", "/*");
        tests = new HashMap<String,Boolean>();
        // matching
        tests.put("/a/b/c/e/f/g/h", true);
        tests.put("/a/b/c/e/d/e/f", true);
        // not-matching
        tests.put("/a/b/c/e", false);  // not matching node path
        tests.put("/a/b/c/e/", false); // not matching node path + /
        tests.put("/a/b/c/ef", false); // not matching siblings of node path
        tests.put("/a/b/ce/f/g/h", false);
        tests.put("/a/b/ce/d/e/f", false);
        tests.put("/a/b/c", false);
        tests.put("/a/b/c/d", false);
        tests.put("/a/b/c/d/e", false);
        tests.put("/a/b/c/d/e/f", false);
        tests.put("/a/b/c/d/f/f", false);
        tests.put("/a/b/c/d/f/e/f", false);
        tests.put("/a/b/cee/d/e/f", false);

        for (String testPath : tests.keySet()) {
            assertMatch(gp, testPath, tests.get(testPath));
        }

        // all descendants of '/a/b/ce'
        gp = GlobPattern.create("/a/b/c", "e/*");
        tests = new HashMap<String,Boolean>();
        // not-matching
        tests.put("/a/b/ce/f/g/h", true);
        tests.put("/a/b/ce/d/e/f", true);
        // not-matching
        tests.put("/a/b/c", false);
        tests.put("/a/b/ce", false);
        tests.put("/a/b/c/d", false);
        tests.put("/a/b/c/d/e", false);
        tests.put("/a/b/c/d/e/f", false);
        tests.put("/a/b/c/d/f/f", false);
        tests.put("/a/b/c/d/f/e/f", false);
        tests.put("/a/b/cee/d/e/f", false);
        tests.put("/a/b/ce/", false);       // missing * after ce/

        for (String testPath : tests.keySet()) {
            assertMatch(gp, testPath, tests.get(testPath));
        }

        // all descendants of '/'
        gp = GlobPattern.create("/", "*");
        tests = new HashMap<String,Boolean>();
        // matching
        tests.put("/a", true);
        tests.put("/b/", true);
        tests.put("/c/d", true);
        tests.put("/a/b/ce/", true);
        tests.put("/a/b/ce/f/g/h", true);
        tests.put("/a/b/ce/d/e/f", true);
        // not-matching
        tests.put("/", false);

        for (String testPath : tests.keySet()) {
            assertMatch(gp, testPath, tests.get(testPath));
        }

        // restriction "*cat/*" matches all siblings and descendants of /foo that have an intermediate segment ending with 'cat'
        gp = GlobPattern.create("/a/b/c", "*e/*");
        tests = new HashMap<String,Boolean>();
        // matching
        tests.put("/a/b/ceeeeeee/f/g/h", true);
        tests.put("/a/b/cde/d/e/f", true);
        tests.put("/a/b/c/d/e/f", true);
        tests.put("/a/b/ced/d/e/f", true);
        // not-matching
        tests.put("/a/b/cde", false);      // sibling ending with e
        tests.put("/a/b/ce/", false);      // ignore trailing / in test path
        tests.put("/a/b/c/d/e/", false);   // ignore trailing / in test path
        tests.put("/a/b/c/d", false);      // missing *e/*
        tests.put("/a/b/c/d/e", false);    // missing /*
        tests.put("/a/b/c/d/f/f", false);  // missing *e
        tests.put("/a/b/c/ed/f/f", false); // missing e/

        for (String testPath : tests.keySet()) {
            assertMatch(gp, testPath, tests.get(testPath));
        }

        //  restriction /*cat/*  matches all descendants of /foo that have an intermediate segment ending with 'cat'
        gp = GlobPattern.create("/a/b/c", "/*e/*");
        tests = new HashMap<String,Boolean>();
        // matching
        tests.put("/a/b/c/d/e/f", true);
        tests.put("/a/b/c/de/f", true);
        // not-matching
        tests.put("/a/b/cde", false);      // sibling ending with e
        tests.put("/a/b/ced/d/e/f", false);// sibling containing intermediate segment
        tests.put("/a/b/cde/d/e/f", false);// sibling containing intermediate segment
        tests.put("/a/b/ce/", false);      // ignore trailing / in test path
        tests.put("/a/b/c/d/e/", false);   // ignore trailing / in test path
        tests.put("/a/b/c/d/e", false);    // no intermediate segment
        tests.put("/a/b/c/d", false);      // missing *e/*
        tests.put("/a/b/c/d/e", false);    // missing /*
        tests.put("/a/b/c/d/f/f", false);  // missing *e
        tests.put("/a/b/c/ed/f/f", false); // missing e/

        for (String testPath : tests.keySet()) {
            assertMatch(gp, testPath, tests.get(testPath));
        }

        //  restriction /*cat  matches all children of /a/b/c whose path ends with "cat"
        gp = GlobPattern.create("/a/b/c", "/*cat");
        tests = new HashMap<String,Boolean>();
        // matching
        tests.put("/a/b/c/cat", true);
        tests.put("/a/b/c/acat", true);
        tests.put("/a/b/c/f/cat", true);
        tests.put("/a/b/c/f/acat", true);
        // not-matching
        tests.put("/a/b/c/d", false);
        tests.put("/a/b/c/d/cat/e", false);  // cat only intermediate segment
        tests.put("/a/b/c/d/acat/e", false);  // cat only intermediate segment
        tests.put("/a/b/c/d/cata/e", false);  // cat only intermediate segment
        tests.put("/a/b/c/d/cate", false);
        tests.put("/a/b/cat", false);        // siblings do no match
        tests.put("/a/b/cat/ed/f/f", false); // ... nor do siblings' children
        tests.put("/a/b/ced/cat", false);    // ... nor do siblings' children

        for (String testPath : tests.keySet()) {
            assertMatch(gp, testPath, tests.get(testPath));
        }

        //  restriction /*/cat  matches all non-direct descendants of /foo named "cat"
        gp = GlobPattern.create("/a/b/c", "/*/cat");
        tests = new HashMap<String,Boolean>();
        // matching
        tests.put("/a/b/c/a/cat", true);
        tests.put("/a/b/c/d/e/f/cat", true);
        // not-matching
        tests.put("/a/b/c/cat", false);
        tests.put("/a/b/c/cate", false);
        tests.put("/a/b/c/acat", false);
        tests.put("/a/b/c/cat/d", false);
        tests.put("/a/b/c/d/acat", false);
        tests.put("/a/b/c/d/cate", false);
        tests.put("/a/b/c/d/cat/e", false);   // cat only intermediate segment
        tests.put("/a/b/c/d/acat/e", false);  // cat only intermediate segment
        tests.put("/a/b/c/d/cata/e", false);  // cat only intermediate segment
        tests.put("/a/b/cat", false);        // siblings do no match
        tests.put("/a/b/cat/ed/f/f", false); // ... nor do siblings' children
        tests.put("/a/b/ced/cat", false);    // ... nor do siblings' children
        tests.put("/a/b/ced/f/cat", false);  // ... nor do siblings' children

        for (String testPath : tests.keySet()) {
            assertMatch(gp, testPath, tests.get(testPath));
        }

        //  restriction /cat* matches all descendant paths of /foo that have the
        //  direct foo-descendant segment starting with "cat"
        gp = GlobPattern.create("/a/b/c", "/cat*");
        tests = new HashMap<String,Boolean>();
        // matching
        tests.put("/a/b/c/cat", true);
        tests.put("/a/b/c/cats", true);
        tests.put("/a/b/c/cat/s", true);
        tests.put("/a/b/c/cats/d/e/f", true);
        // not-matching
        tests.put("/a/b/c/d/cat", false);
        tests.put("/a/b/c/d/cats", false);
        tests.put("/a/b/c/d/e/cat", false);
        tests.put("/a/b/c/d/e/cats", false);
        tests.put("/a/b/c/acat", false);
        tests.put("/a/b/c/d/acat", false);
        tests.put("/a/b/c/d/cat/e", false);
        tests.put("/a/b/c/d/acat/e", false);
        tests.put("/a/b/c/d/cata/e", false);
        tests.put("/a/b/cat", false);        // siblings do no match
        tests.put("/a/b/cat/ed/f/f", false); // ... nor do siblings' children
        tests.put("/a/b/ced/cat", false);    // ... nor do siblings' children
        tests.put("/a/b/ced/f/cat", false);  // ... nor do siblings' children

        for (String testPath : tests.keySet()) {
            assertMatch(gp, testPath, tests.get(testPath));
        }
    }

    @Test
    public void testEmptyRestriction() {
        GlobPattern gp = GlobPattern.create("/", "");
        Map<String,Boolean> tests = new HashMap<String,Boolean>();
        tests.put("/", true);

        tests.put("/a/b/c/d", false);
        tests.put("/a/b/c/d/e", false);
        tests.put("/a/b/c/d/e/f", false);
        tests.put("/a/b/c", false);
        tests.put("/a", false);
        tests.put("/a/b/cde", false);

        for (String toTest : tests.keySet()) {
            assertTrue(gp + " : " + toTest, tests.get(toTest) == gp.matches(toTest));
        }

        gp = GlobPattern.create("/a/b/c", "");

        tests = new HashMap<String,Boolean>();
        tests.put("/a/b/c", true);

        tests.put("/a/b/c/d", false);
        tests.put("/a/b/c/d/e", false);
        tests.put("/a/b/c/d/e/f", false);
        tests.put("/", false);
        tests.put("/a", false);
        tests.put("/a/b/cde", false);

        for (String toTest : tests.keySet()) {
            assertTrue(gp + " : " + toTest, tests.get(toTest) == gp.matches(toTest));
        }
    }

    @Test
    public void testPathRestriction() {
        GlobPattern gp = GlobPattern.create("/a/b/c", "d");
        Map<String,Boolean> tests = new HashMap<String,Boolean>();
        tests.put("/", false);
        tests.put("/a", false);
        tests.put("/a/b/c", false);
        tests.put("/a/b/c/d", false);
        tests.put("/a/b/c/d/e/f", false);
        tests.put("/a/b/cd", true);
        tests.put("/a/b/cd/e", true);
        tests.put("/a/b/cd/e/f", true);
        tests.put("/a/b/cde", false);

        for (String toTest : tests.keySet()) {
            assertTrue(gp + " : " + toTest, tests.get(toTest) == gp.matches(toTest));
        }

        gp = GlobPattern.create("/a/b/c", "/d");

        tests = new HashMap<String,Boolean>();
        tests.put("/", false);
        tests.put("/a", false);
        tests.put("/a/b/c", false);
        tests.put("/a/b/c/d", true);
        tests.put("/a/b/c/d/e/f", true);
        tests.put("/a/b/cd", false);
        tests.put("/a/b/cd/e", false);
        tests.put("/a/b/cd/e/f", false);
        tests.put("/a/b/cde", false);

        for (String toTest : tests.keySet()) {
            assertTrue(gp + " : " + toTest, tests.get(toTest) == gp.matches(toTest));
        }
    }

    @Test
    public void testMaxOccurrences() {
        GlobPattern gp = GlobPattern.create("/", "1*/2*/3*/4*/5*/6*/7*/8*/9*/10*/11*/12*/13*/14*/15*/16*/17*/18*/19*/20*/21*");
        try {
            gp.matches("/1/2/3/4/5/6/7/8/9/10/11/12/13/14/15/16/17/18/19/20/21");
            fail();
        } catch (IllegalArgumentException e) {
            // success
        };
        try {
            gp.matches("/11/22/33/44/55/66/77/88/99/100/111/122/133/144/155/166/177/188/199/200/211");
            fail();
        } catch (IllegalArgumentException e) {
            // success
        };
    }

    @Test
    public void testMatches() {
        assertFalse(GlobPattern.create("/a/b/c/d", "/*").matches());
    }

    @Test
    public void testHashCode() {
        GlobPattern pattern = GlobPattern.create("/a/b/c/d", "/*");
        assertEquals(Objects.hashCode("/a/b/c/d", "/*"), pattern.hashCode());
    }

    @Test
    public void testEquals() {
        GlobPattern pattern = GlobPattern.create("/a/b/c/d", "/*");

        assertEquals(pattern, pattern);
        assertEquals(pattern, GlobPattern.create("/a/b/c/d", "/*"));
    }

    @Test
    public void testNotEquals() {
        GlobPattern pattern = GlobPattern.create("/a/b/c/d", "/*");

        assertNotEquals(pattern, GlobPattern.create("/a/b/c", "/*"));
        assertNotEquals(pattern, GlobPattern.create("/a/b/c/d", "*"));
        assertNotEquals(pattern, new PrefixPattern(ImmutableSet.of("/a/b/c", "/*")));
    }
}
