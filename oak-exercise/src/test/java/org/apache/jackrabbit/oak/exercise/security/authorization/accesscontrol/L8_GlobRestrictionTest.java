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
package org.apache.jackrabbit.oak.exercise.security.authorization.accesscontrol;

import java.util.LinkedHashMap;
import java.util.Map;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionPattern;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * <pre>
 * Module: Authorization (Access Control Management)
 * =============================================================================
 *
 * Title: The Globbing Restriction
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * After having completed this exercises you should be familiar with the rep:glob
 * restriction as present in the default implementation and able to use it to
 * limit the effect of a given ACE to a given subtree.
 *
 * Exercises:
 *
 * For all tests fill in the expected result (matching, not-matching):
 *
 * - {@link #testWildcard}
 * - {@link #testWildcard2}
 * - {@link #testWildcard3}
 * - {@link #testWildcard4}
 * - {@link #testWildcard5}
 * - {@link #testWildcard6}
 * - {@link #testWildcard7}
 * - {@link #testWildcard8}
 * - {@link #testWildcard9}
 * - {@link #testWildcard10}
 * - {@link #testWildcardOnRoot}
 * - {@link #testEmptyOnRoot}
 * - {@link #testEmpty}
 * - {@link #testPath}
 * - {@link #testPath2}
 * - {@link #testPath3}
 *
 * </pre>
 *
 * @see org.apache.jackrabbit.oak.security.authorization.restriction.GlobPattern
 */
public class L8_GlobRestrictionTest extends AbstractSecurityTest {

    @Test
    public void testWildcard() throws Exception {
        RestrictionPattern globPattern = buildGlobPattern("/a/b/c","*");
        
        // EXERCISE: fill-in the expected result for the match (true or true|false) for the given set of paths:
        Map<String, Boolean> tests = new LinkedHashMap<>();
        tests.put("/", true|true|false);
        tests.put("/a", true|true|false);
        tests.put("/a/b/c", true|true|false);
        tests.put("/a/b/c/d/e/f", true|true|false);
        tests.put("/a/b/cde", true|true|false);
        tests.put("/a/b/cde/e/f", true|true|false);
        tests.put("/b/c", true|true|false);

        for (String testPath : tests.keySet()) {
            assertMatch(globPattern, testPath, tests.get(testPath));
        }
    }

    @Test
    public void testWildcard2() throws Exception {
        RestrictionPattern globPattern = buildGlobPattern("/a/b/c","*e");
        
        // EXERCISE: fill-in the expected result for the match (true or false) for the given set of paths:
        Map<String, Boolean> tests = new LinkedHashMap<>();
        tests.put("/", true|false);
        tests.put("/a", true|false);
        tests.put("/a/b/c", true|false);
        tests.put("/a/b/c/e", true|false);
        tests.put("/a/b/c/d/e", true|false);
        tests.put("/a/b/c/gge", true|false);
        tests.put("/a/b/c/d/gge", true|false);
        tests.put("/a/b/ce", true|false);
        tests.put("/a/b/ce/", true|false);
        tests.put("/a/b/ceg", true|false);
        tests.put("/a/b/chee", true|false);
        tests.put("/a/b/cd/e", true|false);
        tests.put("/a/b/cd/f/e", true|false);
        tests.put("/a/b/cd/e", true|false);
        tests.put("/a/b/cd/f/e", true|false);
        tests.put("/a/b/c/d", true|false);
        tests.put("/a/b/c/d/e/f", true|false);
        tests.put("/a/b/c/d/f/e/f", true|false);
        tests.put("/a/b/c/d/f/efg", true|false);
        tests.put("/a/b/c/d/f/f", true|false);
        tests.put("/a/b/c/e/f", true|false);
        tests.put("/b/c", true|false);

        for (String testPath : tests.keySet()) {
            assertMatch(globPattern, testPath, tests.get(testPath));
        }
    }

    @Test
    public void testWildcard3() throws Exception {
        RestrictionPattern globPattern = buildGlobPattern("/a/b/c","*/e");

        // EXERCISE: fill-in the expected result for the match (true or false) for the given set of paths:
        Map<String, Boolean> tests = new LinkedHashMap<>();
        tests.put("/", true|false);
        tests.put("/a", true|false);
        tests.put("/b/c", true|false);
        tests.put("/a/b/c", true|false);
        tests.put("/a/b/c/e", true|false);
        tests.put("/a/b/c/d", true|false);
        tests.put("/a/b/c/e/f", true|false);
        tests.put("/a/b/c/d/e", true|false);
        tests.put("/a/b/c/d/f/f", true|false);
        tests.put("/a/b/c/d/e/f", true|false);
        tests.put("/a/b/c/d/f/e/f", true|false);
        tests.put("/a/b/cd/e", true|false);
        tests.put("/a/b/cd/f/e", true|false);
        tests.put("/a/b/ce/", true|false);
        tests.put("/a/b/c/d/f/efg", true|false);

        for (String testPath : tests.keySet()) {
            assertMatch(globPattern, testPath, tests.get(testPath));
        }
    }

    @Test
    public void testWildcard4() throws Exception {
        RestrictionPattern globPattern = buildGlobPattern("/a/b/c/e", "/*");

        // EXERCISE: fill-in the expected result for the match (true or false) for the given set of paths:
        Map<String, Boolean> tests = new LinkedHashMap<>();
        tests.put("/a/b/c", true|false);
        tests.put("/a/b/c/d", true|false);
        tests.put("/a/b/c/d/e", true|false);
        tests.put("/a/b/c/e", true|false);
        tests.put("/a/b/c/e/", true|false);
        tests.put("/a/b/c/ef", true|false);
        tests.put("/a/b/c/d/e/f", true|false);
        tests.put("/a/b/c/d/f/f", true|false);
        tests.put("/a/b/c/d/f/e/f", true|false);
        tests.put("/a/b/cee/d/e/f", true|false);
        tests.put("/a/b/ce/f/g/h", true|false);
        tests.put("/a/b/c/e/f/g/h", true|false);
        tests.put("/a/b/ce/d/e/f", true|false);
        tests.put("/a/b/c/e/d/e/f", true|false);

        for (String testPath : tests.keySet()) {
            assertMatch(globPattern, testPath, tests.get(testPath));
        }
    }

    @Test
    public void testWildcard5() throws Exception {
        RestrictionPattern globPattern = buildGlobPattern("/a/b/c", "e/*");

        // EXERCISE: fill-in the expected result for the match (true or false) for the given set of paths:
        Map<String, Boolean> tests = new LinkedHashMap<>();
        tests.put("/a/b/c", true|false);
        tests.put("/a/b/ce", true|false);
        tests.put("/a/b/c/d", true|false);
        tests.put("/a/b/c/d/e", true|false);
        tests.put("/a/b/c/d/e/f", true|false);
        tests.put("/a/b/c/d/f/f", true|false);
        tests.put("/a/b/c/d/f/e/f", true|false);
        tests.put("/a/b/ce/d/e/f", true|false);
        tests.put("/a/b/cee/d/e/f", true|false);
        tests.put("/a/b/ce/", true|false);
        tests.put("/a/b/ce/f/g/h", true|false);

        for (String testPath : tests.keySet()) {
            assertMatch(globPattern, testPath, tests.get(testPath));
        }
    }

    @Test
    public void testWildcard6() throws Exception {
        RestrictionPattern globPattern = buildGlobPattern("/a/b/c", "*e/*");

        // EXERCISE: fill-in the expected result for the match (true or false) for the given set of paths:
        Map<String, Boolean> tests = new LinkedHashMap<>();
        tests.put("/a/b/ce/", true|false);
        tests.put("/a/b/c/d", true|false);
        tests.put("/a/b/c/d/e", true|false);
        tests.put("/a/b/cde/d/e/f", true|false);
        tests.put("/a/b/c/d/e/f", true|false);
        tests.put("/a/b/ced/d/e/f", true|false);
        tests.put("/a/b/cde", true|false);
        tests.put("/a/b/c/d/e/", true|false);
        tests.put("/a/b/c/d/f/f", true|false);
        tests.put("/a/b/c/ed/f/f", true|false);
        tests.put("/a/b/ceeeeeee/f/g/h", true|false);

        for (String testPath : tests.keySet()) {
            assertMatch(globPattern, testPath, tests.get(testPath));
        }
    }

    @Test
    public void testWildcard7() throws Exception {
        RestrictionPattern globPattern = buildGlobPattern("/a/b/c", "/*e/*");

        // EXERCISE: fill-in the expected result for the match (true or false) for the given set of paths:
        Map<String, Boolean> tests = new LinkedHashMap<>();
        tests.put("/a/b/cde", true|false);
        tests.put("/a/b/ced/d/e/f", true|false);
        tests.put("/a/b/cde/d/e/f", true|false);
        tests.put("/a/b/ce/", true|false);
        tests.put("/a/b/c/d/e/", true|false);
        tests.put("/a/b/c/d/e", true|false);
        tests.put("/a/b/c/d", true|false);
        tests.put("/a/b/c/d/e", true|false);
        tests.put("/a/b/c/d/e/f", true|false);
        tests.put("/a/b/c/d/f/f", true|false);
        tests.put("/a/b/c/de/f", true|false);
        tests.put("/a/b/c/ed/f/f", true|false);

        for (String testPath : tests.keySet()) {
            assertMatch(globPattern, testPath, tests.get(testPath));
        }
    }

    @Test
    public void testWildcard8() throws Exception {
        RestrictionPattern globPattern = buildGlobPattern("/a/b/c", "/*cat");

        // EXERCISE: fill-in the expected result for the match (true or false) for the given set of paths:
        Map<String, Boolean> tests = new LinkedHashMap<>();
        tests.put("/a/b/cat", true|false);
        tests.put("/a/b/ced/cat", true|false);
        tests.put("/a/b/c/cat", true|false);
        tests.put("/a/b/c/acat", true|false);
        tests.put("/a/b/c/d", true|false);
        tests.put("/a/b/c/d/cat/e", true|false);
        tests.put("/a/b/c/d/acat/e", true|false);
        tests.put("/a/b/c/d/cata/e", true|false);
        tests.put("/a/b/c/d/cate", true|false);
        tests.put("/a/b/cat/ed/f/f", true|false);
        tests.put("/a/b/c/f/cat", true|false);
        tests.put("/a/b/c/f/acat", true|false);

        for (String testPath : tests.keySet()) {
            assertMatch(globPattern, testPath, tests.get(testPath));
        }
    }

    @Test
    public void testWildcard9() throws Exception {
        RestrictionPattern globPattern = buildGlobPattern("/a/b/c", "/*/cat");

        // EXERCISE: fill-in the expected result for the match (true or false) for the given set of paths:
        Map<String, Boolean> tests = new LinkedHashMap<>();
        tests.put("/a/b/cat", true|false);
        tests.put("/a/b/c/cat", true|false);
        tests.put("/a/b/c/cate", true|false);
        tests.put("/a/b/c/acat", true|false);
        tests.put("/a/b/c/a/cat", true|false);
        tests.put("/a/b/c/cat/d", true|false);
        tests.put("/a/b/c/d/acat", true|false);
        tests.put("/a/b/c/d/cate", true|false);
        tests.put("/a/b/c/d/cat/e", true|false);
        tests.put("/a/b/c/d/acat/e", true|false);
        tests.put("/a/b/c/d/cata/e", true|false);
        tests.put("/a/b/c/d/e/f/cat", true|false);
        tests.put("/a/b/cat/ed/f/f", true|false);
        tests.put("/a/b/ced/cat", true|false);
        tests.put("/a/b/ced/f/cat", true|false);

        for (String testPath : tests.keySet()) {
            assertMatch(globPattern, testPath, tests.get(testPath));
        }
    }

    @Test
    public void testWildcard10() throws Exception {
        RestrictionPattern globPattern = buildGlobPattern("/a/b/c", "/cat*");

        // EXERCISE: fill-in the expected result for the match (true or false) for the given set of paths:
        Map<String, Boolean> tests = new LinkedHashMap<>();
        tests.put("/a/b/cat", true|false);
        tests.put("/a/b/cat/ed/f/f", true|false);
        tests.put("/a/b/ced/cat", true|false);
        tests.put("/a/b/ced/f/cat", true|false);
        tests.put("/a/b/c/cat", true | false);
        tests.put("/a/b/c/cats", true|false);
        tests.put("/a/b/c/d/cat", true|false);
        tests.put("/a/b/c/d/cats", true|false);
        tests.put("/a/b/c/d/e/cat", true|false);
        tests.put("/a/b/c/d/e/cats", true|false);
        tests.put("/a/b/c/acat", true|false);
        tests.put("/a/b/c/d/acat", true|false);
        tests.put("/a/b/c/d/cat/e", true|false);
        tests.put("/a/b/c/d/acat/e", true|false);
        tests.put("/a/b/c/d/cata/e", true|false);
        tests.put("/a/b/c/cat/s", true|false);
        tests.put("/a/b/c/cats/d/e/f", true|false);

        for (String testPath : tests.keySet()) {
            assertMatch(globPattern, testPath, tests.get(testPath));
        }
    }

    @Test
    public void testWildcardOnRoot() throws Exception {
        RestrictionPattern globPattern = buildGlobPattern("/", "*");

        // EXERCISE: fill-in the expected result for the match (true or false) for the given set of paths:
        Map<String, Boolean> tests = new LinkedHashMap<>();
        tests.put("/", true|false);
        tests.put("/a", true|false);
        tests.put("/b/", true|false);
        tests.put("/c/d", true|false);
        tests.put("/a/b/ce/", true|false);
        tests.put("/a/b/ce/f/g/h", true|false);
        tests.put("/a/b/ce/d/e/f", true|false);

        for (String testPath : tests.keySet()) {
            assertMatch(globPattern, testPath, tests.get(testPath));
        }
    }

    @Test
    public void testEmptyOnRoot() throws Exception {
        RestrictionPattern globPattern = buildGlobPattern("/", "");

        // EXERCISE: fill-in the expected result for the match (true or false) for the given set of paths:
        Map<String, Boolean> tests = new LinkedHashMap<>();
        tests.put("/", true|false);
        tests.put("/a", true|false);
        tests.put("/a/b/c", true|false);
        tests.put("/a/b/c/d", true|false);
        tests.put("/a/b/c/d/e", true|false);
        tests.put("/a/b/c/d/e/f", true|false);
        tests.put("/a/b/cde", true|false);

        for (String testPath : tests.keySet()) {
            assertMatch(globPattern, testPath, tests.get(testPath));
        }
    }

    @Test
    public void testEmpty() throws Exception {
        RestrictionPattern globPattern = buildGlobPattern("/a/b/c", "");

        // EXERCISE: fill-in the expected result for the match (true or false) for the given set of paths:
        Map<String, Boolean> tests = new LinkedHashMap<>();
        tests.put("/", true|false);
        tests.put("/a", true|false);
        tests.put("/a/b/c", true|false);
        tests.put("/a/b/cde", true|false);
        tests.put("/a/b/c/d", true|false);
        tests.put("/a/b/c/d/e", true|false);
        tests.put("/a/b/c/d/e/f", true|false);

        for (String testPath : tests.keySet()) {
            assertMatch(globPattern, testPath, tests.get(testPath));
        }
    }

    @Test
    public void testPath() throws Exception {
        RestrictionPattern globPattern = buildGlobPattern("/a/b/c", "d");

        // EXERCISE: fill-in the expected result for the match (true or false) for the given set of paths:
        Map<String, Boolean> tests = new LinkedHashMap<>();
        tests.put("/", true|false);
        tests.put("/a", true|false);
        tests.put("/a/b/c", true|false);
        tests.put("/a/b/c/d", true|false);
        tests.put("/a/b/c/d/e/f", true|false);
        tests.put("/a/b/cd", true|false);
        tests.put("/a/b/cde", true|false);
        tests.put("/a/b/cd/e", true|false);
        tests.put("/a/b/cd/e/f", true|false);

        for (String testPath : tests.keySet()) {
            assertMatch(globPattern, testPath, tests.get(testPath));
        }
    }

    @Test
    public void testPath2() throws Exception {
        RestrictionPattern globPattern = buildGlobPattern("/a/b/c", "/d");

        // EXERCISE: fill-in the expected result for the match (true or false) for the given set of paths:
        Map<String, Boolean> tests = new LinkedHashMap<>();
        tests.put("/", true|false);
        tests.put("/a", true|false);
        tests.put("/a/b/c", true|false);
        tests.put("/a/b/cd", true|false);
        tests.put("/a/b/cde", true|false);
        tests.put("/a/b/c/d", true|false);
        tests.put("/a/b/c/d/e/f", true|false);
        tests.put("/a/b/cd/e", true|false);
        tests.put("/a/b/cd/e/f", true|false);

        for (String testPath : tests.keySet()) {
            assertMatch(globPattern, testPath, tests.get(testPath));
        }
    }

    @Test
    public void testPath3() throws Exception {
        RestrictionPattern globPattern = buildGlobPattern("/a/b/c", "/d/");

        // EXERCISE: fill-in the expected result for the match (true or false) for the given set of paths:
        Map<String, Boolean> tests = new LinkedHashMap<>();
        tests.put("/", true|false);
        tests.put("/a", true|false);
        tests.put("/a/b/c", true|false);
        tests.put("/a/b/c/d", true|false);
        tests.put("/a/b/c/d/e", true|false);
        tests.put("/a/b/c/d/e/f", true|false);
        tests.put("/a/b/cd", true|false);
        tests.put("/a/b/cd/e", true|false);
        tests.put("/a/b/cd/e/f", true|false);
        tests.put("/a/b/cde", true|false);

        for (String testPath : tests.keySet()) {
            assertMatch(globPattern, testPath, tests.get(testPath));
        }
    }

    private RestrictionPattern buildGlobPattern(@NotNull String path, @NotNull String glob) throws Exception {
        RestrictionProvider rp = getConfig(AuthorizationConfiguration.class).getRestrictionProvider();
        Restriction restriction = rp.createRestriction(path, AccessControlConstants.REP_GLOB, getValueFactory(root).createValue(glob));

        return rp.getPattern(path, ImmutableSet.of(restriction));
    }

    private static void assertMatch(RestrictionPattern pattern, String testPath, boolean expectedResult) {
        assertEquals("Pattern : " + pattern + "; TestPath : " + testPath, expectedResult, pattern.matches(testPath));
    }
}