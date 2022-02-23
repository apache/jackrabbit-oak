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

import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class GlobsPatternTest {
    
    private static final String PATH = "/a/b/c";
    
    private static final Map<String, List<String>> MATCH = new HashMap<>();
    private static final Map<String, List<String>> NO_MATCH = new HashMap<>();
    static {
        GlobPatternTest.createWildcardTests().forEach((globPattern, tests) -> {
            String path = extractPath(globPattern);
            if (PATH.equals(path)) {
                String restriction = extractRestriction(globPattern);
                List<String> matching = new ArrayList<>();
                List<String> notMatching = new ArrayList<>();
                tests.forEach((testPath, expectedMatch) -> {
                    if (expectedMatch) {
                        matching.add(testPath);
                    } else {
                        notMatching.add(testPath);
                    }
                });
                MATCH.put(restriction, matching);
                NO_MATCH.put(restriction, notMatching);
            }
        });
    }
    
    private static String extractPath(@NotNull GlobPattern globPattern) {
        String s = globPattern.toString();
        return s.substring(0, s.indexOf(":")-1);
    }

    private static String extractRestriction(@NotNull GlobPattern globPattern) {
        String s = globPattern.toString();
        return s.substring(s.indexOf(":")+2);
    }
    
    private static String getAnyRestriction(@NotNull Map<String, List<String>> m) {
        return Iterables.get(m.keySet(), new Random().nextInt(m.size()));
    }
    
    @Test
    public void testAllMatching() {
        GlobsPattern gp = new GlobsPattern(PATH, MATCH.keySet());
        MATCH.values().forEach(testPaths -> testPaths.forEach(testPath -> assertTrue(gp.matches(testPath))));
    }

    @Test
    public void testNoMatching() {
        String rest1 = getAnyRestriction(NO_MATCH);
        String rest2 = getAnyRestriction(NO_MATCH);
        
        GlobsPattern gp = new GlobsPattern(PATH, Arrays.asList(rest1, rest2));

        GlobPattern p1 = GlobPattern.create(PATH, rest1);
        NO_MATCH.get(rest2).forEach(testPath -> {
            boolean expectedMatch = p1.matches(testPath);
            assertEquals(expectedMatch, gp.matches(testPath));
        });    
    }

    @Test
    public void testMixedMatching() {
        String rest1 = getAnyRestriction(MATCH);
        String rest2 = getAnyRestriction(NO_MATCH);
        GlobsPattern gp = new GlobsPattern(PATH, Arrays.asList(rest1, rest2));
        
        GlobPattern p1 = GlobPattern.create(PATH, rest1);
        // test-paths for the rest1 will always match
        MATCH.get(rest1).forEach(testPath -> assertTrue(gp.matches(testPath)));
        // test-pathss for the non-matching restriction will match if they match rest1
        NO_MATCH.get(rest2).forEach(testPath -> {
            boolean expectedMatch = p1.matches(testPath);
            assertEquals(expectedMatch, gp.matches(testPath));
        });
    }

    @Test
    public void testMatchTreeProperty() {
        String rest1 = getAnyRestriction(MATCH);
        String rest2 = getAnyRestriction(MATCH);
        GlobsPattern gp = new GlobsPattern(PATH, Arrays.asList(rest1, rest2));

        MATCH.get(rest1).forEach(testPath -> {
            Tree t = when(mock(Tree.class).getPath()).thenReturn(testPath).getMock();
            assertTrue(gp.matches(t, null));
            verify(t).getPath();
            verifyNoMoreInteractions(t);
        });

        MATCH.get(rest2).forEach(testPath -> {
            String parentPath = PathUtils.getParentPath(testPath);
            Tree t = when(mock(Tree.class).getPath()).thenReturn(parentPath).getMock();
            PropertyState ps = when(mock(PropertyState.class).getName()).thenReturn(PathUtils.getName(testPath)).getMock();
            assertTrue(gp.matches(t, ps));
            verify(t).getPath();
            verify(ps).getName();
            verifyNoMoreInteractions(t, ps);
        });
    }
    
    @Test
    public void testEmptyValues() {
        GlobsPattern gp = new GlobsPattern(PATH, Collections.emptyList());
        assertNoMatch(gp);
    }

    @Test
    public void testNullValues() {
        GlobsPattern gp = new GlobsPattern(PATH, Collections.singletonList(null));
        assertNoMatch(gp);
    }
    
    private static void assertNoMatch(@NotNull GlobsPattern gp) {
        assertFalse(gp.matches("/"));
        assertFalse(gp.matches(PATH));
        assertFalse(gp.matches(PATH + "-sibling"));
        assertFalse(gp.matches(PATH + "/sub/tree"));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testMaxOccurrences() {
        GlobsPattern gp = new GlobsPattern("/", 
                Arrays.asList("simple/restriction", GlobPatternTest.restrictionExcessiveWildcard()));
        gp.matches("/1/2/3/4/5/6/7/8/9/10/11/12/13/14/15/16/17/18/19/20/21");
    }
    
    @Test
    public void testMatches() {
        GlobsPattern gp = new GlobsPattern("/path", Collections.emptyList());
        assertFalse(gp.matches());
    }

    @Test
    public void testHashCode() {
        String rest1 = getAnyRestriction(MATCH);
        String rest2 = getAnyRestriction(NO_MATCH);
        List<String> rests = Arrays.asList(rest1, rest2);
        
        GlobsPattern pattern = new GlobsPattern(PATH, rests);
        
        assertEquals(pattern.hashCode(), new GlobsPattern(PATH, rests).hashCode());
        assertNotEquals(pattern.hashCode(), new GlobsPattern(PATH, Collections.emptyList()).hashCode());
        assertNotEquals(pattern.hashCode(), new GlobsPattern(PATH, Collections.singletonList(rest2)).hashCode());
        assertNotEquals(pattern.hashCode(), new GlobsPattern("/different/path", rests).hashCode());
    }

    @Test
    public void testEquals() {
        String rest1 = getAnyRestriction(MATCH);
        String rest2 = getAnyRestriction(NO_MATCH);
        List<String> rests = Arrays.asList(rest1, rest2);

        GlobsPattern pattern = new GlobsPattern(PATH, rests);
        
        assertEquals(pattern, pattern);
        assertEquals(pattern, new GlobsPattern(PATH, rests));
    }

    @Test
    public void testNotEquals() {
        String rest1 = getAnyRestriction(MATCH);
        String rest2 = getAnyRestriction(NO_MATCH);
        List<String> rests = Arrays.asList(rest1, rest2);

        GlobsPattern pattern = new GlobsPattern(PATH, rests);
        
        // different tree path
        assertNotEquals(pattern, new GlobsPattern("/another/path", rests));
        // different set of subtrees
        assertNotEquals(pattern, new GlobsPattern(PATH, Collections.emptyList()));
        assertNotEquals(pattern, new GlobsPattern(PATH, Collections.singletonList(rest1)));
        assertNotEquals(pattern, new GlobsPattern(PATH, Arrays.asList(rest1, rest2, getAnyRestriction(NO_MATCH))));
        // different restriction
        assertNotEquals(new GlobsPattern(PATH, Collections.singletonList(rest1)), GlobPattern.create(PATH, rest1));
    }
}