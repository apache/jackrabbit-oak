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
package org.apache.jackrabbit.oak.jcr.observation;

import static org.junit.Assert.assertEquals;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

public class OakEventFilterImplTest {

    @Test
    public void testAddAncestorPaths() throws Exception {
        // parent of / doesnt exist, hence no ancestor path. "" will anyway resolve to "/" in FilterBuilder.getSubTrees()
        assertMatches(new String[] {}, "/");
        
        assertMatches(new String[] {"/*"}, "/*");
        assertMatches(new String[] {"/**"}, "/**");
        assertMatches(new String[] {"/a"}, "/a");
        assertMatches(new String[] {"/a", "/a/b"}, "/a/b");
        assertMatches(new String[] {"/a", "/a/*"}, "/a/*");
        assertMatches(new String[] {"/a", "/a/**"}, "/a/**");
        assertMatches(new String[] {"/a", "/a/b", "/a/b/c"}, "/a/b/c");
        assertMatches(new String[] {"/a", "/a/b", "/a/b/*"}, "/a/b/*");
        assertMatches(new String[] {"/a", "/a/b", "/a/b/**"}, "/a/b/**");
        assertMatches(new String[] {"/a", "/a/b", "/a/b/**"}, "/a/b/**/d");
        assertMatches(new String[] {"/a", "/a/b", "/a/b/**"}, "/a/b/**/d/**");
        assertMatches(new String[] {"/a", "/a/b", "/a/b/**"}, "/a/b/**/d/**/f");
        assertMatches(new String[] {"/a", "/a/b", "/a/b/c", "/a/b/c/d"}, "/a/b/c/d");
        assertMatches(new String[] {"/a", "/a/b", "/a/b/c", "/a/b/c/*"}, "/a/b/c/*");
        assertMatches(new String[] {"/a", "/a/b", "/a/b/c", "/a/b/c/**"}, "/a/b/c/**");
    }

    private void assertMatches(String[] expectedPaths, String globPath) {
        Set<String> ancestorPaths = new HashSet<String>();
        OakEventFilterImpl.addAncestorPaths(ancestorPaths, globPath);
        Set<String> expected = new HashSet<String>(Arrays.asList(expectedPaths));
        assertEquals(expected, ancestorPaths);
    }
}
