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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.jackrabbit.oak.plugins.observation.ChangeSet;
import org.apache.jackrabbit.oak.plugins.observation.ChangeSetBuilder;
import org.junit.Test;

public class ChangeSetFilterImplTest {

    /** shortcut for creating a set of strings */
    private Set<String> s(String... entries) {
        return new HashSet<String>(Arrays.asList(entries));
    }
    
    private ChangeSet newChangeSet(int maxPathDepth, Set<String> parentPaths,
            Set<String> parentNodeNames,
            Set<String> parentNodeTypes,
            Set<String> propertyNames) {
        ChangeSetBuilder changeSetBuilder = new ChangeSetBuilder(Integer.MAX_VALUE, maxPathDepth);
        changeSetBuilder.getParentPaths().addAll(parentPaths);
        changeSetBuilder.getParentNodeNames().addAll(parentNodeNames);
        changeSetBuilder.getParentNodeTypes().addAll(parentNodeTypes);
        changeSetBuilder.getPropertyNames().addAll(propertyNames);
        return changeSetBuilder.build();
    }

    @Test
    public void testIsDeepFalse() throws Exception {
        ChangeSetFilterImpl prefilter = new ChangeSetFilterImpl(s("/"), false, s("/excluded"), s(), s(), s());
        
        assertTrue(prefilter.excludes(newChangeSet(5, s("/child1", "/child2"), s("child1", "child2"), s(), s())));
        assertFalse(prefilter.excludes(newChangeSet(5, s("/", "/child2"), s("child2"), s(), s())));
    }

    @Test
    public void testParentPathsIncludeExclude() throws Exception {
        ChangeSetFilterImpl prefilter = new ChangeSetFilterImpl(s("/"), true, s("/excluded"), s(), s(), s());
        assertFalse(prefilter.excludes(newChangeSet(5, s("/a", "/b"), s("a", "b"), s(), s())));
        assertTrue(prefilter.excludes(newChangeSet(5, s("/excluded/foo", "/excluded/bar"), s("foo", "bar"), s(), s())));
        
        prefilter = new ChangeSetFilterImpl(s("/included"), true, s("/excluded"), s(), s(), s());
        assertTrue(prefilter.excludes(newChangeSet(5, s("/a", "/b"), s(), s(), s())));
        assertFalse(prefilter.excludes(newChangeSet(5, s("/included/a", "/included/b"), s(), s(), s())));
        assertTrue(prefilter.excludes(newChangeSet(5, s("/excluded/foo", "/excluded/bar"), s(), s(), s())));

        prefilter = new ChangeSetFilterImpl(s("/foo/**/included/**"), true /*ignored for globs */, s("/excluded"), s(), s(), s());
        assertTrue(prefilter.excludes(newChangeSet(5, s("/a", "/b"), s(), s(), s())));
        assertTrue(prefilter.excludes(newChangeSet(5, s("/included/a", "/included/b"), s(), s(), s())));
        assertFalse(prefilter.excludes(newChangeSet(5, s("/foo/included/a"), s(), s(), s())));
        assertTrue(prefilter.excludes(newChangeSet(5, s("/included/b"), s(), s(), s())));
        assertFalse(prefilter.excludes(newChangeSet(5, s("/foo/bar/included/a", "/included/b"), s(), s(), s())));
        assertTrue(prefilter.excludes(newChangeSet(5, s("/excluded/foo", "/excluded/bar"), s(), s(), s())));

        prefilter = new ChangeSetFilterImpl(s("/main/**/included"), true, s("/main/excluded"), s(), s(), s());
        assertTrue(prefilter.excludes(newChangeSet(5, s("/main", "/main/foo"), s(), s(), s())));
        assertFalse(prefilter.excludes(newChangeSet(5, s("/main/included", "/main/excluded"), s(), s(), s())));
        assertTrue(prefilter.excludes(newChangeSet(5, s("/main/excluded/included", "/main/excluded"), s(), s(), s())));

        prefilter = new ChangeSetFilterImpl(s("/main/included/**"), true, s("/main/excluded"), s(), s(), s());
        assertTrue(prefilter.excludes(newChangeSet(5, s("/main", "/main/foo"), s(), s(), s())));
        assertTrue(prefilter.excludes(newChangeSet(5, s("/main/excluded"), s(), s(), s())));
        assertFalse(prefilter.excludes(newChangeSet(5, s("/main/included"), s(), s(), s())));
        assertTrue(prefilter.excludes(newChangeSet(5, s("/main/excluded/included", "/main/excluded"), s(), s(), s())));

        prefilter = new ChangeSetFilterImpl(s("/main/inc-*/**"), true, s("/main/excluded"), s(), s(), s());
        assertTrue(prefilter.excludes(newChangeSet(5, s("/main", "/main/foo"), s(), s(), s())));
        assertTrue(prefilter.excludes(newChangeSet(5, s("/main/excluded"), s(), s(), s())));
        assertFalse(prefilter.excludes(newChangeSet(5, s("/main/inc-luded"), s(), s(), s())));
        assertTrue(prefilter.excludes(newChangeSet(5, s("/main/excluded/included", "/main/excluded"), s(), s(), s())));
    }
    
    @Test
    public void testParentNodeNames() throws Exception {
        ChangeSetFilterImpl prefilter = new ChangeSetFilterImpl(s("/"), true, s(), s("foo", "bar"), s(), s());
        assertFalse(prefilter.excludes(newChangeSet(5, s("/a/foo", "/b"), s("foo", "b"), s(), s())));
        assertTrue(prefilter.excludes(newChangeSet(5, s("/a/zoo", "/b"), s("zoo", "b"), s(), s())));
        assertFalse(prefilter.excludes(newChangeSet(5, s("/a/zoo", "/bar"), s("zoo", "bar"), s(), s())));
    }

    @Test
    public void testParentNodeTypes() throws Exception {
        ChangeSetFilterImpl prefilter = new ChangeSetFilterImpl(s("/"), true, s(), s(), s("nt:folder"), s());
        assertTrue(prefilter.excludes(newChangeSet(5, s("/a"), s("a"), s("nt:unstructured"), s())));
        assertFalse(prefilter.excludes(newChangeSet(5, s("/a"), s("a"), s("nt:folder"), s())));
    }

    @Test
    public void testPropertyNames() throws Exception {
        ChangeSetFilterImpl prefilter = new ChangeSetFilterImpl(s("/"), true, s(), s(), s(), s("jcr:data"));
        assertTrue(prefilter.excludes(newChangeSet(5, s("/a"), s("a"), s(), s("myProperty"))));
        assertFalse(prefilter.excludes(newChangeSet(5, s("/a"), s("a"), s(), s("jcr:data"))));
    }

}
