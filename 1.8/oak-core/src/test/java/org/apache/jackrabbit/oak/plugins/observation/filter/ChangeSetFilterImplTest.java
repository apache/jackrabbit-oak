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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.observation.ChangeSet;
import org.apache.jackrabbit.oak.spi.observation.ChangeSetBuilder;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChangeSetFilterImplTest {

    private static final Logger LOG = LoggerFactory.getLogger(ChangeSetFilterImplTest.class);

    /** shortcut for creating a set of strings */
    private Set<String> s(String... entries) {
        return new HashSet<String>(Arrays.asList(entries));
    }
    
    private ChangeSet newChangeSet(int maxPathDepth, Set<String> parentPaths,
            Set<String> parentNodeNames,
            Set<String> parentNodeTypes,
            Set<String> propertyNames) {
        return newChangeSet(maxPathDepth, parentPaths, parentNodeNames, parentNodeTypes, propertyNames, s());
    }
    
    private ChangeSet newChangeSet(int maxPathDepth, Set<String> parentPaths,
            Set<String> parentNodeNames,
            Set<String> parentNodeTypes,
            Set<String> propertyNames,
            Set<String> allNodeTypes) {
        ChangeSetBuilder changeSetBuilder = new ChangeSetBuilder(Integer.MAX_VALUE, maxPathDepth);
        for (String path : parentPaths){
            changeSetBuilder.addParentPath(path);
        }
        for (String nodeName : parentNodeNames){
            changeSetBuilder.addParentNodeName(nodeName);
        }
        for (String parentNodeType : parentNodeTypes){
            changeSetBuilder.addParentNodeType(parentNodeType);
        }
        for (String propertyName : propertyNames){
            changeSetBuilder.addPropertyName(propertyName);
        }
        for (String nodeType : allNodeTypes){
            changeSetBuilder.addNodeType(nodeType);
        }
        return changeSetBuilder.build();
    }
    
    private ChangeSetBuilder newBuilder(int maxItems, int maxPathDepth) {
        return new ChangeSetBuilder(maxItems, maxPathDepth);
    }

    private ChangeSetBuilder overflowAllNodeTypes(ChangeSetBuilder builder) {
        int i = 0;
        while (!builder.isAllNodeTypeOverflown()) {
            builder.addNodeType("foo" + i++);
        }
        return builder;
    }

    private ChangeSetBuilder overflowParentNodeTypes(ChangeSetBuilder builder) {
        int i = 0;
        while (!builder.isParentNodeTypeOverflown()) {
            builder.addParentNodeType("foo" + i++);
        }
        return builder;
    }

    private ChangeSetBuilder overflowParentNodeNames(ChangeSetBuilder builder) {
        int i = 0;
        while (!builder.isParentNodeNameOverflown()) {
            builder.addParentNodeName("foo" + i++);
        }
        return builder;
    }

    private ChangeSetBuilder overflowParentPaths(ChangeSetBuilder builder) {
        int i = 0;
        while (!builder.isParentPathOverflown()) {
            builder.addParentPath("foo" + i++);
        }
        return builder;
    }

    private ChangeSetBuilder overflowPropertyNames(ChangeSetBuilder builder) {
        int i = 0;
        while (!builder.isPropertyNameOverflown()) {
            builder.addPropertyName("foo" + i++);
        }
        return builder;
    }

    @Test
    public void testIsDeepFalse() throws Exception {
        ChangeSetFilterImpl prefilter = new ChangeSetFilterImpl(s("/"), false, null, s("/excluded"), s(), s(), s());
        
        assertTrue(prefilter.excludes(newChangeSet(5, s("/child1", "/child2"), s("child1", "child2"), s(), s())));
        assertFalse(prefilter.excludes(newChangeSet(5, s("/", "/child2"), s("child2"), s(), s())));
    }

    @Test
    public void testParentPathsIncludeExclude() throws Exception {
        ChangeSetFilterImpl prefilter = new ChangeSetFilterImpl(s("/"), true, null, s("/excluded"), s(), s(), s());
        assertFalse(prefilter.excludes(newChangeSet(5, s("/a", "/b"), s("a", "b"), s(), s())));
        assertTrue(prefilter.excludes(newChangeSet(5, s("/excluded/foo", "/excluded/bar"), s("foo", "bar"), s(), s())));
        
        prefilter = new ChangeSetFilterImpl(s("/included"), true, null, s("/excluded"), s(), s(), s());
        assertTrue(prefilter.excludes(newChangeSet(5, s("/a", "/b"), s(), s(), s())));
        assertFalse(prefilter.excludes(newChangeSet(5, s("/included/a", "/included/b"), s(), s(), s())));
        assertTrue(prefilter.excludes(newChangeSet(5, s("/excluded/foo", "/excluded/bar"), s(), s(), s())));

        prefilter = new ChangeSetFilterImpl(s("/foo/**/included/**"), true /*ignored for globs */, null, s("/excluded"), s(), s(), s());
        assertTrue(prefilter.excludes(newChangeSet(5, s("/a", "/b"), s(), s(), s())));
        assertTrue(prefilter.excludes(newChangeSet(5, s("/included/a", "/included/b"), s(), s(), s())));
        assertFalse(prefilter.excludes(newChangeSet(5, s("/foo/included/a"), s(), s(), s())));
        assertTrue(prefilter.excludes(newChangeSet(5, s("/included/b"), s(), s(), s())));
        assertFalse(prefilter.excludes(newChangeSet(5, s("/foo/bar/included/a", "/included/b"), s(), s(), s())));
        assertTrue(prefilter.excludes(newChangeSet(5, s("/excluded/foo", "/excluded/bar"), s(), s(), s())));

        prefilter = new ChangeSetFilterImpl(s("/main/**/included"), true, null, s("/main/excluded"), s(), s(), s());
        assertTrue(prefilter.excludes(newChangeSet(5, s("/main", "/main/foo"), s(), s(), s())));
        assertFalse(prefilter.excludes(newChangeSet(5, s("/main/included", "/main/excluded"), s(), s(), s())));
        assertTrue(prefilter.excludes(newChangeSet(5, s("/main/excluded/included", "/main/excluded"), s(), s(), s())));

        prefilter = new ChangeSetFilterImpl(s("/main/included/**"), true, null, s("/main/excluded"), s(), s(), s());
        assertTrue(prefilter.excludes(newChangeSet(5, s("/main", "/main/foo"), s(), s(), s())));
        assertTrue(prefilter.excludes(newChangeSet(5, s("/main/excluded"), s(), s(), s())));
        assertFalse(prefilter.excludes(newChangeSet(5, s("/main/included"), s(), s(), s())));
        assertTrue(prefilter.excludes(newChangeSet(5, s("/main/excluded/included", "/main/excluded"), s(), s(), s())));

        prefilter = new ChangeSetFilterImpl(s("/main/inc-*/**"), true, null, s("/main/excluded"), s(), s(), s());
        assertTrue(prefilter.excludes(newChangeSet(5, s("/main", "/main/foo"), s(), s(), s())));
        assertTrue(prefilter.excludes(newChangeSet(5, s("/main/excluded"), s(), s(), s())));
        assertFalse(prefilter.excludes(newChangeSet(5, s("/main/inc-luded"), s(), s(), s())));
        assertTrue(prefilter.excludes(newChangeSet(5, s("/main/excluded/included", "/main/excluded"), s(), s(), s())));
    }
    
    @Test
    public void testParentNodeNames() throws Exception {
        ChangeSetFilterImpl prefilter = new ChangeSetFilterImpl(s("/"), true, null, s(), s("foo", "bar"), s(), s());
        assertFalse(prefilter.excludes(newChangeSet(5, s("/a/foo", "/b"), s("foo", "b"), s(), s())));
        assertTrue(prefilter.excludes(newChangeSet(5, s("/a/zoo", "/b"), s("zoo", "b"), s(), s())));
        assertFalse(prefilter.excludes(newChangeSet(5, s("/a/zoo", "/bar"), s("zoo", "bar"), s(), s())));
    }

    @Test
    public void testParentNodeTypes() throws Exception {
        ChangeSetFilterImpl prefilter = new ChangeSetFilterImpl(s("/"), true, null, s(), s(), s("nt:folder"), s());
        assertTrue(prefilter.excludes(newChangeSet(5, s("/a"), s("a"), s("nt:unstructured"), s())));
        assertFalse(prefilter.excludes(newChangeSet(5, s("/a"), s("a"), s("nt:folder"), s())));
    }

    @Test
    public void testPropertyNames() throws Exception {
        ChangeSetFilterImpl prefilter = new ChangeSetFilterImpl(s("/"), true, null, s(), s(), s(), s("jcr:data"));
        assertTrue(prefilter.excludes(newChangeSet(5, s("/a"), s("a"), s(), s("myProperty"))));
        assertFalse(prefilter.excludes(newChangeSet(5, s("/a"), s("a"), s(), s("jcr:data"))));
    }

    @Test
    public void testOverflowing() throws Exception {
        ChangeSetBuilder builder = newBuilder(5, 5);
        assertTrue(overflowAllNodeTypes(builder).isAllNodeTypeOverflown());
        assertTrue(overflowParentNodeTypes(builder).isParentNodeTypeOverflown());
        assertTrue(overflowParentNodeNames(builder).isParentNodeNameOverflown());
        assertTrue(overflowParentPaths(builder).isParentPathOverflown());
        assertTrue(overflowPropertyNames(builder).isPropertyNameOverflown());
    }
    
    private ChangeSetBuilder sampleBuilder() {
        ChangeSetBuilder builder = newBuilder(5, 5);
        builder.addNodeType("nt:file");
        builder.addParentNodeType("nt:file");
        builder.addParentPath("/bar");
        builder.addParentNodeName("bar");
        builder.addPropertyName("a");
        builder.addPropertyName("b");
        return builder;
    }

    @Test
    public void testIncludeOnAllNodeTypeOverflow() throws Exception {
        ChangeSetBuilder builder = sampleBuilder();
        ChangeSetFilterImpl prefilter = new ChangeSetFilterImpl(s("/"), true, null, s("/excluded"), s("foo", "bars"), s("nt:file"), s());
        assertTrue(prefilter.excludes(builder.build()));
        overflowAllNodeTypes(builder);
        assertFalse(prefilter.excludes(builder.build()));
    }

    @Test
    public void testIncludeOnParentNodeNameOverflow() throws Exception {
        ChangeSetBuilder builder = sampleBuilder();
        ChangeSetFilterImpl prefilter = new ChangeSetFilterImpl(s("/"), true, null, s("/excluded"), s("foo", "bars"), s("nt:file"), s());
        assertTrue(prefilter.excludes(builder.build()));
        overflowParentNodeNames(builder);
        assertFalse(prefilter.excludes(builder.build()));
    }

    @Test
    public void testIncludeOnPropertyNamesOverflow() throws Exception {
        ChangeSetBuilder builder = sampleBuilder();
        ChangeSetFilterImpl prefilter = new ChangeSetFilterImpl(s("/"), true, null, s("/excluded"), s("foo", "bars"), s("nt:file"), s());
        assertTrue(prefilter.excludes(builder.build()));
        overflowPropertyNames(builder);
        assertFalse(prefilter.excludes(builder.build()));
    }

    @Test
    public void testIncludeOnParentNodeTypeOverflow() throws Exception {
        ChangeSetBuilder builder = sampleBuilder();
        ChangeSetFilterImpl prefilter = new ChangeSetFilterImpl(s("/"), true, null, s("/excluded"), s("foo", "bars"), s("nt:file"), s());
        assertTrue(prefilter.excludes(builder.build()));
        overflowParentNodeTypes(builder);
        assertFalse(prefilter.excludes(builder.build()));
    }

    @Test
    public void testIncludeOnParentPathsOverflow() throws Exception {
        ChangeSetBuilder builder = sampleBuilder();
        ChangeSetFilterImpl prefilter = new ChangeSetFilterImpl(s("/"), true, null, s("/excluded"), s("foo", "bars"), s("nt:file"), s());
        assertTrue(prefilter.excludes(builder.build()));
        overflowParentPaths(builder);
        assertFalse(prefilter.excludes(builder.build()));
    }
    
    @Test
    public void testUnpreciseInclude() throws Exception {
        ChangeSetBuilder builder = newBuilder(5, 5);
        builder.addNodeType("nt:file");
        builder.addParentNodeType("nt:file");
        builder.addParentPath("/a/b/c/e");
        builder.addParentNodeName("d");
        builder.addPropertyName("e");
        builder.addPropertyName("f");
        Set<String> largeExcludeSet = new HashSet<String>();
        for(int a=0;a<3;a++) {
            for(int b=0;b<3;b++) {
                for(int c=0;c<10;c++) {
                    String s = "/a";
                    if (a > 0) {
                        s += a;
                    }
                    s += "/b";
                    if (b > 0) {
                        s += b;
                    }
                    s += "/c";
                    if (c > 0) {
                        s += c;
                    }
                    s += "/d";
                    largeExcludeSet.add(s);
                }
            }
        }
        ChangeSetFilterImpl prefilter = new ChangeSetFilterImpl(s("/"), true, null, largeExcludeSet, s("foo", "bars"), s("nt:file"), s(), 999);
        assertTrue(prefilter.excludes(builder.build()));

        prefilter = new ChangeSetFilterImpl(s("/"), true, null, largeExcludeSet, s("foo", "bars"), s("nt:file"), s(), 15);
        assertFalse(prefilter.excludes(builder.build()));

        prefilter = new ChangeSetFilterImpl(s("/"), true, null, largeExcludeSet, s("foo", "bars"), s("nt:file"), s(), 1);
        assertFalse(prefilter.excludes(builder.build()));
    }
    
    @Test
    public void testDeepPaths() throws Exception {
        doTestDeepPath("/a/b/c/d/e/f/g/h/i/j/k/l", "/a", 5, false);
        doTestDeepPath("/a/b/c/d/e/f/g/h/i/j/k/l", "/a/b", 5, false);
        doTestDeepPath("/a/b/c/d/e/f/g/h/i/j/k/l", "/a/b/c", 5, false);
        doTestDeepPath("/a/b/c/d/e/f/g/h/i/j/k/l", "/a/b/c/d", 5, false);
        doTestDeepPath("/a/b/c/d/e/f/g/h/i/j/k/l", "/a/b/c/d/e", 5, false);
        doTestDeepPath("/a/b/c/d/e/f/g/h/i/j/k/l", "/a/b/c/d/e/f", 5, false);
        doTestDeepPath("/a/b/c/d/e/f/g/h/i/j/k/l", "/a/b/c/d/e/f/g", 5, false);
        doTestDeepPath("/a/b/c/d/e/f/g/h/i/j/k/l", "/a/b/c/d/e/f/g/h", 5, false);
        
        for (int maxPathDepth = 1; maxPathDepth < 13; maxPathDepth++) {
            doTestDeepPath("/a/b/c/d/e/f/g/h/i/j/k/l", "/a/b/c/d/e/f/g/h", maxPathDepth, false);
        }

        doTestDeepPath("/a/b/c/d/e/f/g/h/i/j/k/l", "/x", 15, true);
        doTestDeepPath("/a/b/c/d/e/f/g/h/i/j/k/l", "/a/x", 15, true);
        doTestDeepPath("/a/b/c/d/e/f/g/h/i/j/k/l", "/a/b/x", 15, true);
        doTestDeepPath("/a/b/c/d/e/f/g/h/i/j/k/l", "/a/b/c/x", 15, true);

        doTestDeepPath("/a/b/c/d/e/f/g/h/i/j/k/l", "/x", 9, true);
        doTestDeepPath("/a/b/c/d/e/f/g/h/i/j/k/l", "/a/x", 9, false);
    }

    private void doTestDeepPath(String changeSetPath, String includePath, int maxPathDepth, boolean expectExclude) {
        ChangeSetBuilder builder = newBuilder(5, maxPathDepth);
        builder.addNodeType("nt:file");
        builder.addParentNodeType("nt:file");
        builder.addParentPath("/bar");
        builder.addParentNodeName("bar");
        builder.addPropertyName("a");
        builder.addPropertyName("b");
        builder.addParentPath(changeSetPath);
        builder.addParentNodeName(PathUtils.getName(changeSetPath));
        ChangeSetFilterImpl prefilter = new ChangeSetFilterImpl(s(includePath), true, null, s("/excluded"), s("foo", "bars", "l"), s("nt:file"), s());
        if (expectExclude) {
            assertTrue(prefilter.excludes(builder.build()));
        } else {
            assertFalse(prefilter.excludes(builder.build()));
        }
    }

    @Test
    public void manyIncludePaths() throws Exception {
        int numPaths = 50;
        ChangeSetBuilder builder = newBuilder(50, 9);
        for (int i = 0; i < numPaths; i++) {
            builder.addParentPath("/a/b/c/d/e/n" + i);
        }
        ChangeSet cs = builder.build();

        Set<String> includes = Sets.newHashSet();
        for (int i = 0; i < 100; i++) {
            includes.add("/foo/bar/n-" + i + "/*.jsp");
        }
        ChangeSetFilter filter = new ChangeSetFilterImpl(s(),true,
                includes, s(), s(), s(), s());

        // warm up
        doManyIncludePaths(filter, cs);

        // and measure
        Stopwatch sw = Stopwatch.createStarted();
        doManyIncludePaths(filter, cs);
        LOG.info("manyIncludePaths() took {}", sw.stop());
    }

    private void doManyIncludePaths(ChangeSetFilter filter, ChangeSet cs)
            throws Exception {
        for (int i = 0; i < 20000; i++) {
            assertTrue(filter.excludes(cs));
        }
    }
}
