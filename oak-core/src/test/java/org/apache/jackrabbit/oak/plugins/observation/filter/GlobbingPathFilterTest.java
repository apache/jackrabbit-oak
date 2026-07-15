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

import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.observation.filter.GlobbingPathFilter.STAR;
import static org.apache.jackrabbit.oak.plugins.observation.filter.GlobbingPathFilter.STAR_STAR;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.oak.plugins.tree.impl.ImmutableTree;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class GlobbingPathFilterTest {

    private ImmutableTree tree;

    @Before
    public void setup() {
        NodeBuilder root = EMPTY_NODE.builder();
        createPath(root, "a/b/c/d");
        createPath(root, "q");
        createPath(root, "x/y/x/y/z");
        createPath(root, "r/s/t/u/v/r/s/t/u/v/r/s/t/u/v/w");
        tree = new ImmutableTree(root.getNodeState());
    }

    private static void createPath(NodeBuilder root, String path) {
        NodeBuilder builder = root;
        for (String name : elements(path)) {
            builder = builder.setChildNode(name);
        }
    }

    /**
     * An empty path pattern should match no path
     */
    @Test
    public void emptyMatchesNothing() {
        EventFilter rootFilter = new GlobbingPathFilter("");
        NodeState a = tree.getChild("a").getNodeState();
        assertFalse(rootFilter.includeAdd("a", a));
        assertNull(rootFilter.create("a", a, a));
    }

    /**
     * q should match q
     */
    @Test
    public void singleMatchesSingle() {
        EventFilter filter = new GlobbingPathFilter("q");

        assertTrue(filter.includeAdd("q", tree.getNodeState()));
    }

    /**
     * * should match q
     */
    @Test
    public void starMatchesSingle() {
        EventFilter filter = new GlobbingPathFilter(STAR);

        assertTrue(filter.includeAdd("q", tree.getNodeState()));
    }
    
    @Test
    public void wildcardMatches() {
        EventFilter filter = new GlobbingPathFilter("*.*");
        assertFalse(filter.includeAdd("a", tree.getNodeState()));
        assertTrue(filter.includeAdd(".b", tree.getNodeState()));
        assertTrue(filter.includeAdd("a.b", tree.getNodeState()));
        assertTrue(filter.includeAdd("a.", tree.getNodeState()));
        
        filter = new GlobbingPathFilter("*.html");
        assertFalse(filter.includeAdd("a.b", tree.getNodeState()));
        assertFalse(filter.includeAdd("html", tree.getNodeState()));
        assertTrue(filter.includeAdd(".html", tree.getNodeState()));
        assertTrue(filter.includeAdd("a.html", tree.getNodeState()));

        filter = new GlobbingPathFilter("*foo*.html");
        assertFalse(filter.includeAdd("a.b", tree.getNodeState()));
        assertFalse(filter.includeAdd("a.html", tree.getNodeState()));
        assertTrue(filter.includeAdd("foo.html", tree.getNodeState()));
        assertTrue(filter.includeAdd("my-foo-bar.html", tree.getNodeState()));
    }

    /**
     * match ** 'in the middle'
     */
    @Test
    public void inTheMiddle() {
        EventFilter filter = new GlobbingPathFilter("/foo/"+STAR_STAR+"/bar");
        ImmutableTree t = tree;

        for(String name : elements("foo/a/b/c")) {
            t = t.getChild(name);
            assertFalse(filter.includeAdd(name, t.getNodeState()));
            filter = filter.create(name, t.getNodeState(), t.getNodeState());
            assertNotNull(filter);
        }

        for(String name : elements("bar")) {
            t = t.getChild(name);
            assertTrue(filter.includeAdd(name, t.getNodeState()));
            filter = filter.create(name, t.getNodeState(), t.getNodeState());
            assertNotNull(filter);
        }
    }

    /**
     * ** should match every path
     */
    @Test
    public void all() {
        EventFilter filter = new GlobbingPathFilter(STAR_STAR);
        ImmutableTree t = tree;

        for(String name : elements("a/b/c/d")) {
            t = t.getChild(name);
            assertTrue(filter.includeAdd(name, t.getNodeState()));
            filter = filter.create(name, t.getNodeState(), t.getNodeState());
            assertNotNull(filter);
        }
    }

    /**
     * a/b/c should match a/b/c
     */
    @Test
    public void literal() {
        EventFilter rootFilter = new GlobbingPathFilter("a/b/c");
        NodeState a = tree.getChild("a").getNodeState();
        assertFalse(rootFilter.includeAdd("a", a));

        EventFilter aFilter = rootFilter.create("a", a, a);
        assertNotNull(aFilter);
        NodeState b = a.getChildNode("b");
        assertFalse(aFilter.includeAdd("b", b));

        EventFilter bFilter = aFilter.create("b", b, b);
        assertNotNull(bFilter);
        NodeState c = b.getChildNode("c");
        assertTrue(bFilter.includeAdd("c", b));
        assertFalse(bFilter.includeAdd("x", b));

        assertNull(bFilter.create("c", c, c));
    }

    /**
     * a/*&#47c should match a/b/c
     */
    @Test
    public void starGlob() {
        EventFilter rootFilter = new GlobbingPathFilter("a/*/c");
        NodeState a = tree.getChild("a").getNodeState();
        assertFalse(rootFilter.includeAdd("a", a));

        EventFilter aFilter = rootFilter.create("a", a, a);
        assertNotNull(aFilter);
        NodeState b = a.getChildNode("b");
        assertFalse(aFilter.includeAdd("b", b));

        EventFilter bFilter = aFilter.create("b", b, b);
        assertNotNull(bFilter);
        NodeState c = b.getChildNode("c");
        assertTrue(bFilter.includeAdd("c", b));
        assertFalse(bFilter.includeAdd("x", b));

        assertNull(bFilter.create("c", c, c));
    }

    /**
     * **&#47/y/z should match x/y/x/y/z
     */
    @Test
    public void starStarGlob() {
        EventFilter rootFilter = new GlobbingPathFilter("**/y/z");
        NodeState x1 = tree.getChild("x").getNodeState();
        assertFalse(rootFilter.includeAdd("x", x1));

        EventFilter x1Filter = rootFilter.create("x", x1, x1);
        assertNotNull(x1Filter);
        NodeState y1 = x1.getChildNode("y");
        assertFalse(x1Filter.includeAdd("y", y1));

        EventFilter y1Filter = x1Filter.create("y", y1, y1);
        assertNotNull(y1Filter);
        NodeState x2 = y1.getChildNode("x");
        assertFalse(y1Filter.includeAdd("x", x2));

        EventFilter x2Filter = y1Filter.create("x", x2, x2);
        assertNotNull(x2Filter);
        NodeState y2 = x2.getChildNode("y");
        assertFalse(x2Filter.includeAdd("y", y2));

        EventFilter y2Filter = x2Filter.create("y", y2, y2);
        assertNotNull(y2Filter);
        NodeState z = y2.getChildNode("z");
        assertTrue(y2Filter.includeAdd("z", z));

        EventFilter zFilter = (y2Filter.create("z", z, z));
        assertFalse(zFilter.includeAdd("x", EMPTY_NODE));
    }

    /**
     * **&#47a/b/c should match a/b/c
     */
    @Test
    public void matchAtStart() {
        EventFilter rootFilter = new GlobbingPathFilter("**/a/b/c");
        NodeState a = tree.getChild("a").getNodeState();
        assertFalse(rootFilter.includeAdd("a", a));

        EventFilter aFilter = rootFilter.create("a", a, a);
        assertNotNull(aFilter);
        NodeState b = a.getChildNode("b");
        assertFalse(aFilter.includeAdd("b", b));

        EventFilter bFilter = aFilter.create("b", b, b);
        assertNotNull(bFilter);
        NodeState c = b.getChildNode("c");
        assertTrue(bFilter.includeAdd("c", b));
        assertFalse(bFilter.includeAdd("x", b));
    }

    /**
     * **&#47r/s/t/u/v should match r/s/t/u/v and r/s/t/u/v/r/s/t/u/v and r/s/t/u/v/r/s/t/u/v/r/s/t/u/v
     */
    @Test
    public void multipleMatches() {
        EventFilter filter = new GlobbingPathFilter("**/r/s/t/u/v");
        ImmutableTree t = tree;

        for(int c = 0; c < 2; c++) {
            for(String name : elements("r/s/t/u")) {
                t = t.getChild(name);
                assertFalse(filter.includeAdd(name, t.getNodeState()));
                filter = filter.create(name, t.getNodeState(), t.getNodeState());
                assertNotNull(filter);
            }

            t = t.getChild("v");
            assertTrue(filter.includeAdd("v", t.getNodeState()));
            filter = filter.create("v", t.getNodeState(), t.getNodeState());
            assertNotNull(filter);
        }
    }

    /**
     * **&#47r/s/t/u/v/w should match r/s/t/u/v/r/s/t/u/v/r/s/t/u/v/w
     */
    @Test
    public void matchAtEnd() {
        EventFilter filter = new GlobbingPathFilter("**/r/s/t/u/v/w");
        ImmutableTree t = tree;

        for(String name : elements("r/s/t/u/v/r/s/t/u/v/r/s/t/u/v")) {
            t = t.getChild(name);
            assertFalse(filter.includeAdd(name, t.getNodeState()));
            filter = filter.create(name, t.getNodeState(), t.getNodeState());
            assertNotNull(filter);
        }

        t = t.getChild("w");
        assertTrue(filter.includeAdd("w", t.getNodeState()));
        filter = filter.create("w", t.getNodeState(), t.getNodeState());
        assertNotNull(filter);
    }

    /**
     * r/s/t&#47** should match r/s/t and all its descendants
     */
    @Test
    public void matchSuffix() {
        EventFilter filter = new GlobbingPathFilter("a/b/c/d/r/s/t/**");
        ImmutableTree t = tree;

        for(String name : elements("a/b/c/d/r/s")) {
            t = t.getChild(name);
            assertFalse(filter.includeAdd(name, t.getNodeState()));
            filter = filter.create(name, t.getNodeState(), t.getNodeState());
            assertNotNull(filter);
        }

        for (String name: elements("t/u/v/r/s/t/u/v/r/s/t/u/v/w")) {
            t = t.getChild(name);
            assertTrue(filter.includeAdd(name, t.getNodeState()));
            filter = filter.create(name, t.getNodeState(), t.getNodeState());
            assertNotNull(filter);
        }
    }

    @Test
    @Ignore("Manual test for OAK-5589")
    public void constructorPerformance() throws Exception {
        final int NUM = 5000000;
        final long start = System.currentTimeMillis();
        for(int i=0; i<NUM; i++) {
            matchSuffix();
            matchAtEnd();
        }
        final long end = System.currentTimeMillis();
        final long diff = end - start;
        System.out.println(NUM + " iterations took " + diff + "ms.");
        // old version: 5000000 iterations took 33-34sec
        // new version: 5000000 iterations took 23-24sec -> ca 25-30% faster
    }

}
