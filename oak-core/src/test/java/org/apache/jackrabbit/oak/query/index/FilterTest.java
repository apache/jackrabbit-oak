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
package org.apache.jackrabbit.oak.query.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Random;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.junit.Test;

/**
 * Tests the Filter class.
 */
public class FilterTest {

    @Test
    public void propertyRestriction() {

        PropertyValue one = PropertyValues.newString("1");
        PropertyValue two = PropertyValues.newString("2");

        FilterImpl f = FilterImpl.newTestInstance();
        assertTrue(null == f.getPropertyRestriction("x"));
        f.restrictProperty("x", Operator.LESS_OR_EQUAL, two);
        assertEquals(
                "Filter(, path=*, property=[x=[..2]]])", 
                f.toString());
        f.restrictProperty("x", Operator.GREATER_OR_EQUAL, one);
        assertEquals(
                "Filter(, path=*, property=[x=[..2], [1..]])", 
                f.toString());

        // no change, as the same restrictions already were added
        f.restrictProperty("x", Operator.LESS_OR_EQUAL, two);
        assertEquals(
                "Filter(, path=*, property=[x=[..2], [1..]])", 
                f.toString());
        f.restrictProperty("x", Operator.GREATER_OR_EQUAL, one);
        assertEquals(
                "Filter(, path=*, property=[x=[..2], [1..]])", 
                f.toString());

        f.restrictProperty("x", Operator.GREATER_THAN, one);
        assertEquals(
                "Filter(, path=*, property=[x=[..2], [1.., (1..]])", 
                f.toString());
        f.restrictProperty("x", Operator.LESS_THAN, two);
        assertEquals(
                "Filter(, path=*, property=[x=[..2], [1.., (1.., ..2)]])", 
                f.toString());

        // TODO could replace / remove the old range conditions,
        // if there is an overlap
        f.restrictProperty("x", Operator.EQUAL, two);
        assertEquals(
                "Filter(, path=*, property=[x=[..2], [1.., (1.., ..2), 2]])", 
                f.toString());

        f = FilterImpl.newTestInstance();
        f.restrictProperty("x", Operator.EQUAL, one);
        assertEquals(
                "Filter(, path=*, property=[x=[1]])", 
                f.toString());
        f.restrictProperty("x", Operator.EQUAL, one);
        assertEquals(
                "Filter(, path=*, property=[x=[1]])", 
                f.toString());
        
        // TODO could replace / remove the old range conditions,
        // if there is an overlap
        f.restrictProperty("x", Operator.GREATER_OR_EQUAL, one);
        assertEquals(
                "Filter(, path=*, property=[x=[1, [1..]])", 
                f.toString());
        f.restrictProperty("x", Operator.LESS_OR_EQUAL, one);
        assertEquals(
                "Filter(, path=*, property=[x=[1, [1.., ..1]]])", 
                f.toString());
        
        // TODO could replace / remove the old range conditions,
        // if there is an overlap
        f.restrictProperty("x", Operator.GREATER_THAN, one);
        assertEquals(
                "Filter(, path=*, property=[x=[1, [1.., ..1], (1..]])", 
                f.toString());

        f = FilterImpl.newTestInstance();
        f.restrictProperty("x", Operator.EQUAL, one);
        assertEquals(
                "Filter(, path=*, property=[x=[1]])", 
                f.toString());

        // TODO could replace / remove the old range conditions,
        // if there is an overlap
        f.restrictProperty("x", Operator.LESS_THAN, one);
        assertEquals(
                "Filter(, path=*, property=[x=[1, ..1)]])", 
                f.toString());

        f = FilterImpl.newTestInstance();
        f.restrictProperty("x", Operator.NOT_EQUAL, null);
        assertEquals(
                "Filter(, path=*, property=[x=[is not null]])", 
                f.toString());
        f.restrictProperty("x", Operator.LESS_THAN, one);
        assertEquals(
                "Filter(, path=*, property=[x=[is not null, ..1)]])", 
                f.toString());
        
        // this should replace the range with an equality
        // (which is faster, and correct even when using multi-valued properties)
        f.restrictProperty("x", Operator.EQUAL, two);
        assertEquals(
                "Filter(, path=*, property=[x=[is not null, ..1), 2]])", 
                f.toString());

    }

    @Test
    public void pathRestrictionsRandomized() throws Exception {
        ArrayList<String> paths = new ArrayList<String>();
        // create paths /a, /b, /c, /a/a, /a/b, ... /c/c/c
        paths.add("/");
        for (int i = 'a'; i <= 'c'; i++) {
            String p1 = "/" + (char) i;
            paths.add(p1);
            for (int j = 'a'; j <= 'c'; j++) {
                String p2 = "/" + (char) j;
                paths.add(p1 + p2);
                for (int k = 'a'; k <= 'c'; k++) {
                    String p3 = "/" + (char) k;
                    paths.add(p1 + p2 + p3);
                }
            }
        }
        Random r = new Random(1);
        for (int i = 0; i < 10000; i++) {
            String p1 = paths.get(r.nextInt(paths.size()));
            String p2 = paths.get(r.nextInt(paths.size()));
            Filter.PathRestriction r1 = Filter.PathRestriction.values()[r
                    .nextInt(Filter.PathRestriction.values().length)];
            Filter.PathRestriction r2 = Filter.PathRestriction.values()[r
                    .nextInt(Filter.PathRestriction.values().length)];
            FilterImpl f1 = FilterImpl.newTestInstance();
            f1.restrictPath(p1, r1);
            FilterImpl f2 = FilterImpl.newTestInstance();
            f2.restrictPath(p2, r2);
            FilterImpl fc = FilterImpl.newTestInstance();
            fc.restrictPath(p1, r1);
            fc.restrictPath(p2, r2);
            int tooMany = 0;
            for (String p : paths) {
                boolean expected = f1.testPath(p) && f2.testPath(p);
                boolean got = fc.testPath(p);
                if (expected == got) {
                    // good
                } else if (expected && !got) {
                    fc = FilterImpl.newTestInstance();
                    fc.restrictPath(p1, r1);
                    fc.restrictPath(p2, r2);
                    fail("not matched: " + p1 + "/" + r1.name() + " && " + p2
                            + "/" + r2.name());
                } else {
                    // not great, but not a problem
                    tooMany++;
                }
            }
            if (tooMany > 3) {
                fail("too many matches: " + p1 + "/" + r1.name() + " && " + p2
                        + "/" + r2.name() + " superfluous: " + tooMany);
            }
        }
    }

    @Test
    public void pathRestrictions() throws Exception {
        FilterImpl f = FilterImpl.newTestInstance();
        assertEquals("/", f.getPath());
        assertEquals(Filter.PathRestriction.NO_RESTRICTION,
                f.getPathRestriction());

        f.restrictPath("/test", Filter.PathRestriction.ALL_CHILDREN);
        f.restrictPath("/test2", Filter.PathRestriction.ALL_CHILDREN);
        assertTrue(f.isAlwaysFalse());

        f = FilterImpl.newTestInstance();
        f.restrictPath("/test", Filter.PathRestriction.ALL_CHILDREN);
        assertEquals("/test", f.getPath());
        assertEquals(Filter.PathRestriction.ALL_CHILDREN,
                f.getPathRestriction());
        f.restrictPath("/test/x", Filter.PathRestriction.DIRECT_CHILDREN);
        assertEquals("/test/x", f.getPath());
        assertEquals(Filter.PathRestriction.DIRECT_CHILDREN,
                f.getPathRestriction());
        f.restrictPath("/test/x/y", Filter.PathRestriction.PARENT);
        assertEquals("/test/x/y", f.getPath());
        assertEquals(Filter.PathRestriction.PARENT, f.getPathRestriction());

        f = FilterImpl.newTestInstance();
        f.restrictPath("/test", Filter.PathRestriction.DIRECT_CHILDREN);
        f.restrictPath("/test/x/y", Filter.PathRestriction.PARENT);
        assertEquals("/test/x/y", f.getPath());
        assertEquals(Filter.PathRestriction.PARENT, f.getPathRestriction());
        f.restrictPath("/test/y", Filter.PathRestriction.DIRECT_CHILDREN);
        assertTrue(f.isAlwaysFalse());

        f = FilterImpl.newTestInstance();
        f.restrictPath("/test/x/y", Filter.PathRestriction.PARENT);
        f.restrictPath("/test/x", Filter.PathRestriction.EXACT);
        assertEquals("/test/x", f.getPath());
        assertEquals(Filter.PathRestriction.EXACT, f.getPathRestriction());
        f.restrictPath("/test/y", Filter.PathRestriction.EXACT);
        assertTrue(f.isAlwaysFalse());

        f = FilterImpl.newTestInstance();
        f.restrictPath("/test", Filter.PathRestriction.ALL_CHILDREN);
        f.restrictPath("/test", Filter.PathRestriction.PARENT);
        assertTrue(f.isAlwaysFalse());

        f = FilterImpl.newTestInstance();
        f.restrictPath("/test/x", Filter.PathRestriction.PARENT);
        f.restrictPath("/test", Filter.PathRestriction.ALL_CHILDREN);
        assertEquals("/test/x", f.getPath());
        assertEquals(Filter.PathRestriction.PARENT, f.getPathRestriction());
        f.restrictPath("/test/x", Filter.PathRestriction.ALL_CHILDREN);
        assertTrue(f.isAlwaysFalse());

        f = FilterImpl.newTestInstance();
        f.restrictPath("/test", Filter.PathRestriction.ALL_CHILDREN);
        f.restrictPath("/test", Filter.PathRestriction.EXACT);
        assertTrue(f.isAlwaysFalse());

        f = FilterImpl.newTestInstance();
        f.restrictPath("/test", Filter.PathRestriction.DIRECT_CHILDREN);
        f.restrictPath("/test/x", Filter.PathRestriction.EXACT);
        assertEquals("/test/x", f.getPath());
        assertEquals(Filter.PathRestriction.EXACT, f.getPathRestriction());

        f = FilterImpl.newTestInstance();
        f.restrictPath("/test", Filter.PathRestriction.DIRECT_CHILDREN);
        f.restrictPath("/test/x/y", Filter.PathRestriction.EXACT);
        assertTrue(f.isAlwaysFalse());

        f = FilterImpl.newTestInstance();
        f.restrictPath("/test/x", Filter.PathRestriction.PARENT);
        f.restrictPath("/", Filter.PathRestriction.ALL_CHILDREN);
        assertEquals("/test/x", f.getPath());
        assertEquals(Filter.PathRestriction.PARENT, f.getPathRestriction());
        f.restrictPath("/test/y", Filter.PathRestriction.EXACT);
        assertTrue(f.isAlwaysFalse());

        f = FilterImpl.newTestInstance();
        f.restrictPath("/test", Filter.PathRestriction.DIRECT_CHILDREN);
        assertEquals("/test", f.getPath());
        assertEquals(Filter.PathRestriction.DIRECT_CHILDREN,
                f.getPathRestriction());
        f.restrictPath("/", Filter.PathRestriction.ALL_CHILDREN);
        assertEquals("/test", f.getPath());
        assertEquals(Filter.PathRestriction.DIRECT_CHILDREN,
                f.getPathRestriction());
        f.restrictPath("/test", Filter.PathRestriction.ALL_CHILDREN);
        assertEquals("/test", f.getPath());
        assertEquals(Filter.PathRestriction.DIRECT_CHILDREN,
                f.getPathRestriction());
        f.restrictPath("/test/x/y", Filter.PathRestriction.PARENT);
        assertEquals("/test/x/y", f.getPath());
        assertEquals(Filter.PathRestriction.PARENT, f.getPathRestriction());
        f.restrictPath("/test2", Filter.PathRestriction.ALL_CHILDREN);
        assertTrue(f.isAlwaysFalse());

        f = FilterImpl.newTestInstance();
        f.restrictPath("/test/x", Filter.PathRestriction.EXACT);
        assertEquals("/test/x", f.getPath());
        assertEquals(Filter.PathRestriction.EXACT, f.getPathRestriction());
        f.restrictPath("/test", Filter.PathRestriction.ALL_CHILDREN);
        f.restrictPath("/test", Filter.PathRestriction.DIRECT_CHILDREN);
        f.restrictPath("/test/x/y", Filter.PathRestriction.PARENT);
        f.restrictPath("/test/y", Filter.PathRestriction.DIRECT_CHILDREN);
        assertTrue(f.isAlwaysFalse());

        f = FilterImpl.newTestInstance();
        f.restrictPath("/test/x/y", Filter.PathRestriction.PARENT);
        assertEquals("/test/x/y", f.getPath());
        assertEquals(Filter.PathRestriction.PARENT, f.getPathRestriction());
        f.restrictPath("/test/x", Filter.PathRestriction.PARENT);
        assertEquals("/test/x", f.getPath());
        assertEquals(Filter.PathRestriction.PARENT, f.getPathRestriction());
        f.restrictPath("/test", Filter.PathRestriction.ALL_CHILDREN);
        assertEquals("/test/x", f.getPath());
        assertEquals(Filter.PathRestriction.PARENT, f.getPathRestriction());
        f.restrictPath("/test", Filter.PathRestriction.DIRECT_CHILDREN);
        assertEquals("/test/x", f.getPath());
        assertEquals(Filter.PathRestriction.PARENT, f.getPathRestriction());
        f.restrictPath("/test/x", Filter.PathRestriction.PARENT);
        assertEquals("/test/x", f.getPath());
        assertEquals(Filter.PathRestriction.PARENT, f.getPathRestriction());
        f.restrictPath("/test", Filter.PathRestriction.PARENT);
        assertEquals("/test", f.getPath());
        assertEquals(Filter.PathRestriction.PARENT, f.getPathRestriction());
        f.restrictPath("/test2", Filter.PathRestriction.EXACT);
        assertTrue(f.isAlwaysFalse());

    }

}
