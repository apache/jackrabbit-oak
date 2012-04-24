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

import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.index.Filter.PathRestriction;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests the Filter class.
 */
public class FilterTest extends AbstractQueryTest {

    @Test
    public void propertyRestriction() {
        CoreValue one = vf.createValue("1");
        CoreValue two = vf.createValue("2");

        Filter f = new Filter(null);
        assertTrue(null == f.getPropertyRestriction("x"));
        f.restrictProperty("x", Operator.LESS_OR_EQUAL, two);
        assertEquals("..2]", f.getPropertyRestriction("x").toString());
        f.restrictProperty("x", Operator.GREATER_OR_EQUAL, one);
        assertEquals("[1..2]", f.getPropertyRestriction("x").toString());
        f.restrictProperty("x", Operator.GREATER_THAN, one);
        assertEquals("(1..2]", f.getPropertyRestriction("x").toString());
        f.restrictProperty("x", Operator.LESS_THAN, two);
        assertEquals("(1..2)", f.getPropertyRestriction("x").toString());
        f.restrictProperty("x", Operator.EQUAL, two);
        assertTrue(f.isAlwaysFalse());

        f = new Filter(null);
        f.restrictProperty("x", Operator.EQUAL, one);
        assertEquals("[1..1]", f.getPropertyRestriction("x").toString());
        f.restrictProperty("x", Operator.EQUAL, one);
        assertEquals("[1..1]", f.getPropertyRestriction("x").toString());
        f.restrictProperty("x", Operator.GREATER_OR_EQUAL, one);
        assertEquals("[1..1]", f.getPropertyRestriction("x").toString());
        f.restrictProperty("x", Operator.LESS_OR_EQUAL, one);
        assertEquals("[1..1]", f.getPropertyRestriction("x").toString());
        f.restrictProperty("x", Operator.GREATER_THAN, one);
        assertTrue(f.isAlwaysFalse());

        f = new Filter(null);
        f.restrictProperty("x", Operator.EQUAL, one);
        assertEquals("[1..1]", f.getPropertyRestriction("x").toString());
        f.restrictProperty("x", Operator.LESS_THAN, one);
        assertTrue(f.isAlwaysFalse());

        f = new Filter(null);
        f.restrictProperty("x", Operator.NOT_EQUAL, null);
        assertEquals("..", f.getPropertyRestriction("x").toString());
        f.restrictProperty("x", Operator.LESS_THAN, one);
        assertEquals("..1)", f.getPropertyRestriction("x").toString());
        f.restrictProperty("x", Operator.EQUAL, two);
        assertTrue(f.isAlwaysFalse());

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
            PathRestriction r1 = PathRestriction.values()[r.nextInt(PathRestriction.values().length)];
            PathRestriction r2 = PathRestriction.values()[r.nextInt(PathRestriction.values().length)];
            Filter f1 = new Filter(null);
            f1.restrictPath(p1, r1);
            Filter f2 = new Filter(null);
            f2.restrictPath(p2, r2);
            Filter fc = new Filter(null);
            fc.restrictPath(p1, r1);
            fc.restrictPath(p2, r2);
            int tooMany = 0;
            for (String p : paths) {
                boolean expected = f1.testPath(p) && f2.testPath(p);
                boolean got = fc.testPath(p);
                if (expected == got) {
                    // good
                } else if (expected && !got) {
                    fc = new Filter(null);
                    fc.restrictPath(p1, r1);
                    fc.restrictPath(p2, r2);
                    fail("not matched: " + p1 + "/" + r1.name() + " && " + p2 + "/" + r2.name());
                } else {
                    // not great, but not a problem
                    tooMany++;
                }
            }
            if (tooMany > 3) {
                fail("too many matches: " + p1 + "/" + r1.name() + " && " + p2 + "/" + r2.name() + " superfluous: " + tooMany);
            }
        }
    }

    @Test
    public void pathRestrictions() throws Exception {
        Filter f = new Filter(null);
        assertEquals("/", f.getPath());
        assertEquals(PathRestriction.ALL_CHILDREN, f.getPathRestriction());

        f.restrictPath("/test", PathRestriction.ALL_CHILDREN);
        f.restrictPath("/test2", PathRestriction.ALL_CHILDREN);
        assertTrue(f.isAlwaysFalse());

        f = new Filter(null);
        f.restrictPath("/test", PathRestriction.ALL_CHILDREN);
        assertEquals("/test", f.getPath());
        assertEquals(PathRestriction.ALL_CHILDREN, f.getPathRestriction());
        f.restrictPath("/test/x", PathRestriction.DIRECT_CHILDREN);
        assertEquals("/test/x", f.getPath());
        assertEquals(PathRestriction.DIRECT_CHILDREN, f.getPathRestriction());
        f.restrictPath("/test/x/y", PathRestriction.PARENT);
        assertEquals("/test/x/y", f.getPath());
        assertEquals(PathRestriction.PARENT, f.getPathRestriction());

        f = new Filter(null);
        f.restrictPath("/test", PathRestriction.DIRECT_CHILDREN);
        f.restrictPath("/test/x/y", PathRestriction.PARENT);
        assertEquals("/test/x/y", f.getPath());
        assertEquals(PathRestriction.PARENT, f.getPathRestriction());
        f.restrictPath("/test/y", PathRestriction.DIRECT_CHILDREN);
        assertTrue(f.isAlwaysFalse());

        f = new Filter(null);
        f.restrictPath("/test/x/y", PathRestriction.PARENT);
        f.restrictPath("/test/x", PathRestriction.EXACT);
        assertEquals("/test/x", f.getPath());
        assertEquals(PathRestriction.EXACT, f.getPathRestriction());
        f.restrictPath("/test/y", PathRestriction.EXACT);
        assertTrue(f.isAlwaysFalse());

        f = new Filter(null);
        f.restrictPath("/test", PathRestriction.ALL_CHILDREN);
        f.restrictPath("/test", PathRestriction.PARENT);
        assertTrue(f.isAlwaysFalse());

        f = new Filter(null);
        f.restrictPath("/test/x", PathRestriction.PARENT);
        f.restrictPath("/test", PathRestriction.ALL_CHILDREN);
        assertEquals("/test/x", f.getPath());
        assertEquals(PathRestriction.PARENT, f.getPathRestriction());
        f.restrictPath("/test/x", PathRestriction.ALL_CHILDREN);
        assertTrue(f.isAlwaysFalse());

        f = new Filter(null);
        f.restrictPath("/test", PathRestriction.ALL_CHILDREN);
        f.restrictPath("/test", PathRestriction.EXACT);
        assertTrue(f.isAlwaysFalse());

        f = new Filter(null);
        f.restrictPath("/test", PathRestriction.DIRECT_CHILDREN);
        f.restrictPath("/test/x", PathRestriction.EXACT);
        assertEquals("/test/x", f.getPath());
        assertEquals(PathRestriction.EXACT, f.getPathRestriction());

        f = new Filter(null);
        f.restrictPath("/test", PathRestriction.DIRECT_CHILDREN);
        f.restrictPath("/test/x/y", PathRestriction.EXACT);
        assertTrue(f.isAlwaysFalse());

        f = new Filter(null);
        f.restrictPath("/test/x", PathRestriction.PARENT);
        f.restrictPath("/", PathRestriction.ALL_CHILDREN);
        assertEquals("/test/x", f.getPath());
        assertEquals(PathRestriction.PARENT, f.getPathRestriction());
        f.restrictPath("/test/y", PathRestriction.EXACT);
        assertTrue(f.isAlwaysFalse());

        f = new Filter(null);
        f.restrictPath("/test", PathRestriction.DIRECT_CHILDREN);
        assertEquals("/test", f.getPath());
        assertEquals(PathRestriction.DIRECT_CHILDREN, f.getPathRestriction());
        f.restrictPath("/", PathRestriction.ALL_CHILDREN);
        assertEquals("/test", f.getPath());
        assertEquals(PathRestriction.DIRECT_CHILDREN, f.getPathRestriction());
        f.restrictPath("/test", PathRestriction.ALL_CHILDREN);
        assertEquals("/test", f.getPath());
        assertEquals(PathRestriction.DIRECT_CHILDREN, f.getPathRestriction());
        f.restrictPath("/test/x/y", PathRestriction.PARENT);
        assertEquals("/test/x/y", f.getPath());
        assertEquals(PathRestriction.PARENT, f.getPathRestriction());
        f.restrictPath("/test2", PathRestriction.ALL_CHILDREN);
        assertTrue(f.isAlwaysFalse());

        f = new Filter(null);
        f.restrictPath("/test/x", PathRestriction.EXACT);
        assertEquals("/test/x", f.getPath());
        assertEquals(PathRestriction.EXACT, f.getPathRestriction());
        f.restrictPath("/test", PathRestriction.ALL_CHILDREN);
        f.restrictPath("/test", PathRestriction.DIRECT_CHILDREN);
        f.restrictPath("/test/x/y", PathRestriction.PARENT);
        f.restrictPath("/test/y", PathRestriction.DIRECT_CHILDREN);
        assertTrue(f.isAlwaysFalse());

        f = new Filter(null);
        f.restrictPath("/test/x/y", PathRestriction.PARENT);
        assertEquals("/test/x/y", f.getPath());
        assertEquals(PathRestriction.PARENT, f.getPathRestriction());
        f.restrictPath("/test/x", PathRestriction.PARENT);
        assertEquals("/test/x", f.getPath());
        assertEquals(PathRestriction.PARENT, f.getPathRestriction());
        f.restrictPath("/test", PathRestriction.ALL_CHILDREN);
        assertEquals("/test/x", f.getPath());
        assertEquals(PathRestriction.PARENT, f.getPathRestriction());
        f.restrictPath("/test", PathRestriction.DIRECT_CHILDREN);
        assertEquals("/test/x", f.getPath());
        assertEquals(PathRestriction.PARENT, f.getPathRestriction());
        f.restrictPath("/test/x", PathRestriction.PARENT);
        assertEquals("/test/x", f.getPath());
        assertEquals(PathRestriction.PARENT, f.getPathRestriction());
        f.restrictPath("/test", PathRestriction.PARENT);
        assertEquals("/test", f.getPath());
        assertEquals(PathRestriction.PARENT, f.getPathRestriction());
        f.restrictPath("/test2", PathRestriction.EXACT);
        assertTrue(f.isAlwaysFalse());

    }

}
