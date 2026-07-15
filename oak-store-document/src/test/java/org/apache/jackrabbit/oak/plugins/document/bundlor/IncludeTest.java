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

package org.apache.jackrabbit.oak.plugins.document.bundlor;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IncludeTest {

    @Test
    public void simpleWildcard() throws Exception{
        Include i = new Include("*");
        assertTrue(i.match("x"));
        assertTrue(i.match("/x"));
        assertFalse(i.match("/x/y"));
    }

    @Test
    public void exactName() throws Exception{
        assertTrue(new Include("x").match("x"));
        assertFalse(new Include("x").match("y"));

        assertTrue(new Include("x/y").match("x"));
        assertFalse(new Include("x/y").match("y"));
        assertTrue(new Include("x/y").match("x/y"));
    }

    @Test
    public void directive() throws Exception{
        Include i0 = new Include("x/*");
        assertEquals(Include.Directive.NONE, i0.getDirective());

        Include i = new Include("x/**");
        assertEquals(Include.Directive.ALL, i.getDirective());
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidDirective() throws Exception{
        new Include("x/y;all/z");
    }

    @Test
    public void directiveAll() throws Exception{
        Include i = new Include("x/**");
        assertTrue(i.match("x/y"));
        assertTrue(i.match("x/y/z"));
        assertTrue(i.match("x/y/z/x"));

        Include i2 = new Include("x/y/**");
        assertTrue(i2.match("x/y"));
        assertTrue(i2.match("x/y/z"));
        assertTrue(i2.match("x/y/z/x"));
    }

    @Test
    public void depth() throws Exception{
        Include i0 = new Include("x/*");
        assertEquals(0, i0.createMatcher().depth());
        assertEquals(1, i0.createMatcher().next("x").depth());
        assertEquals(2, i0.createMatcher().next("x").next("y").depth());

        // x/y/z would not match so depth should be 0
        assertEquals(0, i0.createMatcher().next("x").next("y").next("z").depth());

        Include i2 = new Include("x/y/**");
        assertEquals(0, i2.createMatcher().depth());
        assertEquals(1, i2.createMatcher().next("x").depth());
        assertEquals(2, i2.createMatcher().next("x").next("y").depth());
        assertEquals(3, i2.createMatcher().next("x").next("y").next("z").depth());
    }

    @Test
    public void matchChildren() throws Exception{
        Include i0 = new Include("x/*");
        Matcher m = i0.createMatcher();
        assertFalse(m.matchesAllChildren());
        assertTrue(m.next("x").matchesAllChildren());

        Include i1 = new Include("x/**");
        m = i1.createMatcher();
        assertFalse(m.matchesAllChildren());
        assertTrue(m.next("x").matchesAllChildren());

    }
}