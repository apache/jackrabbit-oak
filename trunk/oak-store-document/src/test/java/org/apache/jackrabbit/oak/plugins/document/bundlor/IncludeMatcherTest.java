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

import static org.junit.Assert.*;

public class IncludeMatcherTest {

    @Test
    public void singleLevel() throws Exception{
        Matcher m = new Include("x").createMatcher();
        assertTrue(m.isMatch());
        assertTrue(m.next("x").isMatch());
        assertEquals("x", m.next("x").getMatchedPath());

        //Next level does not match
        assertFalse(m.next("x").next("x").isMatch());

        //Same level different path element name does not match
        assertFalse(m.next("y").isMatch());
    }

    @Test
    public void includeAll() throws Exception{
        Matcher m = new Include("x/**").createMatcher();

        assertTrue(m.isMatch());
        assertTrue(m.next("x").isMatch());
        assertTrue(m.next("x").next("x").isMatch());
        assertTrue(m.next("x").next("y").isMatch());
        assertTrue(m.next("x").next("y").next("z").isMatch());

        assertEquals("x/y/z", m.next("x").next("y").next("z").getMatchedPath());
    }

}