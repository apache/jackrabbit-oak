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

import java.util.Collections;

import org.junit.Test;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class CompositeMatcherTest {

    @Test
    public void empty() throws Exception{
        Matcher m = CompositeMatcher.compose(Collections.<Matcher>emptyList());
        assertFalse(m.isMatch());
    }

    @Test(expected = IllegalArgumentException.class)
    public void multiWithFailing() throws Exception{
        CompositeMatcher.compose(asList(new Include("x").createMatcher(), Matcher.NON_MATCHING));
    }

    @Test
    public void multi() throws Exception{
        Matcher m = CompositeMatcher.compose(asList(
                new Include("x/z").createMatcher(),
                new Include("x/y").createMatcher())
        );

        Matcher m2 = m.next("x");
        assertTrue(m2.isMatch());
        assertEquals("x", m2.getMatchedPath());
        assertEquals(1, m2.depth());

        assertFalse(m.next("a").isMatch());

        Matcher m3 = m2.next("y");
        assertTrue(m3.isMatch());
        assertEquals("x/y", m3.getMatchedPath());
        assertEquals(2, m3.depth());

        Matcher m4 = m3.next("a");
        assertFalse(m4.isMatch());
    }

    @Test
    public void matchChildren() throws Exception{
        //Hypothetical case. First pattern is redundant
        Matcher m = CompositeMatcher.compose(asList(
                new Include("x/z").createMatcher(),
                new Include("x/*").createMatcher())
        );

        assertFalse(m.matchesAllChildren());
        assertTrue(m.next("x").matchesAllChildren());
    }

}