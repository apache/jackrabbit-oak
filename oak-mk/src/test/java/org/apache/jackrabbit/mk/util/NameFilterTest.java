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
package org.apache.jackrabbit.mk.util;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests the NameFilter utility class.
 */
public class NameFilterTest {

    @Test
    public void test() {
        NameFilter filter = new NameFilter(new String[]{"foo*", "-foo99"});
        assertTrue(filter.matches("foo1"));
        assertTrue(filter.matches("foo*"));
        assertTrue(filter.matches("foo bar"));
        assertTrue(filter.matches("foo 99"));
        assertFalse(filter.matches("foo99"));
        assertTrue(filter.containsWildcard());

        filter = new NameFilter(new String[]{"*foo"});
        assertTrue(filter.matches("foo"));
        assertTrue(filter.matches("-123foo"));
        assertFalse(filter.matches("bar"));
        assertTrue(filter.containsWildcard());

        filter = new NameFilter(new String[]{"foo\\*bar"});
        assertFalse(filter.matches("foo bar"));
        assertTrue(filter.matches("foo*bar"));
        assertFalse(filter.containsWildcard());

        filter = new NameFilter(new String[]{"foo\\bar"});
        assertTrue(filter.matches("foo\\bar"));
        assertFalse(filter.containsWildcard());

        filter = new NameFilter(new String[]{"foo\\"});
        assertTrue(filter.matches("foo\\"));
        assertFalse(filter.containsWildcard());

        filter = new NameFilter(new String[]{"*"});
        assertTrue(filter.matches("*"));
        assertTrue(filter.matches("\\*"));
        assertTrue(filter.matches("blah"));
        assertTrue(filter.containsWildcard());

        filter = new NameFilter(new String[]{"\\*"});
        assertTrue(filter.matches("*"));
        assertFalse(filter.matches("\\*"));
        assertFalse(filter.matches("blah"));
        assertFalse(filter.containsWildcard());

        filter = new NameFilter(new String[]{"\\- topic"});
        assertTrue(filter.matches("- topic"));
        assertFalse(filter.containsWildcard());

        filter = new NameFilter(new String[]{"*", "- topic"});
        assertFalse(filter.matches(" topic"));
        assertTrue(filter.matches("- topic"));
        assertTrue(filter.matches("blah"));
        assertTrue(filter.containsWildcard());

        filter = new NameFilter(new String[]{"foo\\-bar"});
        assertFalse(filter.matches("foo-bar"));
        assertTrue(filter.matches("foo\\-bar"));
        assertFalse(filter.containsWildcard());

        filter = new NameFilter(new String[]{"foo\\\\*bar"});
        assertTrue(filter.matches("foo\\*bar"));
        assertFalse(filter.matches("foo\\ blah bar"));
        assertFalse(filter.matches("foo*bar"));
        assertFalse(filter.containsWildcard());
    }
}
