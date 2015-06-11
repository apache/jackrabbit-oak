/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jackrabbit.oak.remote.filter;

import com.google.common.collect.Sets;
import org.junit.Test;

import java.util.HashSet;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FiltersTest {

    @Test(expected = IllegalArgumentException.class)
    public void testNullFilters() {
        new Filters(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullFilter() {
        new Filters(Sets.newHashSet((String) null));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyIncludeFilter() {
        new Filters(Sets.newHashSet(""));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyExcludeFilter() {
        new Filters(Sets.newHashSet("-"));
    }

    @Test
    public void testEmptyFilters() {
        Filters filters = new Filters(new HashSet<String>());

        assertTrue(filters.matches("foo"));
        assertTrue(filters.matches("bar"));
        assertTrue(filters.matches("baz"));
    }

    @Test
    public void testIncludeFilter() {
        Filters filters = new Filters(Sets.newHashSet("ba*"));

        assertFalse(filters.matches("foo"));
        assertTrue(filters.matches("bar"));
        assertTrue(filters.matches("baz"));
    }

    @Test
    public void testExcludeFilter() {
        Filters filters = new Filters(Sets.newHashSet("-foo"));

        assertFalse(filters.matches("foo"));
        assertTrue(filters.matches("bar"));
        assertTrue(filters.matches("baz"));
    }

    @Test
    public void testIncludeExcludeFilters() {
        Filters filters = new Filters(Sets.newHashSet("ba*", "-baz"));

        assertFalse(filters.matches("foo"));
        assertTrue(filters.matches("bar"));
        assertFalse(filters.matches("baz"));
    }

}
