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
package org.apache.jackrabbit.oak.plugins.index.property;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 * tests the utility class ValuePathTuple
 */
public class ValuePathTupleTest {
    /**
     * testing for asserting the right comparison behaviour of the custom class
     */
    @Test
    public void valuePathTupleComparison() {
        try {
            new ValuePathTuple("value", "path").compareTo(null);
            fail("It should have raised a NPE");
        } catch (NullPointerException e) {
            // so far so good
        }
        assertEquals(0,
            (new ValuePathTuple("value", "path")).compareTo(new ValuePathTuple("value", "path")));
        assertEquals(-1,
            (new ValuePathTuple("value", "path")).compareTo(new ValuePathTuple("value1", "path")));
        assertEquals(-1,
            (new ValuePathTuple("value1", "path")).compareTo(new ValuePathTuple("value1", "path1")));
        assertEquals(1,
            (new ValuePathTuple("value1", "path")).compareTo(new ValuePathTuple("value", "path")));
        assertEquals(1,
            (new ValuePathTuple("value1", "path1")).compareTo(new ValuePathTuple("value1", "path")));

        assertEquals(-1, (new ValuePathTuple("value000", "/test/n1")).compareTo(new ValuePathTuple(
            "value001", "/test/n0")));
        assertEquals(1, (new ValuePathTuple("value001", "/test/n0")).compareTo(new ValuePathTuple(
            "value000", "/test/n1")));
    }

    @Test
    public void greaterThanPredicate() {
        List<ValuePathTuple> data = ImmutableList.of(new ValuePathTuple("a", "foobar"),
            new ValuePathTuple("b", "foobar"), new ValuePathTuple("c", "foobar"),
            new ValuePathTuple("d", "foobar"), new ValuePathTuple("e", "foobar"),
            new ValuePathTuple("f", "foobar"));
        Iterator<ValuePathTuple> filtered = Iterables.filter(data,
            new ValuePathTuple.GreaterThanPredicate("b")).iterator();
        assertTrue(filtered.hasNext());
        assertEquals("c", filtered.next().getValue());
        assertEquals("d", filtered.next().getValue());
        assertEquals("e", filtered.next().getValue());
        assertEquals("f", filtered.next().getValue());
        assertFalse(filtered.hasNext());
    }

    @Test
    public void greaterThanEqualaPredicate() {
        List<ValuePathTuple> data = ImmutableList.of(new ValuePathTuple("a", "foobar"),
            new ValuePathTuple("b", "foobar"), new ValuePathTuple("c", "foobar"),
            new ValuePathTuple("d", "foobar"), new ValuePathTuple("e", "foobar"),
            new ValuePathTuple("f", "foobar"));
        Iterator<ValuePathTuple> filtered = Iterables.filter(data,
            new ValuePathTuple.GreaterThanPredicate("b", true)).iterator();
        assertTrue(filtered.hasNext());
        assertEquals("b", filtered.next().getValue());
        assertEquals("c", filtered.next().getValue());
        assertEquals("d", filtered.next().getValue());
        assertEquals("e", filtered.next().getValue());
        assertEquals("f", filtered.next().getValue());
        assertFalse(filtered.hasNext());
    }

    @Test
    public void lessThanPredicate() {
        List<ValuePathTuple> data = ImmutableList.of(
            new ValuePathTuple("a", "foobar"),
            new ValuePathTuple("b", "foobar"),
            new ValuePathTuple("c", "foobar"),
            new ValuePathTuple("d", "foobar"),
            new ValuePathTuple("e", "foobar"),
            new ValuePathTuple("f", "foobar"));
        Iterator<ValuePathTuple> filtered = Iterables.filter(data,
            new ValuePathTuple.LessThanPredicate("e")).iterator();
        assertTrue(filtered.hasNext());
        assertEquals("a", filtered.next().getValue());
        assertEquals("b", filtered.next().getValue());
        assertEquals("c", filtered.next().getValue());
        assertEquals("d", filtered.next().getValue());
        assertFalse(filtered.hasNext());

        data = ImmutableList.of(
            new ValuePathTuple("f", "foobar"),
            new ValuePathTuple("e", "foobar"),
            new ValuePathTuple("d", "foobar"),
            new ValuePathTuple("c", "foobar"),
            new ValuePathTuple("b", "foobar"),
            new ValuePathTuple("a", "foobar")
            );
        filtered = Iterables.filter(data,
            new ValuePathTuple.LessThanPredicate("e")).iterator();
        assertTrue(filtered.hasNext());
        assertEquals("d", filtered.next().getValue());
        assertEquals("c", filtered.next().getValue());
        assertEquals("b", filtered.next().getValue());
        assertEquals("a", filtered.next().getValue());
        assertFalse(filtered.hasNext());
    }

    @Test
    public void lessThanEqualPredicate() {
        List<ValuePathTuple> data = ImmutableList.of(
            new ValuePathTuple("a", "foobar"),
            new ValuePathTuple("b", "foobar"),
            new ValuePathTuple("c", "foobar"),
            new ValuePathTuple("d", "foobar"),
            new ValuePathTuple("e", "foobar"),
            new ValuePathTuple("f", "foobar"));
        Iterator<ValuePathTuple> filtered = Iterables.filter(data,
                new ValuePathTuple.LessThanPredicate("e", true)).iterator();
        assertTrue(filtered.hasNext());
        assertEquals("a", filtered.next().getValue());
        assertEquals("b", filtered.next().getValue());
        assertEquals("c", filtered.next().getValue());
        assertEquals("d", filtered.next().getValue());
        assertEquals("e", filtered.next().getValue());
        assertFalse(filtered.hasNext());
    }
    
    @Test
    public void betweenNoIncludes() {
        List<ValuePathTuple> data = ImmutableList.of(
            new ValuePathTuple("a", "foobar"),
            new ValuePathTuple("b", "foobar"), 
            new ValuePathTuple("c", "foobar"),
            new ValuePathTuple("d", "foobar"), 
            new ValuePathTuple("e", "foobar"),
            new ValuePathTuple("f", "foobar"));
        Iterator<ValuePathTuple> filtered = Iterables.filter(data,
            new ValuePathTuple.BetweenPredicate("b", "d", false, false)).iterator();
        assertTrue(filtered.hasNext());
        assertEquals("c", filtered.next().getValue());
        assertFalse(filtered.hasNext());
    }

    @Test
    public void betweenIncludeStart() {
        List<ValuePathTuple> data = ImmutableList.of(
            new ValuePathTuple("a", "foobar"),
            new ValuePathTuple("b", "foobar"), 
            new ValuePathTuple("c", "foobar"),
            new ValuePathTuple("d", "foobar"), 
            new ValuePathTuple("e", "foobar"),
            new ValuePathTuple("f", "foobar"));
        Iterator<ValuePathTuple> filtered = Iterables.filter(data,
            new ValuePathTuple.BetweenPredicate("b", "d", true, false)).iterator();
        assertTrue(filtered.hasNext());
        assertEquals("b", filtered.next().getValue());
        assertEquals("c", filtered.next().getValue());
        assertFalse(filtered.hasNext());
    }

    @Test
    public void betweenIncludeEnd() {
        List<ValuePathTuple> data = ImmutableList.of(
            new ValuePathTuple("a", "foobar"),
            new ValuePathTuple("b", "foobar"), 
            new ValuePathTuple("c", "foobar"),
            new ValuePathTuple("d", "foobar"), 
            new ValuePathTuple("e", "foobar"),
            new ValuePathTuple("f", "foobar"));
        Iterator<ValuePathTuple> filtered = Iterables.filter(data,
            new ValuePathTuple.BetweenPredicate("b", "d", false, true)).iterator();
        assertTrue(filtered.hasNext());
        assertEquals("c", filtered.next().getValue());
        assertEquals("d", filtered.next().getValue());
        assertFalse(filtered.hasNext());
    }

    @Test
    public void betweenIncludeBoth() {
        List<ValuePathTuple> data = ImmutableList.of(
            new ValuePathTuple("a", "foobar"),
            new ValuePathTuple("b", "foobar"), 
            new ValuePathTuple("c", "foobar"),
            new ValuePathTuple("d", "foobar"), 
            new ValuePathTuple("e", "foobar"),
            new ValuePathTuple("f", "foobar"));
        Iterator<ValuePathTuple> filtered = Iterables.filter(data,
            new ValuePathTuple.BetweenPredicate("b", "d", true, true)).iterator();
        assertTrue(filtered.hasNext());
        assertEquals("b", filtered.next().getValue());
        assertEquals("c", filtered.next().getValue());
        assertEquals("d", filtered.next().getValue());
        assertFalse(filtered.hasNext());
    }
}
