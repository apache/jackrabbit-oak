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
package org.apache.jackrabbit.oak.commons.collections;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.fail;

public class CollectionUtilsTest {

    final List<String> data = Arrays.asList("one", "two", "three", null);

    @Test
    public void iterableToList() {
        // create an iterable
        final Iterable<String> iterable = new SimpleIterable<>(data);

        Assert.assertEquals(data, CollectionUtils.toList(iterable));

    }

    @Test
    public void iteratorToList() {
        // create an iterator
        final Iterable<String> iterable = new SimpleIterable<>(data);

        Assert.assertEquals(data, CollectionUtils.toList(iterable.iterator()));
    }

    @Test
    public void iterableToSet() {
        // create an iterable
        final Set<String> s = new HashSet<>(data);
        final Iterable<String> iterable = new SimpleIterable<>(s);

        Assert.assertEquals(s, CollectionUtils.toSet(iterable));
    }

    @Test
    public void iterableToLinkedSet() {
        // create an iterable
        final Set<String> s = new LinkedHashSet<>(data);
        final Iterable<String> iterable = new SimpleIterable<>(s);

        Assert.assertEquals(s, CollectionUtils.toLinkedSet(iterable));
    }

    @Test
    public void iteratorToSet() {
        // create an iterable
        final Set<String> s = new HashSet<>(data);
        final Iterable<String> iterable = new SimpleIterable<>(s);

        Assert.assertEquals(s, CollectionUtils.toSet(iterable.iterator()));
    }

    @Test
    public void arrayToSet() {
        final Set<String> s = CollectionUtils.toSet(data);
        Assert.assertEquals(s, CollectionUtils.toSet(data.toArray()));
    }

    @Test
    public void arrayContainingNullToSet() {
        final Set<String> expected = Collections.singleton(null);
        final Set<String> result = CollectionUtils.toSet((String)null);
        Assert.assertEquals(expected, result);
    }

    @Test(expected = NullPointerException.class)
    public void nullArrayToSet() {
        CollectionUtils.toSet((String[])null);
    }

    @Test
    public void iteratorToIIteratable() {
        Iterator<String> iterator = List.of("a", "b", "c").iterator();
        iterator.next();
        Iterable<String> iterable = CollectionUtils.toIterable(iterator);
        Iterator<String> testit = iterable.iterator();
        Assert.assertEquals("b", testit.next());
        Assert.assertEquals("c", testit.next());
        Assert.assertFalse(testit.hasNext());
        try {
            testit = iterable.iterator();
            fail("should only work once");
        } catch (IllegalStateException expected) {
            // that's what we want
        }
    }

    @Test
    public void iteratorToStream() {
        List<String> input = List.of("a", "b", "c");
        Iterator<String> iterator = input.iterator();
        Stream<String> stream = CollectionUtils.toStream(iterator);
        List<String> result = stream.collect(Collectors.toList());
        Assert.assertEquals(input.toString(), result.toString());
    }

    @Test
    public void ensureCapacity() {
        int capacity = CollectionUtils.ensureCapacity(8);
        Assert.assertEquals(11, capacity);
    }

    @Test
    public void ensureCapacityWithMaxValue() {
        int capacity = CollectionUtils.ensureCapacity(1073741825);
        Assert.assertEquals(1073741824, capacity);
    }

    @Test(expected = IllegalArgumentException.class)
    public void ensureCapacityWithNegativeValue() {
        int capacity = CollectionUtils.ensureCapacity(-8);
        fail("Should throw IllegalArgumentException");
    }
}