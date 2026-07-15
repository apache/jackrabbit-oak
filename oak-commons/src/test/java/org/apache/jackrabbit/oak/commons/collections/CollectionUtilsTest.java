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
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class CollectionUtilsTest {

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

    @Test
    public void ensureCapacityWithNegativeValue() {
        Assert.assertThrows(IllegalArgumentException.class, () -> CollectionUtils.ensureCapacity(-8));
    }

    @Test
    public void testToCollectionWithCollection() {
        // Create a Collection
        Collection<String> original = Arrays.asList("a", "b", "c");

        // Convert to Collection
        Collection<String> result = CollectionUtils.toCollection(original);

        // Verify it's the same instance
        Assert.assertSame(original, result);
        Assert.assertEquals(Arrays.asList("a", "b", "c"), result);
    }

    @Test
    public void testToCollectionWithNonCollection() {
        // Create a non-Collection Iterable using custom implementation
        Iterable<Integer> iterable = () -> Arrays.asList(1, 2, 3).iterator();

        // Convert to Collection
        Collection<Integer> result = CollectionUtils.toCollection(iterable);

        // Verify it created a new List with the correct elements
        Assert.assertTrue(result instanceof List);
        Assert.assertEquals(Arrays.asList(1, 2, 3), result);
    }

    @Test
    public void testToCollectionWithNull() {
        Assert.assertThrows(NullPointerException.class, () -> CollectionUtils.toCollection(null));
    }

    @Test
    public void testToCollectionWithEmptyIterable() {
        Iterable<String> empty = Collections.emptyList();
        Collection<String> result = CollectionUtils.toCollection(empty);

        Assert.assertTrue(result.isEmpty());
        Assert.assertEquals(0, result.size());
    }

    @Test
    public void testToCollectionWithCustomIterable() {
        // Create a custom Iterable
        Iterable<Character> chars = () -> Arrays.asList('a', 'b', 'c').iterator();

        Collection<Character> result = CollectionUtils.toCollection(chars);

        Assert.assertEquals(3, result.size());
        Assert.assertTrue(result.contains('a'));
        Assert.assertTrue(result.contains('b'));
        Assert.assertTrue(result.contains('c'));
    }

    @Test
    public void testToCollectionMutability() {
        // Create a non-Collection Iterable
        Iterable<String> iterable = () -> Arrays.asList("a", "b").iterator();

        // Convert to Collection
        Collection<String> result = CollectionUtils.toCollection(iterable);

        // Verify the resulting collection is mutable
        result.add("c");
        Assert.assertEquals(Arrays.asList("a", "b", "c"), result);
    }
}