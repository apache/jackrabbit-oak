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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.junit.Assert.fail;

/**
 * Unit tests for the {@link SetUtils} class.
 * <p>
 * This class contains test cases to verify the functionality of the methods
 * in the {@link SetUtils} class.
 * </p>
 */
public class SetUtilsTest {

    final List<String> data = Arrays.asList("one", "two", "three", null);

    @Test
    public void iterableToSet() {
        // create an iterable
        final Set<String> s = new HashSet<>(data);
        final Iterable<String> iterable = new SimpleIterable<>(s);

        Assert.assertEquals(s, SetUtils.toSet(iterable));
    }

    @Test
    public void iteratorToSet() {
        // create an iterable
        final Set<String> s = new HashSet<>(data);
        final Iterable<String> iterable = new SimpleIterable<>(s);

        Assert.assertEquals(s, SetUtils.toSet(iterable.iterator()));
    }

    @Test(expected = NullPointerException.class)
    public void nullArrayToSet() {
        SetUtils.toSet((String[])null);
    }

    @Test
    public void testToSetWithElements() {
        Set<String> result = SetUtils.toSet("one", "two", "three");
        Assert.assertEquals(3, result.size());
        Assert.assertTrue(result.contains("one"));
        Assert.assertTrue(result.contains("two"));
        Assert.assertTrue(result.contains("three"));
    }

    @Test
    public void testToSetWithDuplicateElements() {
        Set<String> result = SetUtils.toSet("one", "two", "two", "three");
        Assert.assertEquals(3, result.size());
        Assert.assertTrue(result.contains("one"));
        Assert.assertTrue(result.contains("two"));
        Assert.assertTrue(result.contains("three"));
    }

    @Test
    public void testToSetWithNoElements() {
        Set<String> result = SetUtils.toSet();
        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void testToSetWithNullElement() {
        Set<String> result = SetUtils.toSet((String) null);
        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.contains(null));
    }

    @Test
    public void testToSetWithMultipleNullElements() {
        Set<String> result = SetUtils.toSet(null, null, null);
        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.contains(null));
    }

    @Test
    public void testToSetWithMixedElements() {
        Set<String> result = SetUtils.toSet("one", null, "two", null);
        Assert.assertEquals(3, result.size());
        Assert.assertTrue(result.contains("one"));
        Assert.assertTrue(result.contains("two"));
        Assert.assertTrue(result.contains(null));
    }

    @Test
    public void arrayToSet() {
        final Set<String> s = SetUtils.toSet(data);
        Assert.assertEquals(s, SetUtils.toSet(data.toArray()));
    }

    @Test
    public void arrayContainingNullToSet() {
        final Set<String> expected = Collections.singleton(null);
        final Set<String> result = SetUtils.toSet((String)null);
        Assert.assertEquals(expected, result);
    }

    @Test
    public void iterableToLinkedSet() {
        // create an iterable
        final Set<String> s = new LinkedHashSet<>(data);
        final Iterable<String> iterable = new SimpleIterable<>(s);

        Assert.assertEquals(s, SetUtils.toLinkedSet(iterable));
    }

    @Test
    public void arrayToLinkedSet() {
        final Set<String> s = SetUtils.toLinkedSet(data);
        Assert.assertEquals(s, SetUtils.toLinkedSet(data.toArray()));
    }

    @Test
    public void arrayContainingNullToLinkedSet() {
        final Set<String> expected = Collections.singleton(null);
        final Set<String> result = SetUtils.toLinkedSet((String)null);
        Assert.assertEquals(expected, result);
    }

    @Test(expected = NullPointerException.class)
    public void nullArrayToLinkedSet() {
        SetUtils.toLinkedSet((String[])null);
    }

    @Test
    public void iterableToTreeSet() {
        // create an iterable, after removing nulls
        final Set<String> s = data.stream().filter(Objects::nonNull).collect(Collectors.toCollection(TreeSet::new));
        final Iterable<String> iterable = new SimpleIterable<>(s);

        Assert.assertEquals(s, SetUtils.toTreeSet(iterable));
    }

    @Test
    public void concurrentHashSet() {
        // create a set of non-null values
        final Set<String> s = data.stream().filter(Objects::nonNull).collect(Collectors.toSet());

        Set<String> concurrentHashSet = SetUtils.newConcurrentHashSet();
        concurrentHashSet.addAll(s);

        Assert.assertEquals(s, concurrentHashSet);
    }

    @Test
    public void concurrentHashSetWithIterable() {
        // create a set of non-null values
        final Iterable<String> elements = data.stream().filter(Objects::nonNull).collect(Collectors.toSet());

        Set<String> concurrentHashSet = SetUtils.newConcurrentHashSet(elements);

        Assert.assertEquals(elements, concurrentHashSet);
    }

    @Test(expected = NullPointerException.class)
    public void concurrentHashSetWithIterableWithNulls() {
        // create a set of null values
        final Iterable<String> elements = new HashSet<>(data);

        SetUtils.newConcurrentHashSet(elements);

        fail("Should throw NullPointerException");
    }

    @Test
    public void testNewHashSetWithPositiveCapacity() {
        int capacity = 10;
        Set<String> result = SetUtils.newHashSet(capacity);
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof HashSet);
        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void testNewHashSetWithZeroCapacity() {
        int capacity = 0;
        Set<String> result = SetUtils.newHashSet(capacity);
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof HashSet);
        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void testNewHashSetWithNegativeCapacity() {
        int capacity = -1;
        Assert.assertThrows(IllegalArgumentException.class, () -> SetUtils.newHashSet(capacity));
    }

    @Test
    public void testUnionWithNonEmptySets() {
        final Set<String> set1 = Set.of("a", "b", "c");
        final Set<String> set2 = Set.of("d", "e", "f");

        final Set<String> expected = Set.of("a", "b", "c", "d", "e", "f");
        Assert.assertEquals(expected, SetUtils.union(set1, set2));
    }

    @Test
    public void testUnionWithEmptySet() {
        final Set<String> set1 = Set.of("a", "b", "c");
        final Set<String> set2 = new HashSet<>();

        final Set<String> expected = Set.of("a", "b", "c");
        Assert.assertEquals(expected, SetUtils.union(set1, set2));
    }

    @Test
    public void testUnionWithBothEmptySets() {
        final Set<String> set1 = new HashSet<>();
        final Set<String> set2 = new HashSet<>();

        Assert.assertEquals(new HashSet<>(), SetUtils.union(set1, set2));
    }

    @Test(expected = NullPointerException.class)
    public void testUnionWithNullFirstSet() {
        Set<String> set1 = null;
        Set<String> set2 = Set.of("a", "b", "c");

        SetUtils.union(set1, set2);
    }

    @Test(expected = NullPointerException.class)
    public void testUnionWithNullSecondSet() {
        Set<String> set1 = Set.of("a", "b", "c");
        Set<String> set2 = null;

        SetUtils.union(set1, set2);
    }

    @Test
    public void testUnionWithOverlappingSets() {
        final Set<String> set1 = Set.of("a", "b", "c");
        final Set<String> set2 = Set.of("b", "c", "d");

        final Set<String> expected = Set.of("a", "b", "c", "d");
        Assert.assertEquals(expected, SetUtils.union(set1, set2));
    }

    @Test
    public void testIntersectionWithNonEmptySets() {
        final Set<String> set1 = Set.of("a", "b", "c");
        final Set<String> set2 = Set.of("b", "c", "d");

        final Set<String> result = SetUtils.intersection(set1, set2);

        final Set<String> expected = Set.of("b", "c");
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testIntersectionWithEmptySet() {
        final Set<String> set1 = Set.of("a", "b", "c");
        final Set<String> set2 = Set.of();

        Assert.assertEquals(Collections.EMPTY_SET, SetUtils.intersection(set1, set2));
    }

    @Test
    public void testIntersectionWithBothEmptySets() {
        final Set<String> set1 = new HashSet<>();
        final Set<String> set2 = new HashSet<>();

        Assert.assertEquals(Collections.EMPTY_SET, SetUtils.intersection(set1, set2));
    }

    @Test(expected = NullPointerException.class)
    public void testIntersectionWithNullFirstSet() {
        final Set<String> set1 = null;
        final Set<String> set2 = Set.of("a", "b", "c");

        SetUtils.intersection(set1, set2);
    }

    @Test(expected = NullPointerException.class)
    public void testIntersectionWithNullSecondSet() {
        final Set<String> set1 = Set.of("a", "b", "c");
        final Set<String> set2 = null;

        SetUtils.intersection(set1, set2);
    }

    @Test
    public void testIntersectionWithNoCommonElements() {
        final Set<String> set1 = Set.of("a", "b", "c");
        final Set<String> set2 = Set.of("d", "e", "f");

        Assert.assertEquals(Collections.EMPTY_SET, SetUtils.intersection(set1, set2));
    }

    @Test
    public void testSymmetricDifferenceWithNonEmptySets() {
        final Set<String> set1 = Set.of("a", "b", "c");
        final Set<String> set2 = Set.of("b", "c", "d");

        final Set<String> result = SetUtils.symmetricDifference(set1, set2);

        final Set<String> expected = Set.of("a", "d");
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testSymmetricDifferenceWithEmptySet() {
        final Set<String> set1 = Set.of("a", "b", "c");
        final Set<String> set2 = Set.of();

        final Set<String> result = SetUtils.symmetricDifference(set1, set2);

        final Set<String> expected = Set.of("a", "b", "c");
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testSymmetricDifferenceWithBothEmptySets() {
        final Set<String> set1 = Set.of();
        final Set<String> set2 = Set.of();

        final Set<String> result = SetUtils.symmetricDifference(set1, set2);

        final Set<String> expected = Set.of();
        Assert.assertEquals(expected, result);
    }

    @Test(expected = NullPointerException.class)
    public void testSymmetricDifferenceWithNullFirstSet() {
        final Set<String> set1 = null;
        final Set<String> set2 = Set.of("a", "b", "c");

        SetUtils.symmetricDifference(set1, set2);
        fail("Shouldn't reach here");
    }

    @Test(expected = NullPointerException.class)
    public void testSymmetricDifferenceWithNullSecondSet() {
        Set<String> set1 = Set.of("a", "b", "c");
        Set<String> set2 = null;

        SetUtils.symmetricDifference(set1, set2);
        fail("Shouldn't reach here");
    }

    @Test
    public void testSymmetricDifferenceWithNoCommonElements() {
        final Set<String> set1 = Set.of("a", "b", "c");
        final Set<String> set2 = Set.of("d", "e", "f");

        final Set<String> result = SetUtils.symmetricDifference(set1, set2);

        final Set<String> expected = Set.of("a", "b", "c", "d", "e", "f");
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testDifferenceWithNonEmptySets() {
        final Set<String> set1 = Set.of("a", "b", "c");
        final Set<String> set2 = Set.of("b", "c", "d");

        final Set<String> result = SetUtils.difference(set1, set2);

        final Set<String> expected = Set.of("a");
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testDifferenceWithEmptySet() {
        final Set<String> set1 = Set.of("a", "b", "c");
        final Set<String> set2 = Set.of();

        final Set<String> result = SetUtils.difference(set1, set2);

        final Set<String> expected = Set.of("a", "b", "c");
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testDifferenceWithBothEmptySets() {
        final Set<String> set1 = Set.of();
        final Set<String> set2 = Set.of();

        final Set<String> result = SetUtils.difference(set1, set2);

        final Set<String> expected = Set.of();
        Assert.assertEquals(expected, result);
    }

    @Test(expected = NullPointerException.class)
    public void testDifferenceWithNullFirstSet() {
        final Set<String> set1 = null;
        final Set<String> set2 = Set.of("a", "b", "c");

        SetUtils.difference(set1, set2);
        fail("Shouldn't reach here");
    }

    @Test(expected = NullPointerException.class)
    public void testDifferenceWithNullSecondSet() {
        final Set<String> set1 = Set.of("a", "b", "c");
        final Set<String> set2 = null;

        SetUtils.difference(set1, set2);
        fail("Shouldn't reach here");
    }

    @Test
    public void testDifferenceWithNoCommonElements() {
        final Set<String> set1 = Set.of("a", "b", "c");
        final Set<String> set2 = Set.of("d", "e", "f");

        final Set<String> result = SetUtils.difference(set1, set2);

        final Set<String> expected = Set.of("a", "b", "c");
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testDifferenceWithAllCommonElements() {
        final Set<String> set1 = Set.of("a", "b", "c");
        final Set<String> set2 = Set.of("a", "b", "c");

        final Set<String> result = SetUtils.difference(set1, set2);

        final Set<String> expected = Set.of();
        Assert.assertEquals(expected, result);
    }
}