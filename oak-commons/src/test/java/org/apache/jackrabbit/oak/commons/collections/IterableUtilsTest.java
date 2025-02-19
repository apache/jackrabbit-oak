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

import org.apache.commons.collections4.Predicate;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Unit tests for the {@link IterableUtils} class.
 * <p>
 * This class contains test cases to verify the functionality of the methods
 * in the {@link IterableUtils} class.
 */
public class IterableUtilsTest {


    @Test
    public void testTwoChainedIterable() {
        List<Integer> list1 = Arrays.asList(1, 2, 3);
        List<Integer> list2 = Arrays.asList(4, 5);

        Iterable<Integer> chained = IterableUtils.chainedIterable(list1, list2);

        Iterator<Integer> iterator = chained.iterator();
        List<Integer> expected = Arrays.asList(1, 2, 3, 4, 5);
        for (Integer value : expected) {
            Assert.assertTrue(iterator.hasNext());
            Assert.assertEquals(value, iterator.next());
        }
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testThreeChainedIterable() {
        List<Integer> list1 = Arrays.asList(1, 2, 3);
        List<Integer> list2 = Arrays.asList(4, 5);
        List<Integer> list3 = Arrays.asList(6, 7, 8, 9);

        Iterable<Integer> chained = IterableUtils.chainedIterable(list1, list2, list3);

        Iterator<Integer> iterator = chained.iterator();
        List<Integer> expected = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        for (Integer value : expected) {
            Assert.assertTrue(iterator.hasNext());
            Assert.assertEquals(value, iterator.next());
        }
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testChainedIterable() {
        List<Integer> list1 = Arrays.asList(1, 2, 3);
        List<Integer> list2 = Arrays.asList(4, 5);
        List<Integer> list3 = Arrays.asList(6, 7, 8, 9);

        Iterable<Iterable<Integer>> iterables = Arrays.asList(list1, list2, list3);
        Iterable<Integer> chained = IterableUtils.chainedIterable(iterables);

        Iterator<Integer> iterator = chained.iterator();
        List<Integer> expected = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        for (Integer value : expected) {
            Assert.assertTrue(iterator.hasNext());
            Assert.assertEquals(value, iterator.next());
        }
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testFourChainedIterable() {
        List<Integer> list1 = Arrays.asList(1, 2, 3);
        List<Integer> list2 = Arrays.asList(4, 5);
        List<Integer> list3 = Arrays.asList(6, 7, 8, 9);
        List<Integer> list4 = Arrays.asList(10, 11, 12, 13);

        Iterable<Integer> chained = IterableUtils.chainedIterable(list1, list2, list3, list4);

        Iterator<Integer> iterator = chained.iterator();
        List<Integer> expected = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13);
        for (Integer value : expected) {
            Assert.assertTrue(iterator.hasNext());
            Assert.assertEquals(value, iterator.next());
        }
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testChainedIterableVaragrs() {
        List<Integer> list1 = Arrays.asList(1, 2, 3);
        List<Integer> list2 = Arrays.asList(4, 5);
        List<Integer> list3 = Arrays.asList(6, 7, 8, 9);
        List<Integer> list4 = Arrays.asList(10, 11, 12, 13);
        List<Integer> list5 = Arrays.asList(14, 15, 16);

        Iterable<Integer> chained = IterableUtils.chainedIterable(list1, list2, list3, list4, list5);

        Iterator<Integer> iterator = chained.iterator();
        List<Integer> expected = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16);
        for (Integer value : expected) {
            Assert.assertTrue(iterator.hasNext());
            Assert.assertEquals(value, iterator.next());
        }
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testChainedIterableEmpty() {
        Iterable<Iterable<Integer>> iterables = Collections.emptyList();
        Iterable<Integer> chained = IterableUtils.chainedIterable(iterables);

        Iterator<Integer> iterator = chained.iterator();
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testChainedIterableSingle() {
        List<Integer> list = Arrays.asList(1, 2, 3);
        Iterable<Iterable<Integer>> iterables = Collections.singletonList(list);
        Iterable<Integer> chained = IterableUtils.chainedIterable(iterables);

        Iterator<Integer> iterator = chained.iterator();
        List<Integer> expected = Arrays.asList(1, 2, 3);
        for (Integer value : expected) {
            Assert.assertTrue(iterator.hasNext());
            Assert.assertEquals(value, iterator.next());
        }
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testChainedIterableNullElement() {
        List<Integer> list1 = Arrays.asList(1, 2, 3);
        List<Integer> list2 = null;
        List<Integer> list3 = Arrays.asList(6, 7, 8, 9);

        Iterable<Iterable<Integer>> iterables = Arrays.asList(list1, list2, list3);
        Iterable<Integer> chained = IterableUtils.chainedIterable(iterables);

        Iterator<Integer> iterator = chained.iterator();
        List<Integer> expected = Arrays.asList(1, 2, 3);
        for (Integer value : expected) {
            Assert.assertTrue(iterator.hasNext());
            Assert.assertEquals(value, iterator.next());
        }

        // now next iterator should be null
        Assert.assertThrows(NullPointerException.class, iterator::hasNext);
    }

    @Test
    public void testContainsWithNonNullElement() {
        Iterable<String> iterable = List.of("a", "b", "c");
        Assert.assertTrue(IterableUtils.contains(iterable, "b"));
    }

    @Test
    public void testContainsWithNullElement() {
        Iterable<String> iterable = Arrays.asList("a", "b", "c", null);
        Assert.assertTrue(IterableUtils.contains(iterable, null));
    }

    @Test
    public void testContainsWithEmptyIterable() {
        Iterable<String> iterable = List.of();
        Assert.assertFalse(IterableUtils.contains(iterable, "a"));
    }

    @Test
    public void testContainsWithElementNotPresent() {
        Iterable<String> iterable = List.of("a", "b", "c");
        Assert.assertFalse(IterableUtils.contains(iterable, "d"));
    }

    @Test
    public void testContainsWithNullIterable() {
        Assert.assertFalse(IterableUtils.contains(null, "a"));
    }

    @Test
    public void testSizeWithNonEmptyIterable() {
        Iterable<String> iterable = Arrays.asList("a", "b", "c");
        Assert.assertEquals(3, IterableUtils.size(iterable));
    }

    @Test
    public void testSizeWithEmptyIterable() {
        Iterable<String> iterable = Collections.emptyList();
        Assert.assertEquals(0, IterableUtils.size(iterable));
    }

    @Test
    public void testSizeWithSingleElement() {
        Iterable<String> iterable = Collections.singletonList("a");
        Assert.assertEquals(1, IterableUtils.size(iterable));
    }

    @Test
    public void testSizeWithNullIterable() {
        Assert.assertEquals(0, IterableUtils.size(null));
    }

    @Test
    public void testMatchesAllWithAllMatchingElements() {
        Iterable<Integer> iterable = Arrays.asList(2, 4, 6);
        Predicate<Integer> isEven = x -> x % 2 == 0;
        Assert.assertTrue(IterableUtils.matchesAll(iterable, isEven));
    }

    @Test
    public void testMatchesAllWithSomeNonMatchingElements() {
        Iterable<Integer> iterable = Arrays.asList(2, 3, 6);
        Predicate<Integer> isEven = x -> x % 2 == 0;
        Assert.assertFalse(IterableUtils.matchesAll(iterable, isEven));
    }

    @Test
    public void testMatchesAllWithEmptyIterable() {
        Iterable<Integer> iterable = Collections.emptyList();
        Predicate<Integer> isEven = x -> x % 2 == 0;
        Assert.assertTrue(IterableUtils.matchesAll(iterable, isEven));
    }

    @Test
    public void testMatchesAllWithNullIterable() {
        Predicate<Integer> isEven = x -> x % 2 == 0;
        Assert.assertTrue(IterableUtils.matchesAll(null, isEven));
    }

    @Test
    public void testMatchesAllWithNullPredicate() {
        Iterable<Integer> iterable = Arrays.asList(2, 4, 6);
        Assert.assertThrows(NullPointerException.class, () -> {
            IterableUtils.matchesAll(iterable, null);
        });
    }
}
