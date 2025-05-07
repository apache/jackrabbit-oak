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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

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

    @Test
    public void testIsEmptyWithEmptyIterable() {
        Iterable<String> iterable = Collections.emptyList();
        Assert.assertTrue(IterableUtils.isEmpty(iterable));
    }

    @Test
    public void testIsEmptyWithNonEmptyIterable() {
        Iterable<String> iterable = Arrays.asList("a", "b", "c");
        Assert.assertFalse(IterableUtils.isEmpty(iterable));
    }

    @Test
    public void testIsEmptyWithSingleElement() {
        Iterable<String> iterable = Collections.singletonList("a");
        Assert.assertFalse(IterableUtils.isEmpty(iterable));
    }

    @Test
    public void testIsEmptyWithNullIterable() {
        Assert.assertTrue(IterableUtils.isEmpty(null));
    }

    @Test
    public void testToArrayWithNonEmptyIterable() {
        Iterable<String> itr = Arrays.asList("a", "b", "c");
        String[] array = IterableUtils.toArray(itr, String.class);
        Assert.assertArrayEquals(new String[]{"a", "b", "c"}, array);
    }

    @Test
    public void testToArrayWithEmptyIterable() {
        Iterable<String> itr = Collections.emptyList();
        String[] array = IterableUtils.toArray(itr, String.class);
        Assert.assertArrayEquals(new String[]{}, array);
    }

    @Test
    public void testToArrayWithSingleElement() {
        Iterable<String> itr = Collections.singletonList("a");
        String[] array = IterableUtils.toArray(itr, String.class);
        Assert.assertArrayEquals(new String[]{"a"}, array);
    }

    @Test
    public void testToArrayWithNullIterable() {
        Assert.assertThrows(NullPointerException.class, () -> {
            IterableUtils.toArray(null, String.class);
        });
    }

    @Test
    public void testToArrayWithNullType() {
        Iterable<String> itr = Arrays.asList("a", "b", "c");
        Assert.assertThrows(NullPointerException.class, () -> {
            IterableUtils.toArray(itr, null);
        });
    }

    @Test
    public void testPartitionWithNonEmptyIterable() {
        Iterable<Integer> iterable = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
        Iterator<List<Integer>> partitions = IterableUtils.partition(iterable, 3).iterator();
        Assert.assertTrue(partitions.hasNext());
        Assert.assertEquals(Arrays.asList(1, 2, 3), partitions.next());
        Assert.assertTrue(partitions.hasNext());
        Assert.assertEquals(Arrays.asList(4, 5, 6), partitions.next());
        Assert.assertTrue(partitions.hasNext());
        Assert.assertEquals(Collections.singletonList(7), partitions.next());
        Assert.assertFalse(partitions.hasNext());
    }

    @Test
    public void testPartitionWithEmptyIterable() {
        Iterable<Integer> iterable = Collections.emptyList();
        Iterator<List<Integer>> partitions = IterableUtils.partition(iterable, 3).iterator();
        Assert.assertFalse(partitions.hasNext());
    }

    @Test
    public void testPartitionWithNotSupportedRemoveIterable() {
        Iterable<Integer> iterable = Collections.emptyList();
        Iterator<List<Integer>> partitions = IterableUtils.partition(iterable, 3).iterator();
        Assert.assertThrows(UnsupportedOperationException.class, partitions::remove);
    }

    @Test
    public void testPartitionWithSingleElement() {
        Iterable<Integer> iterable = Collections.singletonList(1);
        Iterator<List<Integer>> partitions = IterableUtils.partition(iterable, 3).iterator();
        Assert.assertTrue(partitions.hasNext());
        Assert.assertEquals(Collections.singletonList(1), partitions.next());
        Assert.assertFalse(partitions.hasNext());
    }

    @Test
    public void testPartitionWithSizeOne() {
        Iterable<Integer> iterable = Arrays.asList(1, 2, 3, 4, 5);
        Iterator<List<Integer>> partitions = IterableUtils.partition(iterable, 1).iterator();
        Assert.assertTrue(partitions.hasNext());
        Assert.assertEquals(Collections.singletonList(1), partitions.next());
        Assert.assertTrue(partitions.hasNext());
        Assert.assertEquals(Collections.singletonList(2), partitions.next());
        Assert.assertTrue(partitions.hasNext());
        Assert.assertEquals(Collections.singletonList(3), partitions.next());
        Assert.assertTrue(partitions.hasNext());
        Assert.assertEquals(Collections.singletonList(4), partitions.next());
        Assert.assertTrue(partitions.hasNext());
        Assert.assertEquals(Collections.singletonList(5), partitions.next());
        Assert.assertFalse(partitions.hasNext());
    }

    @Test
    public void testPartitionWithNullIterable() {
        Assert.assertThrows(NullPointerException.class, () -> {
            IterableUtils.partition(null, 3);
        });
    }

    @Test
    public void testPartitionWithInvalidSize() {
        Iterable<Integer> iterable = Arrays.asList(1, 2, 3);
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            IterableUtils.partition(iterable, 0);
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            IterableUtils.partition(iterable, -1);
        });
    }

    @Test
    public void testPartitionWithEmptyIterableAndSizeOne() {
        Iterable<List<Integer>> partition = IterableUtils.partition(Collections.emptyList(), 1);
        Iterator<List<Integer>> iterator = partition.iterator();
        Assert.assertFalse(iterator.hasNext());
        Assert.assertThrows(NoSuchElementException.class, iterator::next);
    }

    @Test
    public void testPartitionReturnsUnmodifiableLists() {
        List<String> list = new ArrayList<>(Arrays.asList("a", "b", "c"));
        Iterable<List<String>> partitioned = IterableUtils.partition(list, 2);

        List<String> partition = partitioned.iterator().next();
        Assert.assertThrows(UnsupportedOperationException.class, () -> partition.add("d")); // Should throw UnsupportedOperationException
    }

    @Test
    public void testPartitionWithRemovableIterable() {
        List<String> list = new ArrayList<>(Arrays.asList("a", "b", "c", "d"));
        Iterable<List<String>> partitioned = IterableUtils.partition(list, 2);

        // Get first partition
        List<String> partition = partitioned.iterator().next();
        Assert.assertEquals(Arrays.asList("a", "b"), partition);

        // Original iterator shouldn't support removal through partition
        Assert.assertThrows(UnsupportedOperationException.class, partitioned.iterator()::remove);

        // But original list should still have all elements
        Assert.assertEquals(Arrays.asList("a", "b", "c", "d"), list);
    }

    @Test
    public void testFilterWithNonEmptyIterable() {
        Iterable<Integer> iterable = Arrays.asList(1, 2, 3, 4, 5);
        Predicate<Integer> predicate = x -> x % 2 == 0;
        Iterable<Integer> filtered = IterableUtils.filter(iterable, predicate);
        List<Integer> result = ListUtils.toList(filtered.iterator());
        Assert.assertEquals(Arrays.asList(2, 4), result);
    }

    @Test
    public void testFilterWithEmptyIterable() {
        Iterable<Integer> iterable = Collections.emptyList();
        Predicate<Integer> predicate = x -> x % 2 == 0;
        Iterable<Integer> filtered = IterableUtils.filter(iterable, predicate);
        List<Integer> result = ListUtils.toList(filtered.iterator());
        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void testFilterWithAllMatchingElements() {
        Iterable<Integer> iterable = Arrays.asList(2, 4, 6);
        Predicate<Integer> predicate = x -> x % 2 == 0;
        Iterable<Integer> filtered = IterableUtils.filter(iterable, predicate);
        List<Integer> result = ListUtils.toList(filtered.iterator());
        Assert.assertEquals(Arrays.asList(2, 4, 6), result);
    }

    @Test
    public void testFilterWithNoMatchingElements() {
        Iterable<Integer> iterable = Arrays.asList(1, 3, 5);
        Predicate<Integer> predicate = x -> x % 2 == 0;
        Iterable<Integer> filtered = IterableUtils.filter(iterable, predicate);
        List<Integer> result = ListUtils.toList(filtered.iterator());
        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void testFilterWithNullIterable() {
        Predicate<Integer> predicate = x -> x % 2 == 0;
        Assert.assertThrows(NullPointerException.class, () -> {
            IterableUtils.filter(null, predicate);
        });
    }

    @Test
    public void testFilterWithNullPredicate() {
        Iterable<Integer> iterable = Arrays.asList(1, 2, 3);
        Assert.assertThrows(NullPointerException.class, () -> {
            IterableUtils.filter(iterable, (Predicate)null);
        });
    }

    @Test
    public void testFilterByClassTypeWithNonEmptyIterable() {
        Iterable<Object> iterable = Arrays.asList(1, "two", 3, "four", 5.0, 6);
        Iterable<Integer> filtered = IterableUtils.filter(iterable, Integer.class);
        List<Integer> result = ListUtils.toList(filtered.iterator());
        Assert.assertEquals(Arrays.asList(1, 3, 6), result);
    }

    @Test
    public void testFilterByClassTypeWithEmptyIterable() {
        Iterable<Object> iterable = Collections.emptyList();
        Iterable<Integer> filtered = IterableUtils.filter(iterable, Integer.class);
        List<Integer> result = ListUtils.toList(filtered.iterator());
        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void testFilterByClassTypeWithAllMatchingElements() {
        Iterable<Object> iterable = Arrays.asList(1, 2, 3);
        Iterable<Integer> filtered = IterableUtils.filter(iterable, Integer.class);
        List<Integer> result = ListUtils.toList(filtered.iterator());
        Assert.assertEquals(Arrays.asList(1, 2, 3), result);
    }

    @Test
    public void testFilterByClassTypeWithNoMatchingElements() {
        Iterable<Object> iterable = Arrays.asList("one", "two", "three");
        Iterable<Integer> filtered = IterableUtils.filter(iterable, Integer.class);
        List<Integer> result = ListUtils.toList(filtered.iterator());
        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void testFilterByClassTypeWithNullIterable() {
        Assert.assertThrows(NullPointerException.class, () -> {
            IterableUtils.filter(null, Integer.class);
        });
    }

    @Test
    public void testFilterByClassTypeWithNullClassType() {
        Iterable<Object> iterable = Arrays.asList(1, 2, 3);
        Assert.assertThrows(NullPointerException.class, () -> {
            IterableUtils.filter(iterable, (Class)null);
        });
    }

    @Test
    public void testTransformWithNonEmptyIterable() {
        Iterable<Integer> iterable = Arrays.asList(1, 2, 3);
        Function<Integer, String> function = Object::toString;
        Iterable<String> transformed = IterableUtils.transform(iterable, function);
        List<String> result = ListUtils.toList(transformed.iterator());
        Assert.assertEquals(Arrays.asList("1", "2", "3"), result);
    }

    @Test
    public void testTransformWithEmptyIterable() {
        Iterable<Integer> iterable = Collections.emptyList();
        Function<Integer, String> function = Object::toString;
        Iterable<String> transformed = IterableUtils.transform(iterable, function);
        List<String> result = ListUtils.toList(transformed.iterator());
        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void testTransformWithNullIterable() {
        Function<Integer, String> function = Object::toString;
        Assert.assertThrows(NullPointerException.class, () -> {
            IterableUtils.transform(null, function);
        });
    }

    @Test
    public void testTransformWithNullFunction() {
        Iterable<Integer> iterable = Arrays.asList(1, 2, 3);
        Assert.assertThrows(NullPointerException.class, () -> {
            IterableUtils.transform(iterable, null);
        });
    }

    @Test
    public void testTransformWithComplexFunction() {
        Iterable<String> iterable = Arrays.asList("a", "bb", "ccc");
        Function<String, Integer> function = String::length;
        Iterable<Integer> transformed = IterableUtils.transform(iterable, function);
        List<Integer> result = ListUtils.toList(transformed.iterator());
        Assert.assertEquals(Arrays.asList(1, 2, 3), result);
    }

    @Test
    public void testMergeSortedWithNonEmptyIterables() {
        List<Integer> list1 = Arrays.asList(1, 4, 9);
        List<Integer> list2 = Arrays.asList(2, 5, 8);
        List<Integer> list3 = Arrays.asList(3, 6, 7);

        Iterable<Iterable<Integer>> iterables = Arrays.asList(list1, list2, list3);
        Iterable<Integer> merged = IterableUtils.mergeSorted(iterables, Comparator.naturalOrder());

        List<Integer> expected = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        Iterator<Integer> iterator = merged.iterator();
        for (Integer value : expected) {
            Assert.assertTrue(iterator.hasNext());
            Assert.assertEquals(value, iterator.next());
        }
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testMergeSortedWithDuplicateElementsIterables() {
        List<Integer> list1 = Arrays.asList(1, 4, 9);
        List<Integer> list2 = Arrays.asList(2, 5, 8);
        List<Integer> list3 = Arrays.asList(3, 6, 9);

        Iterable<Iterable<Integer>> iterables = Arrays.asList(list1, list2, list3);
        Iterable<Integer> merged = IterableUtils.mergeSorted(iterables, Comparator.naturalOrder());

        List<Integer> expected = Arrays.asList(1, 2, 3, 4, 5, 6, 8, 9, 9);
        Iterator<Integer> iterator = merged.iterator();
        for (Integer value : expected) {
            Assert.assertTrue(iterator.hasNext());
            Assert.assertEquals(value, iterator.next());
        }
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testMergeSortedWithEmptyIterables() {
        List<Integer> list1 = Collections.emptyList();
        List<Integer> list2 = Collections.emptyList();
        List<Integer> list3 = Collections.emptyList();

        Iterable<Iterable<Integer>> iterables = Arrays.asList(list1, list2, list3);
        Iterable<Integer> merged = IterableUtils.mergeSorted(iterables, Comparator.naturalOrder());

        Assert.assertFalse(merged.iterator().hasNext());
    }

    @Test
    public void testMergeSortedWithNullIterables() {
        Assert.assertThrows(NullPointerException.class, () -> {
            IterableUtils.mergeSorted(null, Comparator.naturalOrder());
        });
    }

    @Test
    public void testMergeSortedWithNullComparator() {
        List<Integer> list1 = Arrays.asList(1, 4, 7);
        List<Integer> list2 = Arrays.asList(2, 5, 8);
        List<Integer> list3 = Arrays.asList(3, 6, 9);

        Iterable<Iterable<Integer>> iterables = Arrays.asList(list1, list2, list3);
        Assert.assertThrows(NullPointerException.class, () -> {
            IterableUtils.mergeSorted(iterables, null);
        });
    }

    @Test
    public void testMergeSortedWithSingleIterable() {
        List<Integer> list1 = Arrays.asList(1, 2, 3);

        Iterable<Iterable<Integer>> iterables = Collections.singletonList(list1);
        Iterable<Integer> merged = IterableUtils.mergeSorted(iterables, Comparator.naturalOrder());

        List<Integer> expected = Arrays.asList(1, 2, 3);
        Iterator<Integer> iterator = merged.iterator();
        for (Integer value : expected) {
            Assert.assertTrue(iterator.hasNext());
            Assert.assertEquals(value, iterator.next());
        }
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testMergeSortedWithNoIterables() {
        Iterable<Iterable<Integer>> iterables = Collections.emptyList();
        Iterable<Integer> merged = IterableUtils.mergeSorted(iterables, Comparator.naturalOrder());

        Assert.assertFalse(merged.iterator().hasNext());
        Assert.assertThrows(NoSuchElementException.class, () -> merged.iterator().next());
    }

    @Test
    public void testNullIterables() {
        // Both null
        Assert.assertTrue(IterableUtils.elementsEqual(null, null));

        // One null
        Assert.assertFalse(IterableUtils.elementsEqual(null, Collections.emptyList()));
        Assert.assertFalse(IterableUtils.elementsEqual(Collections.emptyList(), null));
    }

    @Test
    public void testEmptyIterables() {
        Iterable<String> empty1 = Collections.emptyList();
        Iterable<String> empty2 = Collections.emptyList();

        Assert.assertTrue(IterableUtils.elementsEqual(empty1, empty2));
    }

    @Test
    public void testSameIterable() {
        Iterable<String> iterable = Arrays.asList("a", "b", "c");
        Assert.assertTrue(IterableUtils.elementsEqual(iterable, iterable));
    }

    @Test
    public void testEqualIterables() {
        Iterable<String> iterable1 = Arrays.asList("a", "b", "c");
        Iterable<String> iterable2 = Arrays.asList("a", "b", "c");

        Assert.assertTrue(IterableUtils.elementsEqual(iterable1, iterable2));
    }

    @Test
    public void testDifferentElements() {
        Iterable<String> iterable1 = Arrays.asList("a", "b", "c");
        Iterable<String> iterable2 = Arrays.asList("a", "d", "c");

        Assert.assertFalse(IterableUtils.elementsEqual(iterable1, iterable2));
    }

    @Test
    public void testDifferentLengthsFirstLonger() {
        Iterable<String> iterable1 = Arrays.asList("a", "b", "c", "d");
        Iterable<String> iterable2 = Arrays.asList("a", "b", "c");

        Assert.assertFalse(IterableUtils.elementsEqual(iterable1, iterable2));
    }

    @Test
    public void testDifferentLengthsSecondLonger() {
        Iterable<String> iterable1 = Arrays.asList("a", "b");
        Iterable<String> iterable2 = Arrays.asList("a", "b", "c");

        Assert.assertFalse(IterableUtils.elementsEqual(iterable1, iterable2));
    }

    @Test
    public void testWithNullElements() {
        Iterable<String> iterable1 = Arrays.asList("a", null, "c");
        Iterable<String> iterable2 = Arrays.asList("a", null, "c");

        Assert.assertTrue(IterableUtils.elementsEqual(iterable1, iterable2));

        Iterable<String> iterable3 = Arrays.asList("a", null, "c");
        Iterable<String> iterable4 = Arrays.asList("a", "b", "c");

        Assert.assertFalse(IterableUtils.elementsEqual(iterable3, iterable4));
    }

    @Test
    public void testCollectionSizeOptimization() {
        // Different sizes should return false quickly without comparing elements
        List<Integer> list1 = new ArrayList<>();
        List<Integer> list2 = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            list1.add(i);
        }

        for (int i = 0; i < 999; i++) {
            list2.add(i);
        }

        Assert.assertFalse(IterableUtils.elementsEqual(list1, list2));
    }

    @Test
    public void testLargeIterables() {
        // Create two large identical lists
        List<Integer> list1 = new ArrayList<>();
        List<Integer> list2 = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            list1.add(i);
            list2.add(i);
        }

        Assert.assertTrue(IterableUtils.elementsEqual(list1, list2));

        // Modify one value in the second list
        list2.set(9999, -1);
        Assert.assertFalse(IterableUtils.elementsEqual(list1, list2));
    }

    @Test
    public void testCustomIterableImplementations() {
        // Test with a custom iterable implementation
        Iterable<Integer> customIterable1 = () -> Arrays.asList(1, 2, 3).iterator();
        Iterable<Integer> customIterable2 = () -> Arrays.asList(1, 2, 3).iterator();

        Assert.assertTrue(IterableUtils.elementsEqual(customIterable1, customIterable2));

        Iterable<Integer> customIterable3 = () -> Arrays.asList(1, 2, 4).iterator();
        Assert.assertFalse(IterableUtils.elementsEqual(customIterable1, customIterable3));
    }

    @Test
    public void testMixedCollectionTypes() {
        // Test with different collection implementations
        List<String> arrayList = new ArrayList<>(Arrays.asList("a", "b", "c"));
        List<String> linkedList = new LinkedList<>(Arrays.asList("a", "b", "c"));

        Assert.assertTrue(IterableUtils.elementsEqual(arrayList, linkedList));
    }

    @Test
    public void testLimitWithFewerElements() {
        List<Integer> list = List.of(1, 2, 3, 4, 5);
        Iterable<Integer> limited = IterableUtils.limit(list, 3);

        List<Integer> result = new ArrayList<>();
        limited.forEach(result::add);

        Assert.assertEquals(List.of(1, 2, 3), result);
    }

    @Test
    public void testLimitWithMoreElements() {
        List<Integer> list = List.of(1, 2, 3);
        Iterable<Integer> limited = IterableUtils.limit(list, 5);

        List<Integer> result = new ArrayList<>();
        limited.forEach(result::add);

        Assert.assertEquals(List.of(1, 2, 3), result);
    }

    @Test
    public void testLimitWithZero() {
        List<Integer> list = List.of(1, 2, 3, 4, 5);
        Iterable<Integer> limited = IterableUtils.limit(list, 0);

        List<Integer> result = new ArrayList<>();
        limited.forEach(result::add);

        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void testLimitWithEmptyIterable() {
        List<Integer> list = Collections.emptyList();
        Iterable<Integer> limited = IterableUtils.limit(list, 3);

        Assert.assertFalse(limited.iterator().hasNext());
    }

    @Test(expected = NullPointerException.class)
    public void testLimitWithNullIterable() {
        Iterable<Integer> limited = IterableUtils.limit(null, 3);

        Assert.assertFalse(limited.iterator().hasNext());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLimitWithNegativeSize() {
        List<Integer> list = List.of(1, 2, 3);
        IterableUtils.limit(list, -1);
    }

    @Test
    public void testLimitWithRemove() {
        List<Integer> list = new ArrayList<>(Arrays.asList(1, 2, 3, 4, 5));
        Iterable<Integer> limited = IterableUtils.limit(list, 2);

        Iterator<Integer> iterator = limited.iterator();
        iterator.next();  // 1
        iterator.remove();
        iterator.next();  // 2

        Assert.assertEquals(Arrays.asList(2, 3, 4, 5), list);
    }

    @Test
    public void testToStringWithNonEmptyIterable() {
        Iterable<String> iterable = List.of("a", "b", "c");
        String result = IterableUtils.toString(iterable);
        Assert.assertEquals("[a, b, c]", result);
    }

    @Test
    public void testToStringWithEmptyIterable() {
        Iterable<String> iterable = Collections.emptyList();
        String result = IterableUtils.toString(iterable);
        Assert.assertEquals("[]", result);
    }

    @Test
    public void testToStringWithNullIterable() {
        String result = IterableUtils.toString(null);
        Assert.assertEquals("[]", result);
    }

    @Test
    public void testToStringWithNullElements() {
        Iterable<String> iterable = Arrays.asList("a", null, "c");
        String result = IterableUtils.toString(iterable);
        Assert.assertEquals("[a, null, c]", result);
    }

    @Test
    public void testToStringWithMixedTypeElements() {
        Iterable<Object> iterable = Arrays.asList("a", 1, true, 3.14);
        String result = IterableUtils.toString(iterable);
        Assert.assertEquals("[a, 1, true, 3.14]", result);
    }

    @Test
    public void testToStringWithSpecialCharacters() {
        Iterable<String> iterable = Arrays.asList("a,b", "c\"d", "e\nf");
        String result = IterableUtils.toString(iterable);
        Assert.assertEquals("[a,b, c\"d, e\nf]", result);
    }

    @Test
    public void testGetFirstWithNonEmptyIterable() {
        List<String> list = Arrays.asList("a", "b", "c");
        String result = IterableUtils.getFirst(list, "default");
        Assert.assertEquals("a", result);
    }

    @Test
    public void testGetFirstWithEmptyIterable() {
        List<String> list = Collections.emptyList();
        String result = IterableUtils.getFirst(list, "default");
        Assert.assertEquals("default", result);
    }

    @Test
    public void testGetFirstWithNullIterable() {
        Assert.assertThrows(NullPointerException.class, () -> IterableUtils.getFirst(null, "default"));
    }

    @Test
    public void testGetFirstWithSingleElement() {
        List<Integer> list = Collections.singletonList(42);
        Integer result = IterableUtils.getFirst(list, 0);
        Assert.assertEquals(Integer.valueOf(42), result);
    }

    @Test
    public void testGetFirstWithNullFirstElement() {
        List<String> list = Arrays.asList(null, "b", "c");
        String result = IterableUtils.getFirst(list, "default");
        Assert.assertNull(result);
    }

    @Test
    public void testGetFirstWithNullDefaultValue() {
        List<String> list = Collections.emptyList();
        String result = IterableUtils.getFirst(list, null);
        Assert.assertNull(result);
    }

    @Test
    public void testGetFirstWithCustomIterable() {
        Iterable<Integer> customIterable = () -> Arrays.asList(5, 10, 15).iterator();
        Integer result = IterableUtils.getFirst(customIterable, 0);
        Assert.assertEquals(Integer.valueOf(5), result);
    }

    @Test
    public void testGetWithValidPosition() {
        List<String> list = Arrays.asList("a", "b", "c", "d", "e");
        String result = IterableUtils.get(list, 2);
        Assert.assertEquals("c", result);
    }

    @Test
    public void testGetFirstElement() {
        List<String> list = Arrays.asList("a", "b", "c");
        String result = IterableUtils.get(list, 0);
        Assert.assertEquals("a", result);
    }

    @Test
    public void testGetLastElement() {
        List<String> list = Arrays.asList("a", "b", "c");
        String result = IterableUtils.get(list, 2);
        Assert.assertEquals("c", result);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetWithNegativePosition() {
        List<String> list = Arrays.asList("a", "b", "c");
        IterableUtils.get(list, -1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetWithPositionTooLarge() {
        List<String> list = Arrays.asList("a", "b", "c");
        IterableUtils.get(list, 3);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetWithEmptyIterable() {
        List<String> list = Collections.emptyList();
        IterableUtils.get(list, 0);
    }

    @Test(expected = NullPointerException.class)
    public void testGetWithNullIterable() {
        IterableUtils.get(null, 0);
    }

    @Test
    public void testGetWithCustomIterable() {
        // Custom iterable implementation
        Iterable<Integer> customIterable = () -> Arrays.asList(5, 10, 15, 20, 25).iterator();
        Integer result = IterableUtils.get(customIterable, 3);
        Assert.assertEquals(Integer.valueOf(20), result);
    }

    @Test
    public void testGetWithSingleElementIterable() {
        List<String> list = Collections.singletonList("only");
        String result = IterableUtils.get(list, 0);
        Assert.assertEquals("only", result);
    }

    @Test
    public void testFindWithMatchingElement() {
        List<String> list = Arrays.asList("apple", "banana", "cherry");
        String result = IterableUtils.find(list, s -> s.startsWith("b"));
        Assert.assertEquals("banana", result);
    }

    @Test
    public void testFindWithNoMatchingElement() {
        List<String> list = Arrays.asList("apple", "banana", "cherry");
        String result = IterableUtils.find(list, s -> s.startsWith("d"));
        Assert.assertNull(result);
    }

    @Test
    public void testFindWithMultipleMatchingElements() {
        List<String> list = Arrays.asList("apple", "avocado", "banana", "apricot");
        String result = IterableUtils.find(list, s -> s.startsWith("a"));
        // Should return the first matching element
        Assert.assertEquals("apple", result);
    }

    @Test
    public void testFindWithEmptyIterable() {
        List<String> list = Collections.emptyList();
        String result = IterableUtils.find(list, s -> true);
        Assert.assertNull(result);
    }

    @Test(expected = NullPointerException.class)
    public void testFindWithNullIterable() {
        IterableUtils.find(null, s -> true);
    }

    @Test(expected = NullPointerException.class)
    public void testFindWithNullPredicate() {
        List<String> list = Arrays.asList("apple", "banana", "cherry");
        IterableUtils.find(list, null);
    }

    @Test
    public void testFindFirstElement() {
        List<Integer> list = Arrays.asList(10, 20, 30, 40, 50);
        Integer result = IterableUtils.find(list, i -> i > 5);
        Assert.assertEquals(Integer.valueOf(10), result);
    }

    @Test
    public void testFindLastElement() {
        List<Integer> list = Arrays.asList(10, 20, 30, 40, 50);
        Integer result = IterableUtils.find(list, i -> i > 45);
        Assert.assertEquals(Integer.valueOf(50), result);
    }

    @Test
    public void testFindWithCustomIterable() {
        Iterable<Integer> customIterable = () -> Arrays.asList(5, 10, 15, 20, 25).iterator();
        Integer result = IterableUtils.find(customIterable, i -> i % 10 == 0);
        Assert.assertEquals(Integer.valueOf(10), result);
    }

    @Test
    public void testGetLastWithNonEmptyIterable() {
        List<String> list = Arrays.asList("a", "b", "c");
        String result = IterableUtils.getLast(list);
        Assert.assertEquals("c", result);
    }

    @Test
    public void testGetLastWithEmptyIterable() {
        List<String> list = Collections.emptyList();
        String result = IterableUtils.getLast(list);
        Assert.assertNull(result);
    }

    @Test
    public void testGetLastWithNullIterable() {
        Assert.assertThrows(NullPointerException.class, () -> IterableUtils.getLast(null));
    }

    @Test
    public void testGetLastWithSingleElement() {
        List<Integer> list = Collections.singletonList(42);
        Integer result = IterableUtils.getLast(list);
        Assert.assertEquals(Integer.valueOf(42), result);
    }

    @Test
    public void testGetLastWithNullLastElement() {
        List<String> list = Arrays.asList("a", "b", null);
        String result = IterableUtils.getLast(list);
        Assert.assertNull(result);
    }

    @Test
    public void testGetLastWithCustomIterable() {
        // Custom iterable that doesn't implement Collection
        Iterable<Integer> customIterable = () -> Arrays.asList(5, 10, 15).iterator();
        Integer result = IterableUtils.getLast(customIterable);
        Assert.assertEquals(Integer.valueOf(15), result);
    }

    @Test
    public void testGetLastWithLargeIterable() {
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            list.add(i);
        }
        Integer result = IterableUtils.getLast(list);
        Assert.assertEquals(Integer.valueOf(999), result);
    }

    @Test
    public void testGetLastWithListImplementation() {
        // Test to confirm optimization for List works
        List<String> list = Arrays.asList("a", "b", "c", "d");
        String result = IterableUtils.getLast(list);
        Assert.assertEquals("d", result);
    }

    @Test
    public void testGetLastWithNonListCollection() {
        // Test with a Collection that isn't a List
        Set<String> set = new LinkedHashSet<>();
        set.add("a");
        set.add("b");
        set.add("c");
        String result = IterableUtils.getLast(set);
        Assert.assertEquals("c", result);
    }
}
