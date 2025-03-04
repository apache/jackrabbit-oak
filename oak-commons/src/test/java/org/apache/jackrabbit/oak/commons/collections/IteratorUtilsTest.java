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
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.Assert.fail;

public class IteratorUtilsTest {

    @Test
    public void iteratorToIIterable() {
        Iterator<String> iterator = List.of("a", "b", "c").iterator();
        iterator.next();
        Iterable<String> iterable = IteratorUtils.toIterable(iterator);
        Iterator<String> testit = iterable.iterator();
        Assert.assertEquals("b", testit.next());
        Assert.assertEquals("c", testit.next());
        Assert.assertFalse(testit.hasNext());
        try {
            iterable.iterator();
            fail("should only work once");
        } catch (IllegalStateException expected) {
            // that's what we want
        }
    }

    @Test
    public void testMergeSortedWithNonEmptyIterators() {
        List<Integer> list1 = Arrays.asList(1, 4, 7);
        List<Integer> list2 = Arrays.asList(2, 5, 9);
        List<Integer> list3 = Arrays.asList(3, 6, 8);

        Iterable<Iterator<Integer>> iterators = Arrays.asList(list1.iterator(), list2.iterator(), list3.iterator());
        Iterator<Integer> merged = IteratorUtils.mergeSorted(iterators, Comparator.naturalOrder());

        List<Integer> expected = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        for (Integer value : expected) {
            Assert.assertTrue(merged.hasNext());
            Assert.assertEquals(value, merged.next());
        }
        Assert.assertFalse(merged.hasNext());
    }

    @Test
    public void testMergeSortedWithDuplicateElementsIterators() {
        List<Integer> list1 = Arrays.asList(1, 4, 7);
        List<Integer> list2 = Arrays.asList(2, 5, 9);
        List<Integer> list3 = Arrays.asList(3, 6, 9);

        Iterable<Iterator<Integer>> iterators = Arrays.asList(list1.iterator(), list2.iterator(), list3.iterator());
        Iterator<Integer> merged = IteratorUtils.mergeSorted(iterators, Comparator.naturalOrder());

        List<Integer> expected = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 9, 9);
        for (Integer value : expected) {
            Assert.assertTrue(merged.hasNext());
            Assert.assertEquals(value, merged.next());
        }
        Assert.assertFalse(merged.hasNext());
    }

    @Test
    public void testMergeSortedWithEmptyIterators() {
        List<Integer> list1 = List.of();
        List<Integer> list2 = List.of();
        List<Integer> list3 = List.of();

        Iterable<Iterator<Integer>> iterators = Arrays.asList(list1.iterator(), list2.iterator(), list3.iterator());
        Iterator<Integer> merged = IteratorUtils.mergeSorted(iterators, Comparator.naturalOrder());

        Assert.assertFalse(merged.hasNext());
    }

    @Test
    public void testMergeSortedWithNullIterators() {
        Assert.assertThrows(NullPointerException.class, () -> {
            IteratorUtils.mergeSorted(null, Comparator.naturalOrder());
        });
    }

    @Test
    public void testMergeSortedWithNullComparator() {
        List<Integer> list1 = Arrays.asList(1, 4, 7);
        List<Integer> list2 = Arrays.asList(2, 5, 8);
        List<Integer> list3 = Arrays.asList(3, 6, 9);

        Iterable<Iterator<Integer>> iterators = Arrays.asList(list1.iterator(), list2.iterator(), list3.iterator());
        Assert.assertThrows(NullPointerException.class, () -> {
            IteratorUtils.mergeSorted(iterators, null);
        });
    }

    @Test
    public void testMergeSortedWithSingleIterator() {
        List<Integer> list1 = Arrays.asList(1, 2, 3);

        Iterable<Iterator<Integer>> iterators = List.of(list1.iterator());
        Iterator<Integer> merged = IteratorUtils.mergeSorted(iterators, Comparator.naturalOrder());

        List<Integer> expected = Arrays.asList(1, 2, 3);
        for (Integer value : expected) {
            Assert.assertTrue(merged.hasNext());
            Assert.assertEquals(value, merged.next());
        }
        Assert.assertFalse(merged.hasNext());
    }

    @Test
    public void testMergeSortedWithNoIterators() {
        Iterable<Iterator<Integer>> iterators = List.of();
        Iterator<Integer> merged = IteratorUtils.mergeSorted(iterators, Comparator.naturalOrder());

        Assert.assertFalse(merged.hasNext());
    }

    @Test
    public void testMergeSortedWithEmptyAndOneIterator() {
        Iterable<Iterator<Integer>> iterators = List.of();
        Iterator<Integer> merged = IteratorUtils.mergeSorted(iterators, Comparator.naturalOrder());

        Assert.assertThrows(NoSuchElementException.class, merged::next);
    }

    @Test
    public void testNullIterators() {
        // Both null
        Assert.assertTrue(IteratorUtils.elementsEqual(null, null));

        // One null
        Assert.assertFalse(IteratorUtils.elementsEqual(null, Collections.emptyIterator()));
        Assert.assertFalse(IteratorUtils.elementsEqual(Collections.emptyIterator(), null));
    }

    @Test
    public void testEmptyIterators() {
        Iterator<String> empty1 = Collections.emptyIterator();
        Iterator<String> empty2 = Collections.emptyIterator();

        Assert.assertTrue(IteratorUtils.elementsEqual(empty1, empty2));
    }

    @Test
    public void testSameIterator() {
        Iterator<String> iterator = Arrays.asList("a", "b", "c").iterator();
        Assert.assertTrue(IteratorUtils.elementsEqual(iterator, iterator));
    }

    @Test
    public void testEqualIterators() {
        Iterator<String> iterator1 = Arrays.asList("a", "b", "c").iterator();
        Iterator<String> iterator2 = Arrays.asList("a", "b", "c").iterator();

        Assert.assertTrue(IteratorUtils.elementsEqual(iterator1, iterator2));
    }

    @Test
    public void testDifferentElements() {
        Iterator<String> iterator1 = Arrays.asList("a", "b", "c").iterator();
        Iterator<String> iterator2 = Arrays.asList("a", "d", "c").iterator();

        Assert.assertFalse(IteratorUtils.elementsEqual(iterator1, iterator2));
    }

    @Test
    public void testDifferentLengthsFirstLonger() {
        Iterator<String> iterator1 = Arrays.asList("a", "b", "c", "d").iterator();
        Iterator<String> iterator2 = Arrays.asList("a", "b", "c").iterator();

        Assert.assertFalse(IteratorUtils.elementsEqual(iterator1, iterator2));
    }

    @Test
    public void testDifferentLengthsSecondLonger() {
        Iterator<String> iterator1 = Arrays.asList("a", "b").iterator();
        Iterator<String> iterator2 = Arrays.asList("a", "b", "c").iterator();

        Assert.assertFalse(IteratorUtils.elementsEqual(iterator1, iterator2));
    }

    @Test
    public void testWithNullElements() {
        Iterator<String> iterator1 = Arrays.asList("a", null, "c").iterator();
        Iterator<String> iterator2 = Arrays.asList("a", null, "c").iterator();

        Assert.assertTrue(IteratorUtils.elementsEqual(iterator1, iterator2));

        Iterator<String> iterator3 = Arrays.asList("a", null, "c").iterator();
        Iterator<String> iterator4 = Arrays.asList("a", "b", "c").iterator();

        Assert.assertFalse(IteratorUtils.elementsEqual(iterator3, iterator4));
    }

    @Test
    public void testLargeIterators() {
        // Create two large identical lists
        List<Integer> list1 = new ArrayList<>();
        List<Integer> list2 = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            list1.add(i);
            list2.add(i);
        }

        Assert.assertTrue(IteratorUtils.elementsEqual(list1.iterator(), list2.iterator()));

        // Modify one value in the second list
        list2.set(9999, -1);
        Assert.assertFalse(IteratorUtils.elementsEqual(list1.iterator(), list2.iterator()));
    }
}
