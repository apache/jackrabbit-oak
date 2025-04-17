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
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

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

    @Test
    public void testSizeWithMultipleElements() {
        List<String> list = Arrays.asList("one", "two", "three", "four", "five");
        Iterator<String> iterator = list.iterator();
        Assert.assertEquals(5, IteratorUtils.size(iterator));
        Assert.assertFalse("Iterator should be consumed after size operation", iterator.hasNext());
    }

    @Test
    public void testSizeWithEmptyIterator() {
        Assert.assertEquals(0, IteratorUtils.size(Collections.emptyIterator()));
    }

    @Test
    public void testSizeWithNullIterator() {
        Assert.assertEquals(0,IteratorUtils.size(null));
    }

    @Test
    public void testSizeConsumesIterator() {
        List<String> list = Arrays.asList("one", "two", "three");
        Iterator<String> iterator = list.iterator();

        Assert.assertEquals(3, IteratorUtils.size(iterator));
        Assert.assertFalse("Iterator should be consumed after size operation", iterator.hasNext());
    }

    @Test
    public void testSizeWithSingleElement() {
        List<String> singletonList = Collections.singletonList("single");
        Assert.assertEquals(1, IteratorUtils.size(singletonList.iterator()));
    }

    @Test
    public void testSizeWithCustomIterator() {
        Iterator<Integer> customIterator = new Iterator<>() {
            private int count = 0;

            @Override
            public boolean hasNext() {
                return count < 10;
            }

            @Override
            public Integer next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return count++;
            }
        };
        Assert.assertEquals(10, IteratorUtils.size(customIterator));
        Assert.assertFalse("Iterator should be consumed after size operation", customIterator.hasNext());
    }

    @Test
    public void testGetFirstElement() {
        Iterator<String> iterator = Arrays.asList("a", "b", "c").iterator();
        Assert.assertEquals("a", IteratorUtils.get(iterator, 0));
        // iterator should be at position 1
        Assert.assertEquals("b", iterator.next());

    }

    @Test
    public void testGetMiddleElement() {
        Iterator<Integer> iterator = Arrays.asList(1, 2, 3).iterator();
        Assert.assertEquals(Integer.valueOf(2), IteratorUtils.get(iterator, 1));
        // iterator should be at position 2
        Assert.assertEquals(Integer.valueOf(3), iterator.next());
    }

    @Test
    public void testGetLastElement() {
        Iterator<String> iterator = Arrays.asList("a", "b", "c").iterator();
        Assert.assertEquals("c", IteratorUtils.get(iterator, 2));
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testGetWithNullIterator() {
        Assert.assertThrows(NullPointerException.class, () -> {
            IteratorUtils.get(null, 0);
        });
    }

    @Test
    public void testGetWithNegativeIndex() {
        List<String> data = Arrays.asList("a", "b", "c");
        Assert.assertThrows(IndexOutOfBoundsException.class, () -> {
            IteratorUtils.get(data.iterator(), -1);
        });
    }

    @Test
    public void testWithIndexGreaterThanSizeOfIterator() {
        List<String> data = Arrays.asList("a", "b", "c");
        Assert.assertThrows(IndexOutOfBoundsException.class, () -> {
            IteratorUtils.get(data.iterator(), 3);
        });
    }

    @Test
    public void testGetWithEmptyIterator() {
        Assert.assertThrows(IndexOutOfBoundsException.class, () -> {
            IteratorUtils.get(Collections.emptyIterator(), 0);
        });
    }

    @Test
    public void testGetWithCustomObject() {
        class TestObject {
            private final String value;

            TestObject(String value) {
                this.value = value;
            }

            @Override
            public String toString() {
                return value;
            }
        }

        List<TestObject> data = Arrays.asList(
                new TestObject("obj1"),
                new TestObject("obj2"),
                new TestObject("obj3")
        );

        TestObject result = IteratorUtils.get(data.iterator(), 1);
        Assert.assertEquals("obj2", result.toString());
    }

    @Test
    public void testGetLastWithMultipleElements() {
        Iterator<String> iterator = Arrays.asList("one", "two", "three").iterator();
        Assert.assertEquals("three", IteratorUtils.getLast(iterator));
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testGetLastWithSingleElement() {
        Iterator<Integer> intIterator = List.of(1).iterator();
        Assert.assertEquals(Integer.valueOf(1), IteratorUtils.getLast(intIterator));
        Assert.assertFalse(intIterator.hasNext());
    }

    @Test(expected = NullPointerException.class)
    public void testGetLastWithNullIterator() {
        IteratorUtils.getLast(null);
    }

    @Test(expected = NoSuchElementException.class)
    public void testGetLastWithEmptyIterator() {
        IteratorUtils.getLast(Collections.emptyIterator());
    }

    @Test
    public void testGetLastWithDifferentTypes() {
        class TestObject {
            private final String value;

            TestObject(String value) {
                this.value = value;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                TestObject that = (TestObject) o;
                return value.equals(that.value);
            }
        }

        TestObject obj1 = new TestObject("first");
        TestObject obj2 = new TestObject("last");
        Iterator<TestObject> objectIterator = Arrays.asList(obj1, obj2).iterator();
        Assert.assertEquals(obj2, IteratorUtils.getLast(objectIterator));
        Assert.assertFalse(objectIterator.hasNext());
    }

    @Test
    public void testContainsWithElementPresent() {
        Iterator<String> iterator = Arrays.asList("a", "b", "c").iterator();
        Assert.assertTrue(IteratorUtils.contains(iterator, "b"));
        // Iterator shouldn't be consumed
        Assert.assertTrue(iterator.hasNext());
    }

    @Test
    public void testContainsWithElementNotPresent() {
        Iterator<String> iterator = Arrays.asList("a", "b", "c").iterator();
        Assert.assertFalse(IteratorUtils.contains(iterator, "z"));
        // Iterator should be consumed
        Assert.assertFalse(iterator.hasNext());
    }

    @Test(expected = NullPointerException.class)
    public void testContainsWithNullIterator() {
        IteratorUtils.contains(null, "test");
    }

    @Test
    public void testContainsWithEmptyIterator() {
        Iterator<String> emptyIterator = Collections.emptyIterator();
        Assert.assertFalse(IteratorUtils.contains(emptyIterator, "anything"));
    }

    @Test
    public void testContainsWithNullElement() {
        Iterator<String> iterator = Arrays.asList("a", null, "c").iterator();
        Assert.assertTrue(IteratorUtils.contains(iterator, null));
        // Iterator should be stopped after finding null
        Assert.assertTrue(iterator.hasNext());
    }

    @Test
    public void testContainsWithMultipleOccurrences() {
        Iterator<String> iterator = Arrays.asList("a", "b", "b", "c").iterator();
        Assert.assertTrue(IteratorUtils.contains(iterator, "b"));
        // Iterator should stop at first occurrence
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("b", iterator.next());
    }

    @Test
    public void testToArrayWithMultipleElements() {
        Iterator<String> iterator = Arrays.asList("one", "two", "three").iterator();
        String[] array = IteratorUtils.toArray(iterator, String.class);
        Assert.assertArrayEquals(new String[]{"one", "two", "three"}, array);
    }

    @Test
    public void testToArrayWithEmptyIterator() {
        Iterator<String> iterator = Collections.emptyIterator();
        String[] array = IteratorUtils.toArray(iterator, String.class);
        Assert.assertArrayEquals(new String[0], array);
    }

    @Test(expected = NullPointerException.class)
    public void testToArrayWithNullIterator() {
        IteratorUtils.toArray(null, String.class);
    }

    @Test(expected = NullPointerException.class)
    public void testToArrayWithNullType() {
        Iterator<String> iterator = Arrays.asList("one").iterator();
        IteratorUtils.toArray(iterator, null);
    }

    @Test
    public void testToArrayWithSingleElement() {
        Iterator<Integer> iterator = Collections.singletonList(10).iterator();
        Integer[] array = IteratorUtils.toArray(iterator, Integer.class);
        Assert.assertArrayEquals(new Integer[]{10}, array);
    }

    @Test
    public void testToArrayWithCustomType() {
        class CustomObject {
            private final String value;

            CustomObject(String value) {
                this.value = value;
            }

            @Override
            public String toString() {
                return value;
            }
        }

        List<CustomObject> list = Arrays.asList(new CustomObject("first"), new CustomObject("second"));
        Iterator<CustomObject> iterator = list.iterator();
        CustomObject[] array = IteratorUtils.toArray(iterator, CustomObject.class);
        Assert.assertEquals("first", array[0].toString());
        Assert.assertEquals("second", array[1].toString());
    }

    @Test
    public void testAsEnumerationWithMultipleElements() {
        Iterator<String> iterator = Arrays.asList("one", "two", "three").iterator();
        Enumeration<String> enumeration = IteratorUtils.asEnumeration(iterator);

        Assert.assertTrue(enumeration.hasMoreElements());
        Assert.assertEquals("one", enumeration.nextElement());
        Assert.assertTrue(enumeration.hasMoreElements());
        Assert.assertEquals("two", enumeration.nextElement());
        Assert.assertTrue(enumeration.hasMoreElements());
        Assert.assertEquals("three", enumeration.nextElement());
        Assert.assertFalse(enumeration.hasMoreElements());
    }

    @Test
    public void testAsEnumerationWithEmptyIterator() {
        Iterator<String> emptyIterator = Collections.emptyIterator();
        Enumeration<String> enumeration = IteratorUtils.asEnumeration(emptyIterator);

        Assert.assertFalse(enumeration.hasMoreElements());
    }

    @Test
    public void testAsEnumerationWithSingleElement() {
        Iterator<Integer> singleElementIterator = Collections.singleton(42).iterator();
        Enumeration<Integer> enumeration = IteratorUtils.asEnumeration(singleElementIterator);

        Assert.assertTrue(enumeration.hasMoreElements());
        Assert.assertEquals(Integer.valueOf(42), enumeration.nextElement());
        Assert.assertFalse(enumeration.hasMoreElements());
    }

    @Test(expected = NoSuchElementException.class)
    public void testAsEnumerationNoMoreElements() {
        Iterator<String> iterator = Collections.singletonList("single").iterator();
        Enumeration<String> enumeration = IteratorUtils.asEnumeration(iterator);

        enumeration.nextElement(); // First element
        enumeration.nextElement(); // Should throw NoSuchElementException
    }

    @Test(expected = NullPointerException.class)
    public void testAsEnumerationWithNullIterator() {
        IteratorUtils.asEnumeration(null);
    }

    @Test
    public void testAsEnumerationWithCustomType() {
        class CustomObject {
            private final String value;

            CustomObject(String value) {
                this.value = value;
            }

            @Override
            public boolean equals(Object obj) {
                if (this == obj) return true;
                if (obj == null || getClass() != obj.getClass()) return false;
                CustomObject that = (CustomObject) obj;
                return Objects.equals(value, that.value);
            }
        }

        List<CustomObject> list = Arrays.asList(new CustomObject("first"), new CustomObject("second"));
        Iterator<CustomObject> iterator = list.iterator();
        Enumeration<CustomObject> enumeration = IteratorUtils.asEnumeration(iterator);

        Assert.assertTrue(enumeration.hasMoreElements());
        Assert.assertEquals(list.get(0), enumeration.nextElement());
        Assert.assertTrue(enumeration.hasMoreElements());
        Assert.assertEquals(list.get(1), enumeration.nextElement());
        Assert.assertFalse(enumeration.hasMoreElements());
    }
}
