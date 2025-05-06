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

    @Test
    public void testChainedIteratorBothNonEmpty() {
        Iterator<String> iterator1 = Arrays.asList("a", "b").iterator();
        Iterator<String> iterator2 = Arrays.asList("c", "d").iterator();
        Iterator<String> chain = IteratorUtils.chainedIterator(iterator1, iterator2);

        // it should iterate the elements in order, first from iterator1 and then from iterator2
        Assert.assertTrue(chain.hasNext());
        Assert.assertEquals("a", chain.next());
        Assert.assertTrue(chain.hasNext());
        Assert.assertEquals("b", chain.next());
        Assert.assertTrue(chain.hasNext());
        Assert.assertEquals("c", chain.next());
        Assert.assertTrue(chain.hasNext());
        Assert.assertEquals("d", chain.next());
        Assert.assertFalse(chain.hasNext());
    }

    @Test
    public void testChainedIteratorFirstEmpty() {
        Iterator<String> empty = Collections.emptyIterator();
        Iterator<String> nonEmpty = Arrays.asList("a", "b").iterator();
        Iterator<String> chain = IteratorUtils.chainedIterator(empty, nonEmpty);

        Assert.assertTrue(chain.hasNext());
        Assert.assertEquals("a", chain.next());
        Assert.assertTrue(chain.hasNext());
        Assert.assertEquals("b", chain.next());
        Assert.assertFalse(chain.hasNext());
    }

    @Test
    public void testChainedIteratorSecondEmpty() {
        Iterator<String> nonEmpty = Arrays.asList("a", "b").iterator();
        Iterator<String> empty = Collections.emptyIterator();
        Iterator<String> chain = IteratorUtils.chainedIterator(nonEmpty, empty);

        Assert.assertTrue(chain.hasNext());
        Assert.assertEquals("a", chain.next());
        Assert.assertTrue(chain.hasNext());
        Assert.assertEquals("b", chain.next());
        Assert.assertFalse(chain.hasNext());
    }

    @Test
    public void testChainedIteratorBothEmpty() {
        Iterator<String> empty1 = Collections.emptyIterator();
        Iterator<String> empty2 = Collections.emptyIterator();
        Iterator<String> chain = IteratorUtils.chainedIterator(empty1, empty2);

        Assert.assertFalse(chain.hasNext());
    }

    @Test
    public void testChainedIteratorNullFirst() {
        Iterator<String> nonEmpty = Arrays.asList("a", "b").iterator();

        Assert.assertThrows(NullPointerException.class, () -> IteratorUtils.chainedIterator(null, nonEmpty));
    }

    @Test
    public void testChainedIteratorNullSecond() {
        Iterator<String> nonEmpty = Arrays.asList("a", "b").iterator();

        Assert.assertThrows(NullPointerException.class, () -> IteratorUtils.chainedIterator(nonEmpty, null));
    }

    @Test
    public void testChainedIteratorBothNull() {
        Assert.assertThrows(NullPointerException.class, () -> IteratorUtils.chainedIterator(null, null));
    }

    @Test
    public void testChainedIteratorRemove() {
        List<String> list1 = new ArrayList<>(Arrays.asList("a", "b"));
        List<String> list2 = new ArrayList<>(Arrays.asList("c", "d"));

        Iterator<String> iterator1 = list1.iterator();
        Iterator<String> iterator2 = list2.iterator();
        Iterator<String> chain = IteratorUtils.chainedIterator(iterator1, iterator2);

        // Remove an element from the first iterator
        chain.next(); // "a"
        chain.remove();
        Assert.assertEquals(List.of("b"), list1);

        // Move to second iterator and remove an element
        chain.next(); // "b"
        chain.next(); // "c"
        chain.remove();
        Assert.assertEquals(List.of("d"), list2);
    }

    @Test
    public void testChainedIteratorRemoveNotSupported() {
        List<String> list1 = List.of("a", "b");
        List<String> list2 = new ArrayList<>(Arrays.asList("c", "d"));

        Iterator<String> iterator1 = list1.iterator();
        Iterator<String> iterator2 = list2.iterator();
        Iterator<String> chain = IteratorUtils.chainedIterator(iterator1, iterator2);

        // Remove an element from the first iterator
        chain.next(); // "a"
        Assert.assertThrows(UnsupportedOperationException.class, chain::remove);
        Assert.assertEquals(List.of("a", "b"), list1);

        // Move to second iterator and remove an element
        chain.next(); // "b"
        chain.next(); // "c"
        chain.remove();
        Assert.assertEquals(List.of("d"), list2);
    }

    @Test
    public void testChainedIteratorWithDifferentTypes() {
        Iterator<Integer> intIterator = Arrays.asList(1, 2).iterator();
        Iterator<Double> doubleIterator = Arrays.asList(3.0, 4.0).iterator();

        // Chain iterators with a common supertype
        Iterator<Number> chain = IteratorUtils.chainedIterator(intIterator, doubleIterator);

        Assert.assertTrue(chain.hasNext());
        Assert.assertEquals(1, chain.next());
        Assert.assertTrue(chain.hasNext());
        Assert.assertEquals(2, chain.next());
        Assert.assertTrue(chain.hasNext());
        Assert.assertEquals(3.0, chain.next());
        Assert.assertTrue(chain.hasNext());
        Assert.assertEquals(4.0, chain.next());
        Assert.assertFalse(chain.hasNext());
    }

    // for chained Iterator with varargs

    @Test
    public void testChainedIteratorArrays() {
        Iterator<String> iterator1 = Arrays.asList("a", "b").iterator();
        Iterator<String> iterator2 = Arrays.asList("c", "d").iterator();
        Iterator<String> iterator3 = Arrays.asList("e", "f").iterator();
        Iterator<String> chain = IteratorUtils.chainedIterator(iterator1, iterator2, iterator3);

        // it should iterate the elements in order, first from iterator1 and then from iterator2
        Assert.assertTrue(chain.hasNext());
        Assert.assertEquals("a", chain.next());
        Assert.assertTrue(chain.hasNext());
        Assert.assertEquals("b", chain.next());
        Assert.assertTrue(chain.hasNext());
        Assert.assertEquals("c", chain.next());
        Assert.assertTrue(chain.hasNext());
        Assert.assertEquals("d", chain.next());
        Assert.assertTrue(chain.hasNext());
        Assert.assertEquals("e", chain.next());
        Assert.assertTrue(chain.hasNext());
        Assert.assertEquals("f", chain.next());
        Assert.assertFalse(chain.hasNext());
    }

    @Test
    public void testChainedIteratorArrayHasNull() {
        Iterator<String> iterator1 = Arrays.asList("a", "b").iterator();
        Iterator<String> iterator2 = Arrays.asList("a", "b").iterator();

        Assert.assertThrows(NullPointerException.class, () -> IteratorUtils.chainedIterator(iterator1, null, iterator2));
    }

    @Test
    public void testChainedIteratorCollection() {
        Iterator<String> iterator1 = Arrays.asList("a", "b").iterator();
        Iterator<String> iterator2 = Arrays.asList("c", "d").iterator();
        Iterator<String> iterator3 = Arrays.asList("e", "f").iterator();
        Iterator<String> chain = IteratorUtils.chainedIterator(List.of(iterator1, iterator2, iterator3));

        // it should iterate the elements in order, first from iterator1 and then from iterator2
        Assert.assertTrue(chain.hasNext());
        Assert.assertEquals("a", chain.next());
        Assert.assertTrue(chain.hasNext());
        Assert.assertEquals("b", chain.next());
        Assert.assertTrue(chain.hasNext());
        Assert.assertEquals("c", chain.next());
        Assert.assertTrue(chain.hasNext());
        Assert.assertEquals("d", chain.next());
        Assert.assertTrue(chain.hasNext());
        Assert.assertEquals("e", chain.next());
        Assert.assertTrue(chain.hasNext());
        Assert.assertEquals("f", chain.next());
        Assert.assertFalse(chain.hasNext());
    }

    @Test
    public void testChainedIteratorCollectionHasNull() {
        Iterator<String> iterator1 = Arrays.asList("a", "b").iterator();
        Iterator<String> iterator2 = Arrays.asList("a", "b").iterator();

        Assert.assertThrows(NullPointerException.class, () -> IteratorUtils.chainedIterator(new ArrayList<>(Arrays.asList(iterator1, iterator2, null))));
    }

    @Test
    public void testChainedIterators() {
        Iterator<String> iterator1 = Arrays.asList("a", "b").iterator();
        Iterator<String> iterator2 = Arrays.asList("c", "d").iterator();
        Iterator<String> iterator3 = Arrays.asList("e", "f").iterator();
        Iterator<String> chain = IteratorUtils.chainedIterator(Arrays.asList(iterator1, iterator2, iterator3).iterator());

        // it should iterate the elements in order, first from iterator1 and then from iterator2
        Assert.assertTrue(chain.hasNext());
        Assert.assertEquals("a", chain.next());
        Assert.assertTrue(chain.hasNext());
        Assert.assertEquals("b", chain.next());
        Assert.assertTrue(chain.hasNext());
        Assert.assertEquals("c", chain.next());
        Assert.assertTrue(chain.hasNext());
        Assert.assertEquals("d", chain.next());
        Assert.assertTrue(chain.hasNext());
        Assert.assertEquals("e", chain.next());
        Assert.assertTrue(chain.hasNext());
        Assert.assertEquals("f", chain.next());
        Assert.assertFalse(chain.hasNext());
    }

    @Test
    public void testChainedIteratorsHasNull() {
        Iterator<String> iterator1 = Arrays.asList("a", "b").iterator();
        Iterator<String> iterator2 = Arrays.asList("a", "b").iterator();

        Assert.assertThrows(NullPointerException.class, () -> IteratorUtils.chainedIterator(new ArrayList<>(Arrays.asList(iterator1, iterator2, null)).iterator()));
    }

    @Test
    public void testFilterWithMatchingElements() {
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
        Iterator<Integer> filtered = IteratorUtils.filter(list.iterator(), n -> n % 2 == 0);

        List<Integer> result = new ArrayList<>();
        filtered.forEachRemaining(result::add);

        Assert.assertEquals(Arrays.asList(2, 4), result);
    }

    @Test
    public void testFilterWithNoMatchingElements() {
        List<String> list = Arrays.asList("apple", "banana", "cherry");
        Iterator<String> filtered = IteratorUtils.filter(list.iterator(), s -> s.startsWith("d"));

        Assert.assertFalse(filtered.hasNext());
    }

    @Test
    public void testFilterWithAllMatchingElements() {
        List<Integer> list = Arrays.asList(10, 20, 30, 40);
        Iterator<Integer> filtered = IteratorUtils.filter(list.iterator(), n -> n > 0);

        List<Integer> result = new ArrayList<>();
        filtered.forEachRemaining(result::add);

        Assert.assertEquals(list, result);
    }

    @Test
    public void testFilterWithEmptyIterator() {
        Iterator<String> emptyIterator = Collections.emptyIterator();
        Iterator<String> filtered = IteratorUtils.filter(emptyIterator, s -> true);

        Assert.assertFalse(filtered.hasNext());
    }

    @Test
    public void testFilterWithNullIterator() {
        Assert.assertThrows(NullPointerException.class, () -> IteratorUtils.filter(null, item -> true));
    }

    @Test
    public void testFilterWithNullPredicate() {
        Iterator<String> iterator = Arrays.asList("a", "b").iterator();
        Assert.assertThrows(NullPointerException.class, () -> IteratorUtils.filter(iterator, null));
    }

    @Test
    public void testFilterWithRemove() {
        List<String> list = new ArrayList<>(Arrays.asList("keep", "remove", "keep"));
        Iterator<String> filtered = IteratorUtils.filter(list.iterator(), "keep"::equals);

        // First element matches
        Assert.assertTrue(filtered.hasNext());
        Assert.assertEquals("keep", filtered.next());
        filtered.remove();

        // Skip "remove" as it doesn't match
        Assert.assertTrue(filtered.hasNext());
        Assert.assertEquals("keep", filtered.next());

        Assert.assertEquals(Arrays.asList("remove", "keep"), list);
    }

    @Test
    public void testFilterWithNullElements() {
        List<String> list = Arrays.asList("a", null, "b", null, "c");
        Iterator<String> filtered = IteratorUtils.filter(list.iterator(), Objects::isNull);

        List<String> result = new ArrayList<>();
        filtered.forEachRemaining(result::add);

        Assert.assertEquals(Arrays.asList(null, null), result);
    }

    @Test
    public void testFilterWithCustomObjects() {
        class Person {
            private final String name;
            private final int age;

            Person(String name, int age) {
                this.name = name;
                this.age = age;
            }

            public int getAge() {
                return age;
            }

            @Override
            public String toString() {
                return name;
            }
        }

        List<Person> people = Arrays.asList(
                new Person("Alice", 25),
                new Person("Bob", 17),
                new Person("Charlie", 30),
                new Person("David", 16)
        );

        // Filter adults (age >= 18)
        Iterator<Person> adults = IteratorUtils.filter(people.iterator(), p -> p.getAge() >= 18);

        List<String> adultNames = new ArrayList<>();
        adults.forEachRemaining(p -> adultNames.add(p.toString()));

        Assert.assertEquals(Arrays.asList("Alice", "Charlie"), adultNames);
    }

    @Test
    public void testTransformWithBasicTypes() {
        List<Integer> list = Arrays.asList(1, 2, 3);
        Iterator<String> transformed = IteratorUtils.transform(list.iterator(), n -> "Number: " + n);

        List<String> result = ListUtils.toList(transformed);

        Assert.assertEquals(Arrays.asList("Number: 1", "Number: 2", "Number: 3"), result);
    }

    @Test
    public void testTransformWithEmptyIterator() {
        Iterator<Integer> emptyIterator = Collections.emptyIterator();
        Iterator<String> transformed = IteratorUtils.transform(emptyIterator, Object::toString);

        Assert.assertFalse(transformed.hasNext());
    }

    @Test
    public void testTransformWithNullIterator() {
        Assert.assertThrows(NullPointerException.class, () -> IteratorUtils.transform(null, Object::toString));
    }

    @Test
    public void testTransformWithNullFunction() {
        Iterator<String> iterator = Arrays.asList("a", "b").iterator();
        Assert.assertThrows(NullPointerException.class, () -> IteratorUtils.transform(iterator, null));
    }

    @Test
    public void testTransformWithRemove() {
        List<Integer> list = new ArrayList<>(Arrays.asList(1, 2, 3));
        Iterator<String> transformed = IteratorUtils.transform(list.iterator(), n -> "Number: " + n);

        transformed.next(); // "Number: 1"
        transformed.remove();

        Assert.assertEquals(Arrays.asList(2, 3), list);

        transformed.next(); // "Number: 2"
        transformed.next(); // "Number: 3"
        Assert.assertFalse(transformed.hasNext());
    }

    @Test
    public void testTransformWithNullElements() {
        List<String> list = Arrays.asList("a", null, "c");
        Iterator<Integer> transformed = IteratorUtils.transform(list.iterator(),
                s -> s == null ? -1 : s.length());

        Assert.assertEquals(Integer.valueOf(1), transformed.next());
        Assert.assertEquals(Integer.valueOf(-1), transformed.next());
        Assert.assertEquals(Integer.valueOf(1), transformed.next());
        Assert.assertFalse(transformed.hasNext());
    }

    @Test
    public void testTransformWithCustomObjects() {
        class Person {
            private final String name;
            private final int age;

            Person(String name, int age) {
                this.name = name;
                this.age = age;
            }

            public String getName() {
                return name;
            }
        }

        List<Person> people = Arrays.asList(
                new Person("Alice", 25),
                new Person("Bob", 30),
                new Person("Charlie", 35)
        );

        // Transform Person objects to their names
        Iterator<String> names = IteratorUtils.transform(people.iterator(), Person::getName);

        List<String> result = ListUtils.toList(names);

        Assert.assertEquals(Arrays.asList("Alice", "Bob", "Charlie"), result);
    }

    @Test
    public void testTransformWithChaining() {
        List<Integer> list = Arrays.asList(1, 2, 3, 4);
        // First transform integers to strings
        Iterator<String> stringIterator = IteratorUtils.transform(list.iterator(), n -> "Num" + n);
        // Then transform strings to their lengths
        Iterator<Integer> lengthIterator = IteratorUtils.transform(stringIterator, String::length);

        List<Integer> result = ListUtils.toList(lengthIterator);

        Assert.assertEquals(Arrays.asList(4, 4, 4, 4), result);
    }

    @Test
    public void testCycleWithNonEmptyElements() {
        Iterator<String> cyclingIterator = IteratorUtils.cycle("a", "b", "c");

        Assert.assertTrue(cyclingIterator.hasNext());
        Assert.assertEquals("a", cyclingIterator.next());
        Assert.assertEquals("b", cyclingIterator.next());
        Assert.assertEquals("c", cyclingIterator.next());
        Assert.assertEquals("a", cyclingIterator.next()); // Cycles back
        Assert.assertEquals("b", cyclingIterator.next()); // Cycles back
        Assert.assertEquals("c", cyclingIterator.next()); // Cycles back
    }

    @Test
    public void testCycleWithSingleElement() {
        Iterator<Integer> cyclingIterator = IteratorUtils.cycle(42);

        Assert.assertTrue(cyclingIterator.hasNext());
        Assert.assertEquals(Integer.valueOf(42), cyclingIterator.next());
        Assert.assertEquals(Integer.valueOf(42), cyclingIterator.next()); // Repeats
    }

    @Test
    public void testCycleWithEmptyElements() {
        Iterator<Object> cyclingIterator = IteratorUtils.cycle();

        Assert.assertFalse(cyclingIterator.hasNext());
    }

    @Test
    public void testCycleWithNullElements() {
        Assert.assertThrows(NullPointerException.class,() -> IteratorUtils.cycle((String[]) null));
    }

    @Test
    public void testCycleWithRemove() {
        Iterator<String> cyclingIterator = IteratorUtils.cycle("x", "y");

        Assert.assertEquals("x", cyclingIterator.next());
        cyclingIterator.remove(); // Should remove "x"
        Assert.assertEquals("y", cyclingIterator.next());
        Assert.assertEquals("y", cyclingIterator.next());
        cyclingIterator.remove(); // Should remove "y"
        Assert.assertFalse(cyclingIterator.hasNext());
    }

    @Test
    public void testCycleWithNonEmptyIterable() {
        List<String> list = Arrays.asList("a", "b", "c");
        Iterator<String> cyclingIterator = IteratorUtils.cycle(list);

        Assert.assertTrue(cyclingIterator.hasNext());
        Assert.assertEquals("a", cyclingIterator.next());
        Assert.assertEquals("b", cyclingIterator.next());
        Assert.assertEquals("c", cyclingIterator.next());
        Assert.assertEquals("a", cyclingIterator.next()); // Cycles back
    }

    @Test
    public void testCycleWithSingleElementIterable() {
        List<Integer> list = Collections.singletonList(42);
        Iterator<Integer> cyclingIterator = IteratorUtils.cycle(list);

        Assert.assertTrue(cyclingIterator.hasNext());
        Assert.assertEquals(Integer.valueOf(42), cyclingIterator.next());
        Assert.assertEquals(Integer.valueOf(42), cyclingIterator.next()); // Repeats
    }

    @Test
    public void testCycleWithEmptyIterable() {
        List<Object> emptyList = Collections.emptyList();
        Iterator<Object> cyclingIterator = IteratorUtils.cycle(emptyList);

        Assert.assertFalse(cyclingIterator.hasNext());
    }

    @Test
    public void testCycleWithNullIterable() {
        Assert.assertThrows(NullPointerException.class, () -> IteratorUtils.cycle((Iterable<String>) null));
    }

    @Test
    public void testCycleWithRemoveWhenIterableAllows() {
        List<String> list = new ArrayList<>(Arrays.asList("x", "y"));
        Iterator<String> cyclingIterator = IteratorUtils.cycle(list);

        Assert.assertEquals("x", cyclingIterator.next());
        cyclingIterator.remove(); // Removes "x"
        Assert.assertEquals("y", cyclingIterator.next());
        cyclingIterator.remove(); // Removes "y"
        Assert.assertFalse(cyclingIterator.hasNext());
    }

    @Test
    public void testCycleWithRemoveWhenIterableDisallows() {
        List<String> list = Arrays.asList("x", "y");
        Iterator<String> cyclingIterator = IteratorUtils.cycle(list);

        Assert.assertEquals("x", cyclingIterator.next());
        Assert.assertThrows(UnsupportedOperationException.class, cyclingIterator::remove); // Doesn't Removes "x"
        Assert.assertEquals("y", cyclingIterator.next());
        Assert.assertEquals("x", cyclingIterator.next());
        Assert.assertTrue(cyclingIterator.hasNext());
    }
}
