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
package org.apache.jackrabbit.oak.commons.collections;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

/** Tests for {@link ConcatenatingIterator} through the public chaining APIs. */
public class ConcatenatingIteratorTest {
    @Test
    public void allNineOverloadsAvoidExponentialPolling() {
        for (Overload overload : Overload.values()) {
            for (boolean prependEmpty : List.of(false, true)) {
                AtomicInteger calls = new AtomicInteger();
                Iterable<Integer> chain = counted(calls);
                for (int i = 0; i < 20; i++) {
                    chain = prependEmpty ? concatenate(overload, List.of(), chain)
                            : concatenate(overload, chain, List.of());
                }
                Assert.assertTrue(chain.iterator().hasNext());
                Assert.assertEquals("overload " + overload, 1, calls.get());
            }
        }
    }

    @Test(timeout = 30000)
    public void deepIterableAndIteratorNestingIsStackSafe() {
        for (Overload overload : List.of(Overload.TWO_ITERABLES, Overload.ITERABLE_OF_ITERABLES, Overload.TWO_ITERATORS)) {
            Iterable<Integer> chain = List.of(1);
            if (overload == Overload.TWO_ITERATORS) {
                Iterator<Integer> iterator = chain.iterator();
                for (int i = 0; i < 100000; i++) {
                    iterator = IteratorUtils.chainedIterator(iterator, Collections.emptyIterator());
                }
                Assert.assertEquals(Integer.valueOf(1), iterator.next());
                Assert.assertFalse(iterator.hasNext());
            } else {
                for (int i = 0; i < 100000; i++) {
                    chain = concatenate(overload, chain, List.of());
                }
                Iterator<Integer> iterator = chain.iterator();
                Assert.assertEquals(Integer.valueOf(1), iterator.next());
                Assert.assertFalse(iterator.hasNext());
            }
        }
    }

    @Test(timeout = 10000)
    public void hundredsOfAppendedInputsRemainConsumable() {
        for (Overload overload : Overload.values()) {
            Iterable<Integer> chain = List.of(0);
            for (int i = 1; i <= 128; i++) {
                chain = concatenate(overload, chain, List.of(i));
            }
            Iterator<Integer> iterator = chain.iterator();
            for (int i = 0; i <= 128; i++) {
                Assert.assertTrue("overload " + overload + ", element " + i, iterator.hasNext());
                Assert.assertEquals(Integer.valueOf(i), iterator.next());
            }
            Assert.assertFalse(iterator.hasNext());
        }
    }

    @Test(timeout = 10000)
    public void manyValuesBehindDeepNestingDoNotRepeatTheTraversal() {
        int size = 100000;
        Iterator<Integer> iterator = Collections.nCopies(size, 1).iterator();
        for (int i = 0; i < size; i++) {
            iterator = IteratorUtils.chainedIterator(iterator, Collections.emptyIterator());
        }
        int count = 0;
        while (iterator.hasNext()) {
            Assert.assertEquals(Integer.valueOf(1), iterator.next());
            count++;
        }
        Assert.assertEquals(size, count);
    }

    @Test
    public void polledAndPartiallyConsumedInputsKeepTheirActiveIterator() {
        Iterator<Integer> child = IteratorUtils.chainedIterator(List.of(1, 2).iterator(), List.of(3).iterator());
        Assert.assertTrue(child.hasNext());
        Assert.assertEquals(Integer.valueOf(1), child.next());
        Assert.assertTrue(child.hasNext());
        Iterator<Integer> parent = IteratorUtils.chainedIterator(child, List.of(4).iterator());
        List<Integer> values = new ArrayList<>();
        parent.forEachRemaining(values::add);
        Assert.assertEquals(List.of(2, 3, 4), values);
    }

    @Test
    public void pollingParentDoesNotTakePendingInputsAwayFromChild() {
        Iterator<Integer> child = IteratorUtils.chainedIterator(List.of(1).iterator(), List.of(2, 3).iterator());
        Iterator<Integer> parent = IteratorUtils.chainedIterator(child, List.of(4).iterator());
        Assert.assertTrue(parent.hasNext());
        Assert.assertEquals(Integer.valueOf(1), child.next());
        Assert.assertEquals(Integer.valueOf(2), child.next());
        Assert.assertEquals(Integer.valueOf(3), parent.next());
        Assert.assertEquals(Integer.valueOf(4), parent.next());
        Assert.assertFalse(parent.hasNext());
        Assert.assertFalse(child.hasNext());
    }

    @Test
    public void alternatingParentsAndChildInvalidateCachedPaths() {
        Iterator<Integer> child = IteratorUtils.chainedIterator(
                List.of(1).iterator(), List.of(2).iterator(), List.of(3).iterator());
        Iterator<Integer> first = IteratorUtils.chainedIterator(child, List.of(4).iterator());
        Iterator<Integer> second = IteratorUtils.chainedIterator(child, List.of(5).iterator());
        Assert.assertTrue(first.hasNext());
        Assert.assertTrue(second.hasNext());
        Assert.assertEquals(Integer.valueOf(1), child.next());
        Assert.assertEquals(Integer.valueOf(2), first.next());
        Assert.assertEquals(Integer.valueOf(3), second.next());
        Assert.assertEquals(Integer.valueOf(4), first.next());
        Assert.assertEquals(Integer.valueOf(5), second.next());
        Assert.assertFalse(child.hasNext());
        Assert.assertFalse(first.hasNext());
        Assert.assertFalse(second.hasNext());
    }

    @Test
    public void childRemovalTracksValuesReturnedThroughParent() {
        List<Integer> values = new ArrayList<>(List.of(1, 2));
        Iterator<Integer> child = IteratorUtils.chainedIterator(values.iterator(), Collections.emptyIterator());
        Iterator<Integer> parent = IteratorUtils.chainedIterator(child, List.of(3).iterator());
        Assert.assertEquals(Integer.valueOf(1), parent.next());
        child.remove();
        Assert.assertEquals(List.of(2), values);
        Assert.assertEquals(Integer.valueOf(2), parent.next());
        Assert.assertTrue(parent.hasNext());
        child.remove();
        Assert.assertEquals(List.of(), values);
        Assert.assertThrows(IllegalStateException.class, child::remove);
        Assert.assertEquals(Integer.valueOf(3), parent.next());
    }

    @Test
    public void borrowingAnAlreadyTraversedNestedChildKeepsRemovalState() {
        List<Integer> values = new ArrayList<>(List.of(1, 2, 3));
        Iterator<Integer> leaf = IteratorUtils.chainedIterator(values.iterator(), Collections.emptyIterator());
        Iterator<Integer> middle = IteratorUtils.chainedIterator(leaf, List.of(4).iterator());
        Assert.assertEquals(Integer.valueOf(1), middle.next());
        Iterator<Integer> parent = IteratorUtils.chainedIterator(middle, List.of(5).iterator());
        Assert.assertTrue(parent.hasNext());
        leaf.remove();
        Assert.assertEquals(List.of(2, 3), values);
        Assert.assertEquals(Integer.valueOf(2), parent.next());
        Assert.assertEquals(Integer.valueOf(3), middle.next());
        Assert.assertEquals(Integer.valueOf(4), parent.next());
        Assert.assertEquals(Integer.valueOf(5), parent.next());
        Assert.assertFalse(parent.hasNext());
    }

    @Test
    public void lazyOuterAndInnerSourcesOpenOnlyWhenNeeded() {
        AtomicInteger opened = new AtomicInteger();
        Iterable<Integer> second = () -> { opened.incrementAndGet(); return List.of(2).iterator(); };
        Iterable<Integer> chain = IterableUtils.chainedIterable(List.of(List.of(1), second));
        Iterator<Integer> iterator = chain.iterator();
        Assert.assertEquals(0, opened.get());
        Assert.assertEquals(Integer.valueOf(1), iterator.next());
        Assert.assertEquals(0, opened.get());
        Assert.assertEquals(Integer.valueOf(2), iterator.next());
        Assert.assertEquals(1, opened.get());
    }

    @Test
    public void flatteningDoesNotOpenNestedOuterSourcesEarly() {
        AtomicInteger opened = new AtomicInteger();
        Iterable<Iterable<Integer>> sources = () -> {
            opened.incrementAndGet();
            return List.<Iterable<Integer>>of(List.of(2)).iterator();
        };
        Iterable<Integer> nested = IterableUtils.chainedIterable(sources);
        Iterator<Integer> iterator = IterableUtils.chainedIterable(List.of(1), nested).iterator();
        Assert.assertEquals(0, opened.get());
        Assert.assertEquals(Integer.valueOf(1), iterator.next());
        Assert.assertEquals(0, opened.get());
        Assert.assertEquals(Integer.valueOf(2), iterator.next());
        Assert.assertEquals(1, opened.get());
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void wrappedChildIteratorsKeepTheirStateAcrossIterableFlattening() {
        Iterator<Integer> child = IteratorUtils.chainedIterator(List.of(1).iterator(), List.of(2, 3).iterator());
        Iterator<Integer> iterator = IterableUtils.chainedIterable(
                List.<Integer>of(), IteratorUtils.toIterable(child), List.of(4)).iterator();
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals(Integer.valueOf(1), child.next());
        Assert.assertEquals(Integer.valueOf(2), child.next());
        Assert.assertEquals(Integer.valueOf(3), iterator.next());
        Assert.assertEquals(Integer.valueOf(4), iterator.next());
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void infiniteOuterInputDoesNotNeedMaterialization() {
        Iterable<Iterable<Integer>> infinite = () -> new Iterator<>() {
            @Override
            public boolean hasNext() { return true; }
            @Override
            public Iterable<Integer> next() { return List.of(1); }
        };
        Iterator<Integer> iterator = IterableUtils.chainedIterable(infinite).iterator();
        Assert.assertEquals(Integer.valueOf(1), iterator.next());
        Assert.assertEquals(Integer.valueOf(1), iterator.next());
    }

    @Test
    public void removalTracksLastReturnedIteratorAcrossBoundaryPolling() {
        List<Integer> first = new ArrayList<>(List.of(1));
        List<Integer> second = new ArrayList<>(List.of(2));
        Iterator<Integer> iterator = IterableUtils.chainedIterable(first, second).iterator();
        Assert.assertThrows(IllegalStateException.class, iterator::remove);
        Assert.assertEquals(Integer.valueOf(1), iterator.next());
        Assert.assertTrue(iterator.hasNext());
        iterator.remove();
        Assert.assertEquals(List.of(), first);
        Assert.assertEquals(List.of(2), second);
        Assert.assertThrows(IllegalStateException.class, iterator::remove);
    }

    @Test
    public void repeatedIterationEmptyInputsAndNullValuesPreserveOrder() {
        Iterable<Integer> chain = IterableUtils.chainedIterable(List.of(), Arrays.asList(1, null), List.of(), List.of(2));
        for (int i = 0; i < 2; i++) {
            List<Integer> actual = new ArrayList<>();
            chain.forEach(actual::add);
            Assert.assertEquals(Arrays.asList(1, null, 2), actual);
        }
    }

    @Test
    public void exhaustedIteratorRejectsNextAndUnsupportedRemovalIsPreserved() {
        Iterator<Integer> iterator = IterableUtils.chainedIterable(List.of(), List.of(1)).iterator();
        Assert.assertEquals(Integer.valueOf(1), iterator.next());
        Assert.assertThrows(UnsupportedOperationException.class, iterator::remove);
        Assert.assertFalse(iterator.hasNext());
        Assert.assertFalse(iterator.hasNext());
        Assert.assertThrows(NoSuchElementException.class, iterator::next);
    }

    @Test
    public void nullSourcesAreRejectedWithoutOpeningEarlierInputs() {
        Iterable<Integer> unopened = () -> { throw new AssertionError("input opened during validation"); };
        for (Overload overload : List.of(Overload.TWO_ITERABLES, Overload.THREE_ITERABLES,
                Overload.FOUR_ITERABLES, Overload.VARARGS_ITERABLES)) {
            Assert.assertThrows(NullPointerException.class, () -> concatenate(overload, unopened, null));
        }
        Assert.assertThrows(NullPointerException.class,
                () -> IterableUtils.chainedIterable((Iterable<Integer>[]) null));
        Assert.assertThrows(NullPointerException.class,
                () -> IterableUtils.chainedIterable((Iterable<Iterable<Integer>>) null));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void iterableVarargsRemainAViewOfTheInputArray() {
        Iterable<Integer>[] inputs = (Iterable<Integer>[]) new Iterable<?>[] {List.of(1), List.of(2)};
        Iterable<Integer> chain = IterableUtils.chainedIterable(inputs);
        inputs[0] = List.of(3);
        List<Integer> actual = new ArrayList<>();
        chain.forEach(actual::add);
        Assert.assertEquals(List.of(3, 2), actual);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void iteratorVarargsKeepTheirExistingSnapshotBehavior() {
        Iterator<Integer>[] inputs = (Iterator<Integer>[]) new Iterator<?>[] {
                List.of(1).iterator(), List.of(2).iterator()};
        Iterator<Integer> chain = IteratorUtils.chainedIterator(inputs);
        inputs[0] = List.of(3).iterator();
        List<Integer> actual = new ArrayList<>();
        chain.forEachRemaining(actual::add);
        Assert.assertEquals(List.of(1, 2), actual);
    }

    @Test(timeout = 10000)
    public void oneHundredThousandFlatInputsKeepLinearPolling() {
        AtomicInteger calls = new AtomicInteger();
        int size = 100000;
        int count = 0;
        for (Integer value : IterableUtils.chainedIterable(Collections.nCopies(size, counted(calls)))) {
            Assert.assertEquals(Integer.valueOf(1), value);
            count++;
        }
        Assert.assertEquals(size, count);
        Assert.assertTrue("leaf calls: " + calls.get(), calls.get() <= 4 * size);
    }

    private static Iterable<Integer> concatenate(Overload overload, Iterable<Integer> a, Iterable<Integer> b) {
        return switch (overload) {
            case TWO_ITERABLES -> IterableUtils.chainedIterable(a, b);
            case THREE_ITERABLES -> IterableUtils.chainedIterable(a, b, List.of());
            case FOUR_ITERABLES -> IterableUtils.chainedIterable(a, b, List.of(), List.of());
            case VARARGS_ITERABLES -> IterableUtils.chainedIterable(a, b, List.of(), List.of(), List.of());
            case ITERABLE_OF_ITERABLES -> IterableUtils.chainedIterable(List.of(a, b));
            case TWO_ITERATORS -> () -> IteratorUtils.chainedIterator(a.iterator(), b.iterator());
            case VARARGS_ITERATORS -> () -> IteratorUtils.chainedIterator(a.iterator(), b.iterator(), Collections.emptyIterator());
            case COLLECTION_OF_ITERATORS -> () -> IteratorUtils.chainedIterator(List.<Iterator<? extends Integer>>of(a.iterator(), b.iterator()));
            case ITERATOR_OF_ITERATORS -> () -> IteratorUtils.chainedIterator(List.<Iterator<? extends Integer>>of(a.iterator(), b.iterator()).iterator());
        };
    }

    private static Iterable<Integer> counted(AtomicInteger calls) {
        return () -> new Iterator<>() {
            private boolean available = true;

            @Override
            public boolean hasNext() {
                calls.incrementAndGet();
                return available;
            }

            @Override
            public Integer next() {
                if (!available) {
                    throw new NoSuchElementException();
                }
                available = false;
                return 1;
            }
        };
    }

    private enum Overload {
        TWO_ITERABLES,
        THREE_ITERABLES,
        FOUR_ITERABLES,
        VARARGS_ITERABLES,
        ITERABLE_OF_ITERABLES,
        TWO_ITERATORS,
        VARARGS_ITERATORS,
        COLLECTION_OF_ITERATORS,
        ITERATOR_OF_ITERATORS
    }
}
