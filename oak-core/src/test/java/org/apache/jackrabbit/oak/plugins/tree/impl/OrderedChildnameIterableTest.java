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
package org.apache.jackrabbit.oak.plugins.tree.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class OrderedChildnameIterableTest {

    static final List<String> ALL_CHILDREN = List.of("1","2","3","4","5");

    // Track iterator access for testing lazy loading
    private static class TrackingIterable implements Iterable<String> {
        private final List<String> elements;
        private int accessCount = 0;
        private final List<String> accessedElements = new ArrayList<>();

        TrackingIterable(List<String> elements) {
            this.elements = elements;
        }

        @Override
        public Iterator<String> iterator() {
            return new Iterator<String>() {
                private int index = 0;

                @Override
                public boolean hasNext() {
                    return index < elements.size();
                }

                @Override
                public String next() {
                    String element = elements.get(index++);
                    accessCount++;
                    accessedElements.add(element);
                    return element;
                }
            };
        }

        public int getAccessCount() {
            return accessCount;
        }

        public List<String> getAccessedElements() {
            return accessedElements;
        }
    }

    List<String> iteratorToList(Iterator<String> iter) {
        List<String> result = new ArrayList<>();
        iter.forEachRemaining(result::add);
        return result;
    }

    @Test
    public void noOrderedChildren() {
        // all children are returned in their order
        OrderedChildnameIterator iterable = new OrderedChildnameIterator(List.of(),ALL_CHILDREN);
        Assert.assertEquals(ALL_CHILDREN, iteratorToList(iterable));
    }

    @Test
    public void orderedChildren() {
        // only 2 child nodes ordered, return them up front
        OrderedChildnameIterator iterable = new OrderedChildnameIterator(List.of("4","5"),ALL_CHILDREN);
        Assert.assertEquals(List.of("4","5","1","2","3"), iteratorToList(iterable));
    }

    @Test
    public void orderedChildrenWithNonExistingOrderedChild() {
        // the ordered list contains non-existing childnames, which are not part of children list
        OrderedChildnameIterator iterable = new OrderedChildnameIterator(List.of("4","nonexisting1","5","nonexisting2"),ALL_CHILDREN);
        Assert.assertEquals(List.of("4","5","1","2","3"), iteratorToList(iterable));
    }

    @Test
    public void orderedChildrenWithOnlyNonExistingOrderedChild() {
        // the ordered list contains non-existing childnames, which are not part of children list
        OrderedChildnameIterator iterable = new OrderedChildnameIterator(List.of("nonexisting"),ALL_CHILDREN);
        Assert.assertEquals(List.of("1","2","3","4","5"), iteratorToList(iterable));
    }

    @Test
    public void onlyOrderedChildrenAvailable() {
        // the orderedChildren property is populated, but no children are available
        OrderedChildnameIterator iterable = new OrderedChildnameIterator(List.of("1","2"),List.of());
        Assert.assertEquals(List.of(), iteratorToList(iterable));
    }

    @Test
    public void testLazyLoading() {
        // Create tracking iterable for allChildren
        TrackingIterable trackingAllChildren = new TrackingIterable(ALL_CHILDREN);

        OrderedChildnameIterator iterator = new OrderedChildnameIterator(
            List.of("4", "1"),
            trackingAllChildren);

        // Get first element ("4")
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("4", iterator.next());
        // iterated through 4 elements in allChildren
        Assert.assertEquals(4, trackingAllChildren.getAccessCount());
        Assert.assertEquals(List.of("1", "2", "3", "4"), trackingAllChildren.getAccessedElements());

        // Get second element ("1")
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("1", iterator.next());
        // No additional access to allChildren
        Assert.assertEquals(4, trackingAllChildren.getAccessCount());
        Assert.assertEquals(List.of("1", "2", "3", "4"), trackingAllChildren.getAccessedElements());

        // Get remaining elements
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("2", iterator.next());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("3", iterator.next());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("5", iterator.next());

        // No more elements should be accessed since we already had them all
        Assert.assertEquals(5, trackingAllChildren.getAccessCount());
        Assert.assertFalse(iterator.hasNext());
    }
}
