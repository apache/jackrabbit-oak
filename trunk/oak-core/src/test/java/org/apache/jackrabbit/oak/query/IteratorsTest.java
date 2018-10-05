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
package org.apache.jackrabbit.oak.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Tests the filtering iterators.
 */
public class IteratorsTest {
    
    private QueryEngineSettings settings = new QueryEngineSettings();
    
    private static final Comparator<Integer> INT_COMP = new Comparator<Integer>() {

        @Override
        public int compare(Integer o1, Integer o2) {
            return o1.compareTo(o2);
        }
        
    };

    @Test
    public void distinct() {
        assertEquals("", toString(FilterIterators.newDistinct(it(), settings)));
        assertEquals("1", toString(FilterIterators.newDistinct(it(1), settings)));
        assertEquals("1, 2", toString(FilterIterators.newDistinct(it(1, 2), settings)));
        assertEquals("1, 2, 3", toString(FilterIterators.newDistinct(it(1, 2, 1, 3, 3, 1), settings)));
    }
    
    @Test
    public void limit() {
        assertEquals("", toString(FilterIterators.newLimit(it(), 0)));
        assertEquals("", toString(FilterIterators.newLimit(it(), 1)));
        assertEquals("", toString(FilterIterators.newLimit(it(), 10)));
        assertEquals("", toString(FilterIterators.newLimit(it(1), 0)));
        assertEquals("1", toString(FilterIterators.newLimit(it(1), 1)));
        assertEquals("1", toString(FilterIterators.newLimit(it(1), 10)));
        assertEquals("", toString(FilterIterators.newLimit(it(1, 2), 0)));
        assertEquals("1", toString(FilterIterators.newLimit(it(1, 2), 1)));
        assertEquals("1, 2", toString(FilterIterators.newLimit(it(1, 2), 10)));
        assertEquals("1", toString(FilterIterators.newLimit(it(1, 2, 3), 1)));
        assertEquals("1, 2", toString(FilterIterators.newLimit(it(1, 2, 3), 2)));
        assertEquals("1, 2, 3", toString(FilterIterators.newLimit(it(1, 2, 3), 3)));
        assertEquals("1, 2, 3", toString(FilterIterators.newLimit(it(1, 2, 3), 4)));
    }
    
    @Test
    public void offset() {
        assertEquals("", toString(FilterIterators.newOffset(it(), 0)));
        assertEquals("1", toString(FilterIterators.newOffset(it(1), 0)));
        assertEquals("1, 2", toString(FilterIterators.newOffset(it(1, 2), 0)));
        assertEquals("", toString(FilterIterators.newOffset(it(), 1)));
        assertEquals("", toString(FilterIterators.newOffset(it(1), 1)));
        assertEquals("2", toString(FilterIterators.newOffset(it(1, 2), 1)));
        assertEquals("", toString(FilterIterators.newOffset(it(1, 2), 2)));
        assertEquals("", toString(FilterIterators.newOffset(it(1, 2), 3)));
        assertEquals("2, 3", toString(FilterIterators.newOffset(it(1, 2, 3), 1)));
    }
    
    @Test
    public void sort() {
        assertEquals("", toString(FilterIterators.newSort(it(new Integer[]{}), INT_COMP, 0, settings)));
        assertEquals("", toString(FilterIterators.newSort(it(1), INT_COMP, 0, settings)));
        assertEquals("1", toString(FilterIterators.newSort(it(1), INT_COMP, 1, settings)));
        assertEquals("1", toString(FilterIterators.newSort(it(1), INT_COMP, 2, settings)));
        assertEquals("1", toString(FilterIterators.newSort(it(1, 2), INT_COMP, 1, settings)));
        assertEquals("1", toString(FilterIterators.newSort(it(2, 1), INT_COMP, 1, settings)));
        assertEquals("1, 2", toString(FilterIterators.newSort(it(1, 2, 3), INT_COMP, 2, settings)));
        assertEquals("1, 2", toString(FilterIterators.newSort(it(3, 2, 1), INT_COMP, 2, settings)));
        assertEquals("1, 1, 2", toString(FilterIterators.newSort(it(3, 3, 2, 1, 1), INT_COMP, 3, settings)));
    }

    @Test
    public void sortCompareCalls() {
        sortCompareCalls(10000, 0);
        sortCompareCalls(10000, 1);
        sortCompareCalls(10000, 10);
        sortCompareCalls(10000, 100);
        sortCompareCalls(10000, 1000);
        sortCompareCalls(10000, 10000);
        sortCompareCalls(10000, 100000);
        sortCompareCalls(10000, 1000000);
        sortCompareCalls(10000, Integer.MAX_VALUE);
    }
    
    private void sortCompareCalls(int count, int keep) {
        
        int len = 1000;
        Random r = new Random(1);
        Integer[] list = new Integer[len];
        for (int i = 0; i < len; i++) {
            list[i] = r.nextInt();
        }
        final AtomicInteger compareCalls = new AtomicInteger();
        Comparator<Integer> comp = new Comparator<Integer>() {

            @Override
            public int compare(Integer o1, Integer o2) {
                compareCalls.incrementAndGet();
                return o1.compareTo(o2);
            }
            
        };
        Iterator<Integer> it = FilterIterators.newSort(it(list), comp, keep, settings);
        int old = Integer.MIN_VALUE;
        while (it.hasNext()) {
            int x = it.next();
            assertTrue(x >= old);
            old = x;
        }
        // n * log2(n)
        int maxCompAll = (int) (len * Math.log(len) / Math.log(2));
        int maxCompKeep = (int) (len * Math.log(3.0 * keep) / Math.log(2));
        int maxComp = Math.min(maxCompAll, Math.max(0, maxCompKeep));
        assertTrue(compareCalls.get() <= maxComp);
    }

    @Test
    public void combined() {

        // no filtering
        assertEquals("3, 3, 2, 1", 
                toString(FilterIterators.newCombinedFilter(
                it(3, 3, 2, 1), false, Long.MAX_VALUE, 0, null, settings)));
        
        // distinct
        assertEquals("3, 2, 1", 
                toString(FilterIterators.newCombinedFilter(
                it(3, 3, 2, 1), true, Long.MAX_VALUE, 0, null, settings)));

        // order by
        assertEquals("1, 2, 3, 3", 
                toString(FilterIterators.newCombinedFilter(
                it(3, 3, 2, 1), false, Long.MAX_VALUE, 0, INT_COMP, settings)));

        // distinct & order by
        assertEquals("1, 2, 3", 
                toString(FilterIterators.newCombinedFilter(
                it(3, 3, 2, 1), true, Long.MAX_VALUE, 0, INT_COMP, settings)));
        
        // limit
        assertEquals("3, 3", 
                toString(FilterIterators.newCombinedFilter(
                it(3, 3, 2, 1), false, 2, 0, null, settings)));

        // offset
        assertEquals("3, 2, 1", 
                toString(FilterIterators.newCombinedFilter(
                it(3, 3, 2, 1), false, Long.MAX_VALUE, 1, null, settings)));

        // limit & offset
        assertEquals("3, 2", 
                toString(FilterIterators.newCombinedFilter(
                it(3, 3, 2, 1), false, 2, 1, null, settings)));

        // distinct & limit & offset
        assertEquals("2, 1", 
                toString(FilterIterators.newCombinedFilter(
                it(3, 3, 2, 1), true, 2, 1, null, settings)));

        // distinct & limit & offset & order by
        assertEquals("2, 3", 
                toString(FilterIterators.newCombinedFilter(
                it(3, 3, 2, 1), true, 2, 1, INT_COMP, settings)));

}

    private static <K> Iterator<K> it(K... x) {
        return Collections.unmodifiableCollection(Lists.newArrayList(x)).iterator();
    }
    
    private static <K> String toString(Iterator<K> it) {
        StringBuilder buff = new StringBuilder();
        while (it.hasNext()) {
            if (buff.length() > 0) {
                buff.append(", ");
            }
            assertTrue(it.hasNext());
            buff.append(it.next());
        }
        assertFalse(it.hasNext());
        try {
            it.remove();
            fail();
        } catch (UnsupportedOperationException e) {
            // expected
        }
        try {
            it.next();
            fail();
        } catch (NoSuchElementException e) {
            // expected
        }
        return buff.toString();
    }
    
}
