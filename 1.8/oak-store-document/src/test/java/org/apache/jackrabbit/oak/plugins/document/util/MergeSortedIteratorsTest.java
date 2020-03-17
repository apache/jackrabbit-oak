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
package org.apache.jackrabbit.oak.plugins.document.util;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.StableRevisionComparator;
import org.junit.Test;

import com.google.common.collect.Lists;

import static junit.framework.Assert.assertEquals;

/**
 * Tests for {@link MergeSortedIterators}.
 */
public class MergeSortedIteratorsTest {

    @SuppressWarnings("unchecked")
    @Test
    public void test() {
        assertEquals(list(), sort());
        assertEquals(list(), sort(list()));
        assertEquals(list(1, 2, 3, 4), sort(list(1, 2), list(3, 4)));
        assertEquals(list(1, 2, 3, 4), sort(list(1, 3), list(2, 4)));
        assertEquals(list(1, 2, 3), sort(list(1, 3), list(2)));
        assertEquals(list(1, 2, 3, 4), sort(list(1, 4), list(2, 3)));
        assertEquals(list(1, 2, 3, 4, 5, 6), sort(list(1, 5), list(2, 4), list(3, 6)));
    }

    /**
     * See OAK-1233 and OAK-1479
     */
    @Test(expected = IllegalStateException.class)
    public void testData() {
        List<Iterator<Revision>> iterators = prepareData();
        final Iterator<Iterator<Revision>> it = iterators.iterator();
        final Comparator<Revision> comp = StableRevisionComparator.REVERSE;
        MergeSortedIterators<Revision> sort = new MergeSortedIterators<Revision>(comp) {
            @Override
            public Iterator<Revision> nextIterator() {
                return it.hasNext() ? it.next() : null;
            }
        };
        while (sort.hasNext()) {
            sort.next();
        }
    }

    private List<Integer> sort(List<Integer>... lists) {
        List<Iterator<Integer>> iterators = Lists.newArrayList();
        for (List<Integer> list : lists) {
            iterators.add(list.iterator());
        }
        final Iterator<Iterator<Integer>> it = iterators.iterator();
        MergeSortedIterators<Integer> sort = new MergeSortedIterators<Integer>(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o1.compareTo(o2);
            }
        }) {
            @Override
            public Iterator<Integer> nextIterator() {
                return it.hasNext() ? it.next() : null;
            }
        };
        List<Integer> sorted = new ArrayList<Integer>();
        while (sort.hasNext()) {
            sorted.add(sort.next());
        }
        return sorted;
    }

    final static String[][] TEST_DATA = {
            {
                    "r14298e4442a-0-2",
                    "r14298e443e5-0-2",
            },
            {
                    "r14298e4548d-0-1"
            },
    };

    private static List<Iterator<Revision>> prepareData() {
        List<Iterator<Revision>> result = new ArrayList<Iterator<Revision>>();
        for (String[] revsString : TEST_DATA) {
            List<Revision> revs = new ArrayList<Revision>();
            for (String r : revsString) {
                revs.add(Revision.fromString(r));
            }
            result.add(revs.iterator());
        }
        return result;
    }

    private static List<Integer> list(int... values) {
        List<Integer> list = new ArrayList<Integer>();
        for (int v : values) {
            list.add(v);
        }
        return list;
    }
}
