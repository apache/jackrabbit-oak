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
package org.apache.jackrabbit.oak.plugins.index.search.util;

import org.apache.jackrabbit.guava.common.collect.AbstractIterator;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TapeSamplingTest {
    @Test
    public void testWithHighestRandom() {
        final int start = 10;
        final int end = 30;
        final int k = 10;
        final Random r = new Random() {
            @Override
            public int nextInt(int i) {
                return i - 1;
            }
        };

        List<Integer> input = range(start, end);
        TapeSampling<Integer> res = new TapeSampling<>(r, input.iterator(), input.size(), k);

        List<Integer> samples = CollectionUtils.toList(res.getSamples());
        List<Integer> expected = range(end - k + 1, end);

        assertEquals(expected, samples);
    }

    @Test
    public void testWithLowestRandom() {
        final int start = 10;
        final int end = 30;
        final int k = 10;
        final Random r = new Random() {
            @Override
            public int nextInt(int i) {
                return 0;
            }
        };

        List<Integer> input = range(start, end);
        TapeSampling<Integer> res = new TapeSampling<>(r, input.iterator(), input.size(), k);

        List<Integer> samples = CollectionUtils.toList(res.getSamples());
        List<Integer> expected = range(start, start + k - 1);

        assertEquals(expected, samples);
    }

    @Test
    public void allItemsWhenKisN() {
        final int start = 11;
        final int end = 20;
        final int k = 10;
        final Random r = new Random();

        List<Integer> input = range(start, end);
        TapeSampling<Integer> res = new TapeSampling<>(r, input.iterator(), input.size(), k);

        List<Integer> samples = CollectionUtils.toList(res.getSamples());
        List<Integer> expected = input;

        assertEquals(expected, samples);
    }

    @Test
    public void sampleExactlyK() {
        final int start = 11;
        final int end = 1000;
        final int k = 10;
        final Random r = new Random();

        List<Integer> input = range(start, end);
        TapeSampling<Integer> res = new TapeSampling<>(r, input.iterator(), input.size(), k);

        assertEquals("Must sample exactly " + k + " items", k, Iterators.size(res.getSamples()));
    }

    @Test
    public void sampleBias() {
        int size = 200;
        int k = 20;
        int[] counts = new int[size];
        Random r = new Random(42);
        int testCount = 100 * size;
        for (int i = 0; i < testCount; i++) {
            List<Integer> input = range(0, size - 1);
            TapeSampling<Integer> res = new TapeSampling<>(r, input.iterator(), input.size(), k);
            Iterator<Integer> it = res.getSamples();
            while (it.hasNext()) {
                counts[it.next()]++;
            }
        }
        int expectedCount = testCount / (size / k);
        for (int i = 0; i < size; i++) {
            assertTrue(counts[i] > expectedCount* 0.9 && counts[i] < expectedCount * 1.1);
        }
    }

    private List<Integer> range(final int start, final int end) {
        Iterator<Integer> iter = new AbstractIterator<Integer>() {
            int curr = start;
            @Override
            protected Integer computeNext() {
                if (curr > end) {
                    return endOfData();
                }

                return curr++;
            }
        };

        return CollectionUtils.toList(iter);
    }
}
