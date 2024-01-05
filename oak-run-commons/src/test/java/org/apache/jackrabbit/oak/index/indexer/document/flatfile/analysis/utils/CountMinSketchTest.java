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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

public class CountMinSketchTest {

    @Test
    public void test() {
        int size = 100_000;
        CountSketchError result;

        result = test(size, false);
        assertTrue(result.stdDevEntryEstimation < 5);
    }

    @Test(expected = IllegalArgumentException.class)
    public void illegalSizeCount() {
        new CountMinSketch(5, 3);
    }

    @Test(expected = IllegalArgumentException.class)
    public void illegalArgs() {
        new CountMinSketch(15, 128);
    }

    @Test(expected = IllegalArgumentException.class)
    public void evenHash() {
        new CountMinSketch(4, 4);
    }

    @Test
    public void testFusedAddEstimate() {
        CountMinSketch c1 = new CountMinSketch(5, 16);
        CountMinSketch c2 = new CountMinSketch(5, 16);
        Random r = new Random(1);
        for (int i = 0; i < 1000; i++) {
            long h = r.nextLong();
            c1.add(h);
            double e1 = c1.estimate(h);
            double e2 = c2.addAndEstimate(h);
            assertEquals(e1 + " " + e2, 0, Double.compare(e1, e2));
        }
    }

    private static CountSketchError test(int size, boolean debug) {
        Random r = new Random(42);
        double sumSquareErrorEntry = 0;
        int countEntry = 0;
        for (double skew = 2; skew < 2000; skew *= 2) {
            for (int repeat = 1; repeat <= 2; repeat++) {
                for (int sort = 0; sort <= 1; sort++) {
                    long[] data = randomData(size, skew, r, repeat);
                    long x = r.nextLong();
                    if (sort > 0) {
                        Arrays.sort(data);
                        if (sort > 1) {
                            reverse(data);
                        }
                    }
                    CountMinSketch est = new CountMinSketch(5, 16);
                    for (int i = 0; i < size; i++) {
                        est.add(Hash.hash64(x + data[i]));
                    }
                    int[] counts = getCounts(data);
                    for (int i = 0; i < 10; i++) {
                        long e = est.estimate(Hash.hash64(x + i));
                        long expectedPercent = (int) (100. * counts[i] / size);
                        long estPercent = (int) (100. * e / size);
                        if (debug) {
                            System.out.println("  " + i + " estimated " + e + " = " + estPercent + "%; real "
                                    + counts[i] + " = " + expectedPercent + "%");
                        }
                        double err = estPercent - expectedPercent;
                        sumSquareErrorEntry += err * err;
                        countEntry++;
                    }
                }
            }
        }
        CountSketchError result = new CountSketchError();
        result.stdDevEntryEstimation = Math.sqrt(sumSquareErrorEntry / countEntry);
        return result;
    }

    private static void reverse(long[] data) {
        for (int i = 0; i < data.length / 2; i++) {
            long temp = data[i];
            data[i] = data[data.length - 1 - i];
            data[data.length - 1 - i] = temp;
        }
    }

    static int[] getCounts(long[] data) {
        int[] counts = new int[10];
        for (int i = 0; i < 10; i++) {
            int count = 0;
            for (long d : data) {
                if (d == i) {
                    count++;
                }
            }
            counts[i] = count;
        }
        return counts;
    }

    static long[] randomData(int size, double skew, Random r, int repeat) {
        long[] data = new long[size];
        for (int i = 0; i < size; i++) {
            long m = (long) (size * Math.pow(r.nextDouble(), skew));
            if (repeat > 1) {
                m = (m / repeat * repeat) + (r.nextInt(repeat));
            }
            data[i] = m;
        }
        return data;
    }

    static class CountSketchError {
        double stdDevEntryEstimation;

        @Override
        public String toString() {
            return "entry " + stdDevEntryEstimation;
        }
    }
}
