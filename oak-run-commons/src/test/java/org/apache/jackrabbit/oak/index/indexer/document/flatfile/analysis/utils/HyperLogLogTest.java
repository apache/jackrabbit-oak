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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class HyperLogLogTest {

    @Test(expected = IllegalArgumentException.class)
    public void illegalHyperLogLogTooSmall() {
        new HyperLogLog(8, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void illegalHyperLogLogNotPowerOfTwo() {
        new HyperLogLog(30, 0);
    }

    @Test
    public void smallSet() {
        HyperLogLog hll100 = new HyperLogLog(16, 100);
        assertEquals(0, hll100.estimate());
        HyperLogLog hll0 = new HyperLogLog(16, 0);
        assertEquals(0, hll0.estimate());
        for (int i = 0; i < 10_000; i++) {
            hll100.add(i % 100);
            hll0.add(i % 100);
        }
        assertEquals(100, hll100.estimate());
        assertNotEquals(100, hll0.estimate());
    }

    @Test
    public void test() {
        int testCount = 50;
        for (int m = 8; m <= 128; m *= 2) {
            double avg = Math.sqrt(averageOverRange(m, 30_000, testCount, false, 2));
            int min, max;
            switch (m) {
            case 8:
                min = 16;
                max = 17;
                break;
            case 16:
                min = 22;
                max = 23;
                break;
            case 32:
                min = 15;
                max = 16;
                break;
            case 64:
                min = 10;
                max = 11;
                break;
            case 128:
                min = 7;
                max = 8;
                break;
            default:
                min = 0;
                max = 0;
                break;
            }
            // System.out.println(type + " expected " + min + ".." + max + " got " + avg);
            assertTrue("m " + m + " expected " + min + ".." + max + " got " + avg, min < avg && avg < max);
        }
    }

    private static double averageOverRange(int m, long maxSize, int testCount, boolean debug, double exponent) {
        double sum = 0;
        int count = 0;
        for (long size = 1; size <= 20; size++) {
            sum += test(m, size, testCount, debug, exponent);
            count++;
        }
        for (long size = 22; size <= 300; size += size / 5) {
            sum += test(m, size, testCount, debug, exponent);
            count++;
        }
        for (long size = 400; size <= maxSize; size *= 2) {
            sum += test(m, size, testCount, debug, exponent);
            count++;
        }
        return sum / count;
    }

    private static double test(int m, long size, int testCount, boolean debug, double exponent) {
        long x = 0;
        long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
        long ns = System.nanoTime();
        double sumSquareError = 0;
        double sum = 0;
        double sumFirst = 0;
        int repeat = 10;
        int runs = 2;
        for (int test = 0; test < testCount; test++) {
            HyperLogLog hll;
            if (m == 8) {
                hll = new HyperLogLogUsingLong(16, 0);
            } else {
                hll = new HyperLogLog(m, 0);
            }
            long baseX = x;
            for (int i = 0; i < size; i++) {
                hll.add(Hash.hash64(x));
                x++;
            }
            long e = hll.estimate();
            sum += e;
            min = Math.min(min, e);
            max = Math.max(max, e);
            long error = e - size;
            sumSquareError += error * error;
            sumFirst += e;
            for (int add = 0; add < repeat; add++) {
                long x2 = baseX;
                for (int i = 0; i < size; i++) {
                    hll.add(Hash.hash64(x2));
                    x2++;
                }
            }
            e = hll.estimate();
            sum += e;
            min = Math.min(min, e);
            max = Math.max(max, e);
            error = e - size;
            sumSquareError += error * error;
        }
        ns = System.nanoTime() - ns;
        long nsPerItem = ns / testCount / runs / (1 + repeat) / size;
        double stdDev = Math.sqrt(sumSquareError / testCount / runs);
        double relStdDevP = stdDev / size * 100;
        int biasFirstP = (int) (100 * (sumFirst / testCount / size) - 100);
        int biasP = (int) (100 * (sum / testCount / runs / size) - 100);
        if (debug) {
            System.out.println("m " + m + " size " + size + " relStdDev% " + (int) relStdDevP +
                    " range " + min + ".." + max +
                    " biasFirst% " + biasFirstP +
                    " bias% " + biasP +
                    " avg " + (sum / testCount / runs) +
                    " time " + nsPerItem);
        }
        // we try to reduce the relStdDevP, make sure there are no large values
        // (trying to reduce sumSquareError directly
        // would mean we care more about larger sets, but we don't)
        return Math.pow(relStdDevP, exponent);
    }

    static class HyperLogLogUsingLong extends HyperLogLog {

        private long value;

        public HyperLogLogUsingLong(int m, int maxSmallSetSize) {
            super(m, maxSmallSetSize);
        }

        public void add(long hash) {
            value = HyperLogLog3Linear64.add(value, hash);
        }

        public long estimate() {
            return HyperLogLog3Linear64.estimate(value);
        }

    }

}
