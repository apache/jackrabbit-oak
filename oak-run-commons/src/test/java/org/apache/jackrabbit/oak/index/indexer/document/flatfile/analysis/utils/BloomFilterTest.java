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

import org.junit.Test;

/**
 * Tests the Bloom filter implementation.
 */
public class BloomFilterTest {

    @Test
    public void calculateBits() {
        assertEquals(9_585_059, BloomFilter.calculateBits(1_000_000, 0.01));
        // test some extreme values
        assertEquals(0, BloomFilter.calculateBits(1, 1.0));
        assertEquals(1, BloomFilter.calculateBits(1, 0.99));
        assertEquals(0, BloomFilter.calculateBits(0, 0.0));
        assertTrue(BloomFilter.calculateBits(Integer.MAX_VALUE, 0.0) > Integer.MAX_VALUE);
    }

    @Test
    public void calculateK() {
        assertEquals(7, BloomFilter.calculateK(10));
        // test some extreme values
        assertEquals(1, BloomFilter.calculateK(1));
        assertEquals(1, BloomFilter.calculateK(0));
        assertEquals(69, BloomFilter.calculateK(100));
    }

    @Test
    public void calculateN() {
        assertEquals(11, BloomFilter.calculateN(100, 0.01));
        // test some extreme values
        assertEquals(1, BloomFilter.calculateN(1, 0.01));
        assertEquals(1, BloomFilter.calculateN(1, 0.1));
        assertEquals(0, BloomFilter.calculateN(0, 0.01));
    }

    @Test
    public void construct() {
        BloomFilter f = BloomFilter.construct(100, 0.01);
        assertEquals(960, f.getBitCount());
        assertEquals(7, f.getK());
        f = BloomFilter.construct(0, 0.01);
        assertEquals(0, f.getBitCount());
        assertEquals(1, f.getK());
    }

    @Test
    public void fpp() {
        for (double fpp = 0.001; fpp < 1; fpp *= 2) {
            int size = 500_000;
            BloomFilter f = BloomFilter.construct(size, fpp);
            for (int i = 0; i < size; i++) {
                f.add(Hash.hash64(i));
            }
            for (int i = 0; i < size; i++) {
                assertTrue(f.mayContain(Hash.hash64(i)));
            }
            int falsePositives = 0;
            for (int i = 0; i < size; i++) {
                if (f.mayContain(Hash.hash64(i + size))) {
                    falsePositives++;
                }
            }
            double realFpp = (double) falsePositives / size;
            // expected to be within 10%
            assertTrue("expected fpp: " + fpp + " got: " + realFpp, realFpp >= fpp * 0.9 && realFpp <= fpp * 1.1);
            long est = f.getEstimatedEntryCount();
            assertTrue("expected n: " + size + " got: " + est, size >= est * 0.9 && size <= est * 1.1);

            double fpp2 = BloomFilter.calculateFpp(size, f.getBitCount(), f.getK());
            assertTrue("expected fpp: " + fpp + " got: " + fpp2, fpp2 >= fpp * 0.9 && fpp2 <= fpp * 1.1);
        }
    }

    @Test
    public void estimatedEntryCount() {
        // let's assume we have a 1 KB Bloom filter with a false positive rate of 1%:
        double fpp = 0.01;
        long bits = 1000 * 8;
        long n = BloomFilter.calculateN(bits, fpp);
        BloomFilter bloom = BloomFilter.construct(n, fpp);
        // and a HyperLogLog of 1 KB:
        HyperLogLog hll = new HyperLogLog(1024, 0);
        // now we calculate estimations with both the Bloom filter and HyperLogLog
        for(int i = 0; i < 20_000; i++) {
            long x = Hash.hash64(i);
            bloom.add(x);
            hll.add(x);
            if (i > 0 && i % 1000 == 0) {
                long estBloom = bloom.getEstimatedEntryCount();
                long estHll = hll.estimate();
                int errBloom = (int) (Math.abs((double) i / estBloom - 1) * 10000);
                int errHll = (int) (Math.abs((double) i / estHll - 1) * 10000);
                if (i < 10_000) {
                    assertTrue(errBloom < 1000);
                } else {
                    assertEquals(Long.MAX_VALUE, estBloom);
                }
                assertTrue(errHll < 1000);
            }
        }
    }

}
