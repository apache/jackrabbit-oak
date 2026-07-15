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

/**
 * A Bloom filter implementation.
 */
public class BloomFilter {

    private final int k;
    private final int arraySize;
    private final long[] data;

    private BloomFilter(long[] data, int k) {
        this.data = data;
        this.k = k;
        this.arraySize = data.length;
    }

    /**
     * Construct a Bloom filter. With a fpp of 0.01, the memory usage is roughly 1
     * byte per entry.
     *
     * @param n     the number of expected entries (eg. 1_000_000)
     * @param fpp   the false-positive probability (eg. 0.01 for a 1% false-positive
     *              probability)
     * @return the Bloom filter
     */
    public static BloomFilter construct(long n, double fpp) {
        if (n <= 0) {
            throw new IllegalArgumentException("n must be greater than 0");
        }
        if (fpp <= 0 || fpp >= 1) {
            throw new IllegalArgumentException("fpp must be between 0 and 1");
        }
        long m = calculateBits(n, fpp);
        int k = calculateK((double) m / n);
        return new BloomFilter(new long[(int) ((m + 63) / 64)], k);
    }

    // See also https://hur.st/bloomfilter

    /**
     * Calculate the best k parameter for a Bloom filter.
     * (k is the number of hash functions to use for one entry).
     *
     * @param bitsPerKey the number of bits per key (eg. 10)
     * @return the k parameter
     */
    public static int calculateK(double bitsPerKey) {
        return Math.max(1, (int) Math.round(bitsPerKey * Math.log(2)));
    }

    /**
     * Calculate the number of bits needed for a Bloom filter,
     * for a given false positive probability.
     *
     * @param n the number of entries (eg. 1_000_000)
     * @param fpp the false positive probability (eg. 0.01)
     * @return the bits needed
     */
    public static long calculateBits(long n, double fpp) {
        return (long) Math.ceil((n * Math.log(fpp)) / Math.log(1 / Math.pow(2, Math.log(2))));
    }

    /**
     * Calculate the maximum number of entries in the set, given the memory size
     * in bits, and a target false positive probability.
     *
     * @param bits the number of bits (eg. 10_000_000)
     * @param fpp  the false positive probability (eg. 0.01)
     * @return the maximum number of entries to be added
     */
    public static long calculateN(long bits, double fpp) {
        return (long) Math.ceil((bits * Math.log(Math.pow(0.5, Math.log(2))) / Math.log(fpp)));
    }

    /**
     * Calculate the false positive probability.
     *
     * @param n    the number of entries (eg. 1_000_000)
     * @param bits the number of bits (eg. 10_000_000)
     * @param k    the number of hash functions
     * @return the false positive probability (eg. 0.01)
     */
    public static double calculateFpp(long n, long bits, int k) {
        // p = pow(1 - exp(-k / (m / n)), k)
        return Math.pow(1 - Math.exp(-k / ((double) bits / n)), k);
    }

    /**
     * Add an entry.
     *
     * @param value the value to add
     */
    public void add(String value) {
        add(HashUtils.hash64(value));
    }

    /**
     * Add an entry.
     *
     * Note that the false positive rate will increase if the quality of the hash
     * value is low, eg. if only 32 bit hash values are used.
     * If needed, use HashUtils.hash64(value) as a supplemental hash.
     *
     * @param hash the hash value (need to be a high quality hash code, with all
     *             bits having high entropy)
     */
    public void add(long hash) {
        long a = (hash >>> 32) | (hash << 32);
        long b = hash;
        for (int i = 0; i < k; i++) {
            data[HashUtils.reduce((int) (a >>> 32), arraySize)] |= 1L << a;
            a += b;
        }
    }

    /**
     * Tests whether the entry might be in the set.
     *
     * @param value the value to check
     * @return true if the entry was added, or, with a certain false positive
     *         probability, even if it was not added
     */
    public boolean mayContain(String value) {
        return mayContain(HashUtils.hash64(value));
    }

    /**
     * Tests whether the entry might be in the set.
     *
     * @param hash the hash value (need to be a high quality hash code, with all
     *             bits having high entropy)
     * @return true if the entry was added, or, with a certain false positive
     *         probability, even if it was not added
     */
    public boolean mayContain(long hash) {
        long a = (hash >>> 32) | (hash << 32);
        long b = hash;
        for (int i = 0; i < k; i++) {
            if ((data[HashUtils.reduce((int) (a >>> 32), arraySize)] & 1L << a) == 0) {
                return false;
            }
            a += b;
        }
        return true;
    }

    /**
     * Get the number of bits needed for the array.
     *
     * @return the number of bits
     */
    public long getBitCount() {
        return data.length * 64L;
    }

    /**
     * Get the k parameter (the number of hash functions for an entry).
     *
     * @return the k parameter
     */
    public int getK() {
        return k;
    }

    /**
     * Get the estimated entry count (number of distinct items added). This
     * operation is relatively slow, as it loops over all the entries.
     *
     * @return the estimated entry count,
     *         or Long.MAX_VALUE if the number can not be estimated.
     */
    public long getEstimatedEntryCount() {
        long x = 0;
        for (long d : data) {
            x += Long.bitCount(d);
        }
        double m = getBitCount();
        return (long) (-(m / k) * Math.log(1 - (x / m)));
    }

} 