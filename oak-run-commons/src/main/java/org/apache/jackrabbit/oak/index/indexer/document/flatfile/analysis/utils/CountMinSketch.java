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

/**
 * A count-min sketch implementation.
 */
public class CountMinSketch {

    // number of hash functions (depth, number of rows)
    private final int k;

    // number of buckets (width, number of columns)
    private final int m;

    // how many bits to shift after each hash function
    // (to reduce the number of hash functions required)
    private final int shift;

    private final long[][] data;

    /**
     * Create a new instance.
     *
     * @param k the number of hash functions
     * @param m the number of buckets per hash function (must be a power of 2)
     */
    public CountMinSketch(int k, int m) {
        if (Integer.bitCount(m) != 1) {
            throw new IllegalArgumentException("Must be a power of 2: " + m);
        }
        if ((k & 1) == 0) {
            throw new IllegalArgumentException("Must be odd: " + k);
        }
        this.shift = Integer.bitCount(m - 1);
        if (shift * k > 64) {
            throw new IllegalArgumentException("Too many hash functions or buckets: " + k + " / " + m);
        }
        this.m = m;
        this.k = k;
        data = new long[k][m];
    }

    /**
     * Add an entry.
     *
     * @param hash the hash
     * @return the new estimation
     */
    public long addAndEstimate(long hash) {
        long min = Long.MAX_VALUE;
        for (int i = 0; i < k; i++) {
            long x = ++data[i][(int) (hash & (m - 1))];
            min = Math.min(min, x);
            hash >>>= shift;
        }
        return min;
    }

    public void add(long hash) {
        for (int i = 0; i < k; i++) {
            data[i][(int) (hash & (m - 1))]++;
            hash >>>= shift;
        }
    }

    public long estimate(long hash) {
        long min = Long.MAX_VALUE;
        for (int i = 0; i < k; i++) {
            long x = data[i][(int) (hash & (m - 1))];
            min = Math.min(min, x);
            hash >>>= shift;
        }
        return min;
    }

}
