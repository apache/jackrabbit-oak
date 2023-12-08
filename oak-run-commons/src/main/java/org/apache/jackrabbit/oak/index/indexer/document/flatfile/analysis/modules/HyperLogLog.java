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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules;

import java.util.HashSet;

public class HyperLogLog {
    
    private static final int MAX_SMALL_SET = 1024;

    private final int m;
    private byte[] counters;
    private final double am;
    private HashSet<Long> smallDistinctSet = new HashSet<>();

    public HyperLogLog(int m) {
        if (m < 16) {
            throw new IllegalArgumentException("Must be >= 16, is " + m);
        }
        if (Integer.bitCount(m) != 1) {
            throw new IllegalArgumentException("Must be a power of 2, is " + m);
        }
        this.m = m;
        switch (m) {
        case 32:
            am = 0.697;
            break;
        case 64:
            am = 0.709;
            break;
        default:
            am = 0.7213 / (1.0 + 1.079 / m);
        }
        this.counters = new byte[m];
    }

    public void add(long hash) {
        if (smallDistinctSet != null) {
            smallDistinctSet.add(hash);
            if (smallDistinctSet.size() > MAX_SMALL_SET) {
                smallDistinctSet = null;
            }
        }
        int i = (int) (hash & (m - 1));
        counters[i] = (byte) Math.max(counters[i], 1 + Long.numberOfLeadingZeros(hash));
    }

    public long estimate() {
        if (smallDistinctSet != null) {
            return smallDistinctSet.size();
        }
        double sum = 0;
        int countZero = 0;
        for(int c : counters) {
            countZero += c == 0 ? 1 : 0;
            sum += 1. / (1L << (c & 0xff));
        }
        long est = (long) (1. / sum * am * m * m);
        if (est <= 5 * m && countZero > 0) {
            // linear counting
            est = (long) (m * Math.log((double) m / countZero));
        }
        return Math.max(1, est);
    }

}
