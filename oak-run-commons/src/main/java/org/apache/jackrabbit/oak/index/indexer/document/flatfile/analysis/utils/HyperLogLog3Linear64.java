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
 * Cardinality estimation with the HyperLogLog algorithm, using the tail cut
 * mechanism. Tail cut is described in the paper "Better with Fewer Bits -
 * Improving the Performance of Cardinality Estimation of Large Data Streams"
 * from Qingjun Xiao, You Zhou, Shigang Chen, in
 * http://cse.seu.edu.cn/PersonalPage/csqjxiao/csqjxiao_files/papers/INFOCOM17.pdf
 *
 * It uses linear counting for 60 bits, until 36 bits are set, then switches to
 * HyperLogLog. There, it uses 20 counters of 3 bits each (re-using the linear
 * counting data), and 4 bits for a base counter, which is increased if all
 * counters are larger than zero.
 *
 * It is a little bit "order-dependent", that is, adding the same entry multiple
 * times can change the internal state. However, unlike in HyperBitBit, here the
 * effect is very small.
 */
public class HyperLogLog3Linear64 {

    private HyperLogLog3Linear64() {
        // utility class
    }

    public static long add(long data, long hash) {
        int base = (int) (data & 0xf);
        if (base == 0) {
            int index = (int) (((hash & 0xffffffffL) * 20) >>> 32);
            int z = Long.numberOfLeadingZeros(hash);
            index += 20 * Math.min(z, 2);
            data |= 1L << (4 + index);
            if (Long.bitCount(data >>> 4) < 36) {
                return data;
            }
            base = 1;
            long newData = base;
            for (int i = 0; i < 20; i++) {
                if ((data >>> (44 + i) & 1) == 1) {
                    newData += 2L << (4 + 3 * i);
                } else if ((data >>> (24 + i) & 1) == 1) {
                    newData += 1L << (4 + 3 * i);
                }
            }
            data = newData;
        }
        int z = Long.numberOfLeadingZeros(hash) - (base - 1);
        if (z > 0) {
            int i = (int) (((hash & 0xffffffffL) * 20) >>> 32);
            int shift = 4 + 3 * i;
            long old = (data >>> shift) & 0x7;
            long m = Math.min(0x7, Math.max(z,  old));
            data = (data & ~(0x7L << shift)) | (m << shift);
            // base shift
            long s = data | (data >>> 1) | (data >>> 2);
            if ((s & 0x2492492492492490L) == 0x2492492492492490L) {
                if (base < 0xf) {
                    data -= 0x2492492492492490L;
                    data += 1;
                }
            }
        }
        return data;
    }

    public static long estimate(long data) {
        long base = data & 0xf;
        if (base == 0) {
            int x = Long.bitCount(data);
            int b = 53;
            return (long) (-b * Math.log(1. - (double) x / b));
        }
        double sum = 0;
        long x = data >>> 4;
        for (int i = 1; i < 20; i++) {
            long n = x & 0x7;
            x >>>= 3;
            sum += 1. / (1L << (base + n));
        }
        if (sum == 0) {
            sum = 1;
        }
        return (long) (20 * 20 * 0.63375 / sum);
    }

}
