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
package org.apache.jackrabbit.oak.segment;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Random;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.segment.memory.MemoryStore;

public class SegmentIdTableBenchmark {

    private static SegmentIdFactory newSegmentIdMaker(final SegmentStore store) {
        return new SegmentIdFactory() {

            @Nonnull
            @Override
            public SegmentId newSegmentId(long msb, long lsb) {
                return new SegmentId(store, msb, lsb);
            }

        };
    }

    public static void main(String... args) throws IOException {
        test();
        test();
        test();
        test();
        test();
        test();
    }

    private static void test() throws IOException {
        long time;
        int repeat = 10000;
        int count = 10000;
        
        long[] array = new long[count];
        Random r = new Random(1);
        for (int i = 0; i < array.length; i++) {
            array[i] = r.nextLong();
        }
        
        time = System.currentTimeMillis();
        MemoryStore store = new MemoryStore();
        SegmentIdFactory maker = newSegmentIdMaker(store);
        final SegmentIdTable tbl = new SegmentIdTable();
        for (int i = 0; i < repeat; i++) {
            for (int j = 0; j < count; j++) {
                tbl.newSegmentId(j, array[j], maker);
            }
        }
        time = System.currentTimeMillis() - time;
        System.out.println("SegmentIdTable: " + time);
        
        time = System.currentTimeMillis();
        ConcurrentTable cm = new ConcurrentTable(store, 16 * 1024);
        for (int i = 0; i < repeat; i++) {
            for (int j = 0; j < count; j++) {
                cm.getSegmentId(j, array[j]);
            }
        }
        time = System.currentTimeMillis() - time;
        System.out.println("ConcurrentTable: " + time);
        
//        time = System.currentTimeMillis();
//        WeakHashMap<SegmentId, SegmentId> map = new WeakHashMap<SegmentId, SegmentId>(count);
//        for (int i = 0; i < repeat; i++) {
//            for (int j = 0; j < count; j++) {
//                SegmentId id = new SegmentId(tracker, j, j);
//                if (map.get(id) == null) {
//                    map.put(id, id);
//                }
//            }
//        }
//        time = System.currentTimeMillis() - time;
//        System.out.println("WeakHashMap: " + time);
    }
    
    static class ConcurrentTable {
        private final SegmentStore store;
        volatile WeakReference<SegmentId>[] map;
        @SuppressWarnings("unchecked")
        ConcurrentTable(SegmentStore store, int size) {
            this.store = store;
            map = (WeakReference<SegmentId>[]) new WeakReference[size];
        }
        SegmentId getSegmentId(long a, long b) {
            outer:
            while (true) {
                int increment = 1;
                WeakReference<SegmentId>[] m = map;
                int length = m.length;
                int index = (int) (b & (length - 1));
                while (true) {
                    WeakReference<SegmentId> ref = m[index];
                    if (ref == null) {
                        SegmentId id = new SegmentId(store, a, b);
                        ref = new WeakReference<SegmentId>(id);
                        m[index] = ref;
                        if (m != map) {
                            continue outer;
                        }
                        return id;
                    }
                    SegmentId id = ref.get();
                    if (id != null) {
                        if (id.getMostSignificantBits() == a && id.getLeastSignificantBits() == b) {
                            return id;
                        }
                    }
                    // guaranteed to work for power of 2 table sizes, see
                    // http://stackoverflow.com/questions/2348187/moving-from-linear-probing-to-quadratic-probing-hash-collisons
                    // http://stackoverflow.com/questions/12121217/limit-for-quadratic-probing-a-hash-table
                    index = (index + increment) & (length - 1);
                    increment++;
                    if (increment > 100) {
                        System.out.println("inc " + increment);
                    }
                }
            }
        }
    }

}
