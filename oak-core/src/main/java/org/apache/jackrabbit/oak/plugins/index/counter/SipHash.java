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
package org.apache.jackrabbit.oak.plugins.index.counter;

/**
 * An implementation of the SipHash-2-2 function, to prevent hash flooding.
 */
public class SipHash {
    
    private final long v0, v1, v2, v3;

    public SipHash(long seed) {
        long k0 = seed;
        long k1 = Long.rotateLeft(seed, 32);
        v0 = k0 ^ 0x736f6d6570736575L;
        v1 = k1 ^ 0x646f72616e646f6dL;
        v2 = k0 ^ 0x6c7967656e657261L;
        v3 = k1 ^ 0x7465646279746573L;
    }

    public SipHash(SipHash parent, long m) {
        long v0 = parent.v0;
        long v1 = parent.v1;
        long v2 = parent.v2;
        long v3 = parent.v3;
        int repeat = 2;
        for (int i = 0; i < repeat; i++) {
            v0 += v1;
            v2 += v3;
            v1 = Long.rotateLeft(v1, 13);
            v3 = Long.rotateLeft(v3, 16);
            v1 ^= v0;
            v3 ^= v2;
            v0 = Long.rotateLeft(v0, 32);
            v2 += v1;
            v0 += v3;
            v1 = Long.rotateLeft(v1, 17);
            v3 = Long.rotateLeft(v3, 21);
            v1 ^= v2;
            v3 ^= v0;
            v2 = Long.rotateLeft(v2, 32);
        }
        v0 ^= m;      
        this.v0 = v0;
        this.v1 = v1;
        this.v2 = v2;
        this.v3= v3;
    }
    
    @Override
    public int hashCode() {
        long x = v0 ^ v1 ^ v2 ^ v3;
        return (int) (x ^ (x >>> 16));
    }

}
