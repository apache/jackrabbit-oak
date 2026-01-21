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
package org.apache.jackrabbit.oak.commons.internal.concurrent;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A lightweight striped-lock implementation
 */
public class StripedLocks {

    private final Lock[] stripes;
    private final int len;

    public StripedLocks(final int stripesCount) {
        if (stripesCount <= 0) {
            throw new IllegalArgumentException("stripesCount must be > 0");
        }
        this.stripes = new Lock[stripesCount];
        for (int i = 0; i < stripesCount; i++) {
            this.stripes[i] = new ReentrantLock();
        }
        this.len = stripesCount;
    }

    public Lock get(Object key) {
        int h = (key == null) ? 0 : key.hashCode();
        // Spread bits to reduce collisions, using
        // https://stackoverflow.com/questions/664014/what-integer-hash-function-are-good-that-accepts-an-integer-hash-key/12996028#12996028
        h *= 0x45d9f3b;
        // http://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
        int index = (int) (((h & 0xffffffffL) * len) >>> 32);
        return stripes[index];
    }

    public int stripesCount() {
        return len;
    }
}
