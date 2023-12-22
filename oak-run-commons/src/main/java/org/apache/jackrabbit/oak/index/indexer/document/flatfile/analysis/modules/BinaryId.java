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

import java.util.Objects;

import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.utils.Hash;

/**
 * A binary id.
 *
 * Internally, this class uses 3 longs (64-bit) values.
 */
public class BinaryId {

    private final long v0;
    private final long v1;
    private final long v2;
    private final long length;

    public BinaryId(String identifier) {
        // we support identifiers of the following format:
        // <hex digits or '-'>#<length>
        // the '-' is ignored
        int hashIndex = identifier.lastIndexOf('#');
        String length = identifier.substring(hashIndex + 1);
        this.length = Long.parseLong(length);
        StringBuilder buff = new StringBuilder(48);
        for (int i = 0; i < hashIndex; i++) {
            char c = identifier.charAt(i);
            if (c != '-') {
                buff.append(c);
            }
        }
        // we need to hash again because some of the bits are fixed
        // in case of UUIDs: always a "4" here: xxxxxxxx-xxxx-4xxx
        // (the hash64 is a reversible mapping, so there is no risk of conflicts)
        this.v0 = Hash.hash64(Long.parseUnsignedLong(buff.substring(0, 16), 16));
        this.v1 = Hash.hash64(Long.parseUnsignedLong(buff.substring(16, 32), 16));
        this.v2 = Hash.hash64(Long.parseUnsignedLong(buff.substring(32, Math.min(48, buff.length())), 16));
    }

    @Override
    public int hashCode() {
        return Objects.hash(length, v0, v1, v2);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        BinaryId other = (BinaryId) obj;
        return length == other.length && v0 == other.v0 && v1 == other.v1 && v2 == other.v2;
    }

    /**
     * Get a 64-bit hash value. The probability of collisions is about: 50% for a
     * set of 5 billion entries, 1% for 600 million, 0.0001% for 6 million entries.
     *
     * @return a 64-bit hash value
     */
    public long getLongHash() {
        return v0 ^ v1 ^ v2 ^ length;
    }

    public long getLength() {
        return length;
    }

}