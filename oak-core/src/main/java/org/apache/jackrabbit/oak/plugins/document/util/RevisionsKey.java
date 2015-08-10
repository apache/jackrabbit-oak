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
package org.apache.jackrabbit.oak.plugins.document.util;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.StableRevisionComparator;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A cache key implementation which consists of two {@link Revision}s.
 */
public final class RevisionsKey implements CacheValue, Comparable<RevisionsKey> {

    private final Revision r1, r2;

    public RevisionsKey(Revision r1, Revision r2) {
        this.r1 = checkNotNull(r1);
        this.r2 = checkNotNull(r2);
    }

    @Override
    public int getMemory() {
        return 88;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof RevisionsKey)) {
            return false;
        }
        RevisionsKey other = (RevisionsKey) obj;
        return r1.equals(other.r1) && r2.equals(other.r2);
    }

    @Override
    public int hashCode() {
        return r1.hashCode() ^ r2.hashCode();
    }

    @Override
    public String toString() {
        return asString();
    }

    public String asString() {
        return r1 + "/" + r2;
    }

    public int compareTo(@Nonnull RevisionsKey k) {
        int c = StableRevisionComparator.INSTANCE.compare(r1, k.r1);
        if (c != 0) {
            return c;
        }
        return StableRevisionComparator.INSTANCE.compare(r2, k.r2);
    }

    public static RevisionsKey fromString(String s) {
        int idx = s.indexOf('/');
        if (idx == -1) {
            throw new IllegalArgumentException(s);
        }
        return new RevisionsKey(
                Revision.fromString(s.substring(0, idx)),
                Revision.fromString(s.substring(idx + 1)));
    }
}
