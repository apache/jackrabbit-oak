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
package org.apache.jackrabbit.oak.index.indexer.document;

public class LastModifiedRange {

    private final long lastModifiedLowerBound;
    private final long lastModifiedUpperBound;
    private final boolean isUpperBoundExclusive;

    public LastModifiedRange(long lastModifiedLowerBound, long lastModifiedUpperBound) {
        this(lastModifiedLowerBound, lastModifiedUpperBound, true);
    }

    public LastModifiedRange(long lastModifiedLowerBound, long lastModifiedUpperBound, boolean isUpperBoundExclusive) {
        if (lastModifiedUpperBound < lastModifiedLowerBound) {
            throw new IllegalArgumentException("Invalid range (" + lastModifiedLowerBound + ", " + lastModifiedUpperBound + ")");
        }
        this.lastModifiedLowerBound = lastModifiedLowerBound;
        this.lastModifiedUpperBound = lastModifiedUpperBound;
        this.isUpperBoundExclusive = isUpperBoundExclusive;
    }

    public long getLastModifiedLowerBound() {
        return lastModifiedLowerBound;
    }

    public long getLastModifiedUpperBound() {
        return lastModifiedUpperBound;
    }

    public boolean checkOverlap(LastModifiedRange range) {
        boolean rangeOnLeft, rangeOnRight;
        rangeOnRight = isUpperBoundExclusive ? lastModifiedUpperBound <= range.getLastModifiedLowerBound()
                : lastModifiedUpperBound < range.getLastModifiedLowerBound();
        rangeOnLeft = isUpperBoundExclusive ? range.getLastModifiedUpperBound() <= lastModifiedLowerBound :
                range.getLastModifiedUpperBound() < lastModifiedLowerBound;
        return !(rangeOnLeft || rangeOnRight);
    }

    @Override
    public String toString() {
        return "LastModifiedRange{" +
                "lastModifiedLowerBound=" + lastModifiedLowerBound +
                ", lastModifiedUpperBound=" + lastModifiedUpperBound +
                ", isUpperBoundExclusive=" + isUpperBoundExclusive +
                '}';
    }
}
