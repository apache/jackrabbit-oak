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

import java.util.Objects;

public class LastModifiedRange {

    private final long lastModifiedFrom;
    private final long lastModifiedTo;

    public LastModifiedRange(long lastModifiedFrom, long lastModifiedTo) {
        if (lastModifiedTo < lastModifiedFrom) {
            throw new IllegalArgumentException("Invalid range (" + lastModifiedFrom + ", " + lastModifiedTo + ")");
        }
        this.lastModifiedFrom = lastModifiedFrom;
        this.lastModifiedTo = lastModifiedTo;
    }

    public long getLastModifiedFrom() {
        return lastModifiedFrom;
    }

    public long getLastModifiedTo() {
        return lastModifiedTo;
    }

    public boolean checkOverlap(LastModifiedRange range) {
        boolean rangeOnLeft, rangeOnRight;
        rangeOnRight = lastModifiedTo <= range.getLastModifiedFrom();
        rangeOnLeft = range.getLastModifiedTo() <= lastModifiedFrom;
        return !(rangeOnLeft || rangeOnRight);
    }

    public boolean contains(Long lastModifiedValue) {
        return lastModifiedValue >= lastModifiedFrom && lastModifiedValue < lastModifiedTo;
    }

    public LastModifiedRange mergeWith(LastModifiedRange range) {
        if (!checkOverlap(range)) {
            throw new IllegalArgumentException("Non overlapping ranges - " + this + " and " + range);
        }
        return new LastModifiedRange(Math.min(lastModifiedFrom, range.lastModifiedFrom), Math.max(lastModifiedTo, range.lastModifiedTo));
    }

    public boolean coversAllDocuments() {
        return lastModifiedFrom == 0 && lastModifiedTo == Long.MAX_VALUE;
    }

    @Override
    public String toString() {
        return "LastModifiedRange [" + lastModifiedFrom + ", " + lastModifiedTo + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LastModifiedRange that = (LastModifiedRange) o;
        return lastModifiedFrom == that.lastModifiedFrom && lastModifiedTo == that.lastModifiedTo;
    }

    @Override
    public int hashCode() {
        return Objects.hash(lastModifiedFrom, lastModifiedTo);
    }
}
