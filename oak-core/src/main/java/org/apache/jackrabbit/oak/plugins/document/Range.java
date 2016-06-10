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
package org.apache.jackrabbit.oak.plugins.document;

import javax.annotation.Nonnull;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
* A revision range for {@link NodeDocument#PREVIOUS} documents.
*/
final class Range {

    final Revision high;
    final Revision low;
    final int height;

    /**
     * A range of revisions, with both inclusive bounds.
     *
     * @param high the high bound.
     * @param low the low bound.
     */
    Range(@Nonnull Revision high, @Nonnull Revision low, int height) {
        this.high = checkNotNull(high);
        this.low = checkNotNull(low);
        this.height = height;
        checkArgument(high.getClusterId() == low.getClusterId(),
                "Revisions from have the same clusterId");
        checkArgument(high.compareRevisionTime(low) >= 0,
                "High Revision must be later than low Revision, high=%s low=%s" ,high, low);
        checkArgument(height >= 0);
    }

    /**
     * Creates a {@code Range} from a revisioned document entry.
     *
     * @param rev the revision of the entry corresponding to the high bound
     *            of the range.
     * @param value the string representation of the lower bound with the height
     *              (e.g. r1-0-1/0).
     * @return the range.
     */
    @Nonnull
    static Range fromEntry(Revision rev, String value) {
        Revision low;
        int height;
        int idx = value.indexOf('/');
        if (idx == -1) {
            // backward compatibility for lower bound without height
            low = Revision.fromString(value);
            height = 0;
        } else {
            low = Revision.fromString(value.substring(0, idx));
            height = Integer.parseInt(value.substring(idx + 1));
        }
        return new Range(rev, low, height);
    }

    /**
     * @return the string representation of the lower bound, including the
     *         height (e.g. r1-0-1/0).
     */
    @Nonnull
    String getLowValue() {
        return low + "/" + height;
    }

    /**
     * Returns <code>true</code> if the given revision is within this range.
     *
     * @param r the revision to check.
     * @return <code>true</code> if within this range; <code>false</code>
     * otherwise.
     */
    boolean includes(@Nonnull Revision r) {
        return high.getClusterId() == r.getClusterId()
                && high.compareRevisionTime(r) >= 0
                && low.compareRevisionTime(r) <= 0;
    }

    /**
     * Returns the height of this range in the tree of previous documents. The
     * range of a leaf document has height zero.
     *
     * @return the height.
     */
    int getHeight() {
        return height;
    }

    @Override
    public String toString() {
        return high.toString() + " : " + low.toString() + "/" + height;
    }

    @Override
    public int hashCode() {
        return high.hashCode() ^ low.hashCode() ^ height;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Range) {
            Range other = (Range) obj;
            return high.equals(other.high)
                    && low.equals(other.low)
                    && height == other.height;
        }
        return false;
    }
}
