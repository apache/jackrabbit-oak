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
package org.apache.jackrabbit.oak.plugins.mongomk;

import javax.annotation.Nonnull;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
* A revision range for {@link NodeDocument#PREVIOUS} documents.
*/
final class Range {

    final Revision high;
    final Revision low;

    /**
     * A range of revisions, with both inclusive bounds.
     *
     * @param high the high bound.
     * @param low the low bound.
     */
    Range(@Nonnull Revision high, @Nonnull Revision low) {
        this.high = checkNotNull(high);
        this.low = checkNotNull(low);
        checkArgument(high.getClusterId() == low.getClusterId(),
                "Revisions from have the same clusterId");
        checkArgument(high.compareRevisionTime(low) >= 0,
                "High Revision must be later than low Revision, high=" + high + " low=" + low);
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

    @Override
    public String toString() {
        return low.toString();
    }
}
