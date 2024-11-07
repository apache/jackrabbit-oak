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

package org.apache.jackrabbit.oak.segment;

import org.apache.jackrabbit.guava.common.collect.AbstractIterator;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.segment.data.SegmentData;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;

/**
 * Represents a list of segment IDs referenced from a segment.
 */
public interface SegmentReferences extends Iterable<SegmentId> {

    /** Builds a new instance of {@link SegmentReferences} from the provided {@link SegmentData}. */
    static @NotNull SegmentReferences fromSegmentData(@NotNull SegmentData data, @NotNull SegmentIdProvider idProvider) {
        final int referencedSegmentIdCount = data.getSegmentReferencesCount();

        Validate.checkState(referencedSegmentIdCount + 1 < 0xffff, "Segment cannot have more than 0xffff references");

        // We need to keep SegmentId references (as opposed to e.g. UUIDs)
        // here as frequently resolving the segment ids via the segment id
        // tables is prohibitively expensive.
        // These SegmentId references are not a problem wrt. heap usage as
        // their individual memoised references to their underlying segment
        // is managed via the SegmentCache. It is the size of that cache that
        // keeps overall heap usage by Segment instances bounded.
        // See OAK-6106.

        final SegmentId[] refIds = new SegmentId[referencedSegmentIdCount];

        return new SegmentReferences() {

            @Override
            public SegmentId getSegmentId(int reference) {
                checkArgument(reference <= referencedSegmentIdCount, "Segment reference out of bounds");
                SegmentId id = refIds[reference - 1];
                if (id == null) {
                    synchronized (refIds) {
                        id = refIds[reference - 1];
                        if (id == null) {
                            long msb = data.getSegmentReferenceMsb(reference - 1);
                            long lsb = data.getSegmentReferenceLsb(reference - 1);
                            id = idProvider.newSegmentId(msb, lsb);
                            refIds[reference - 1] = id;
                        }
                    }
                }
                return id;
            }

            @NotNull
            @Override
            public Iterator<SegmentId> iterator() {
                return new AbstractIterator<>() {
                    private int reference = 1;

                    @Override
                    protected SegmentId computeNext() {
                        if (reference <= referencedSegmentIdCount) {
                            return getSegmentId(reference++);
                        } else {
                            return endOfData();
                        }
                    }
                };
            }
        };
    }

    /**
     * Fetch the referenced segment ID associated to the provided reference.
     *
     * @param reference The reference to a segment ID.
     * @return The segment ID associated to the reference of {@code null} if
     * none is found.
     */
    SegmentId getSegmentId(int reference);

}
