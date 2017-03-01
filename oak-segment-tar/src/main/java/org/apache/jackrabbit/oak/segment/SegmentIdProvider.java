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
 *
 */

package org.apache.jackrabbit.oak.segment;

import javax.annotation.Nonnull;

/**
 * Instances of this class provides {@link SegmentId} instances of a given
 * {@link SegmentStore} and creates new {@code SegmentId} instances on the fly
 * if required.
 */
public interface SegmentIdProvider {

    /**
     * @return The number of distinct segment ids this provider is tracking.
     */
    int getSegmentIdCount();

    /**
     * Provide a {@code SegmentId} represented by the given MSB/LSB pair.
     *
     * @param msb The most significant bits of the {@code SegmentId}.
     * @param lsb The least significant bits of the {@code SegmentId}.
     * @return A non-{@code null} instance of {@code SegmentId}.
     */
    @Nonnull
    SegmentId newSegmentId(long msb, long lsb);


    /**
     * Provide a {@code SegmentId} for a segment of type "bulk".
     *
     * @return A non-{@code null} instance of {@code SegmentId}.
     */
    @Nonnull
    SegmentId newDataSegmentId();


    /**
     * Provide a {@code SegmentId} for a segment of type "data".
     *
     * @return A non-{@code null} instance of {@code SegmentId}.
     */
    @Nonnull
    SegmentId newBulkSegmentId();
}
