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

package org.apache.jackrabbit.oak.segment;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Listener for {@code SegmentNotFoundException}. Its purpose is to e.g. provide meaningful
 * logging information about these exceptions.
 */
public interface SegmentNotFoundExceptionListener {

    /**
     * Listener instance doing nothing on a {@code SegmentNotFoundException}
     */
    SegmentNotFoundExceptionListener IGNORE_SNFE = new SegmentNotFoundExceptionListener() {
        @Override
        public void notify(@Nonnull SegmentId id, @Nonnull SegmentNotFoundException snfe) { }
    };

    /**
     * Listener instance logging the {@code SegmentNotFoundException} at error level.
     */
    SegmentNotFoundExceptionListener LOG_SNFE = new SegmentNotFoundExceptionListener() {
        private final Logger log = LoggerFactory.getLogger(SegmentNotFoundExceptionListener.class);
        @Override
        public void notify(@Nonnull SegmentId id, @Nonnull SegmentNotFoundException snfe) {
            log.error("Segment not found: {}. {}", id, id.gcInfo(), snfe);
        }
    };
    
    /**
     * Notification about {@code SegmentNotFoundException} thrown by the underlying store
     * in a meaningful way. E.g. by logging it.
     * @param id the segment id of the offending {@code Segment}
     * @param snfe the raised exception
     */
    void notify(@Nonnull final SegmentId id, @Nonnull final SegmentNotFoundException snfe);
}
