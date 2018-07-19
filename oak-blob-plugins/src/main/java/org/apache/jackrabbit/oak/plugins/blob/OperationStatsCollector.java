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

package org.apache.jackrabbit.oak.plugins.blob;

import java.util.concurrent.TimeUnit;

/**
 * Implementations of this can use to mark the relevant statistics.
 */
public interface OperationStatsCollector {
    OperationStatsCollector NOOP = new OperationStatsCollector() {
        @Override public void start() {
        }

        @Override public void finishFailure() {
        }

        @Override public void updateNumDeleted(long num) {
        }

        @Override public void updateNumCandidates(long num) {
        }

        @Override public void updateTotalSizeDeleted(long size) {
        }

        @Override public void updateDuration(long time, TimeUnit timeUnit) {
        }

        @Override public void updateMarkDuration(long time, TimeUnit timeUnit) {
        }

        @Override public void updateSweepDuration(long time, TimeUnit timeUnit) {
        }
    };

    /**
     * Increment the start counter
     */
    void start();

    /**
     * Increment the finishFailure counter
     */
    void finishFailure();

    /**
     * Update the number deleted
     * @param num
     */
    void updateNumDeleted(long num);

    /**
     * Update the number of candidates found
     * @param num
     */
    void updateNumCandidates(long num);

    /**
     * Update the size deleted
     * @param size
     */
    void updateTotalSizeDeleted(long size);

    /**
     * Increment the duration timer
     *
     * @param time time recorded for the operation
     * @param timeUnit unit of time
     */
    void updateDuration(long time, TimeUnit timeUnit);

    /**
     * Increment the mark phase duration timer
     *
     * @param time time recorded for the operation
     * @param timeUnit unit of time
     */
    void updateMarkDuration(long time, TimeUnit timeUnit);

    /**
     * Increment the sweep phase duration timer
     *
     * @param time time recorded for the operation
     * @param timeUnit unit of time
     */
    void updateSweepDuration(long time, TimeUnit timeUnit);

}
