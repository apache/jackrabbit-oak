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
        public void start() {
        }

        public void finishSuccess() {
        }

        public void finishFailure() {
        }

        public void updateDuration(long time, TimeUnit timeUnit) {
        }
    };

    /**
     * Increment the start counter
     */
    void start();

    /**
     * Increment the finishSuccess counter
     */
    void finishSuccess();

    /**
     * Increment the finishFailure counter
     */
    void finishFailure();

    /**
     * Increment the duration timer
     *
     * @param time time recorded for the operation
     * @param timeUnit unit of time
     */
    void updateDuration(long time, TimeUnit timeUnit);
}
