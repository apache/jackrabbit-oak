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
package org.apache.jackrabbit.oak.plugins.document.mongo;

import com.google.common.util.concurrent.AtomicDouble;
import org.apache.jackrabbit.oak.plugins.document.Throttler;
import org.jetbrains.annotations.NotNull;

import static com.google.common.math.DoubleMath.fuzzyCompare;
import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.plugins.document.Throttler.NO_THROTTLING;

/**
 * Factory to create Mongo Throttlers
 */
public final class MongoThrottlerFactory {

    private MongoThrottlerFactory() {
    }

    /**
     * Returns an instance of @{@link Throttler} which throttles the system exponentially based on remaining threshold defined.
     * @param threshold threshold for throttling
     * @param oplogWindow current oplog window for mongo
     * @param throttlingTime time duration for throttling
     * @return an exponential throttler to throttle system if required
     */
    public static Throttler exponentialThrottler(final int threshold, final AtomicDouble oplogWindow, final long throttlingTime) {
        requireNonNull(oplogWindow);
        return new ExponentialThrottler(threshold, oplogWindow, throttlingTime);
    }

    /**
     * A {@link Throttler} which doesn't do any throttling, no matter how much system is under load
     * @return No throttler
     */
    public static Throttler noThrottler() {
        return NO_THROTTLING;
    }

    private static class ExponentialThrottler implements Throttler {

        private final int threshold;
        @NotNull
        private final AtomicDouble oplogWindow;
        private final long throttlingTime;

        public ExponentialThrottler(final int threshold, final @NotNull AtomicDouble oplogWindow, final long throttlingTime) {
            this.threshold = threshold;
            this.oplogWindow = oplogWindow;
            this.throttlingTime = throttlingTime;
        }

        /**
         * The time duration (in Millis) for which we need to throttle the system.
         *
         * @return the throttling time duration (in Millis)
         */
        @Override
        public long throttlingTime() {
            final double threshold = this.threshold;
            final double oplogWindow = this.oplogWindow.doubleValue();
            long throttleTime = throttlingTime;

            if (fuzzyCompare(oplogWindow,threshold/8,  0.001) <= 0) {
                throttleTime = throttleTime * 8;
            } else if (fuzzyCompare(oplogWindow,threshold/4, 0.001) <= 0) {
                throttleTime = throttleTime * 4;
            } else if (fuzzyCompare(oplogWindow, threshold/2, 0.001) <= 0) {
                throttleTime = throttleTime * 2;
            } else if (fuzzyCompare(oplogWindow, threshold,0.001) <= 0) {
                throttleTime = throttlingTime;
            } else {
                throttleTime = 0;
            }
            return throttleTime;
        }
    }
}
