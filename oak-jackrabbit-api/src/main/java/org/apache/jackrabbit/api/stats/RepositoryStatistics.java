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
package org.apache.jackrabbit.api.stats;


/**
 * Statistics on core repository operations
 * 
 */
public interface RepositoryStatistics {

    /**
     * The values of this enum determine the type of the time
     * series returned by {@link #getTimeSeries(Type)}
     * and link {@link #getTimeSeries(String, boolean)}.
     */
    enum Type {
        BUNDLE_READ_COUNTER(true),
        BUNDLE_WRITE_COUNTER(true),
        BUNDLE_WRITE_DURATION(true),
        BUNDLE_WRITE_AVERAGE(false),
        BUNDLE_CACHE_ACCESS_COUNTER(true),
        BUNDLE_CACHE_SIZE_COUNTER(false),
        BUNDLE_CACHE_MISS_COUNTER(true),
        BUNDLE_CACHE_MISS_DURATION(true),
        BUNDLE_CACHE_MISS_AVERAGE(false),
        BUNDLE_COUNTER(true),
        BUNDLE_WS_SIZE_COUNTER(true),

        /**
         * Number of read accesses through any session.
         */
        SESSION_READ_COUNTER(true),

        /**
         * Total time spent reading from sessions in nano seconds.
         */
        SESSION_READ_DURATION(true),

        /**
         * Average time spent reading from sessions in nano seconds.
         * This is the sum of all read durations divided by the number
         * of reads in the respective time period.
         */
        SESSION_READ_AVERAGE(false),

        /**
         * Number of write accesses through any session.
         */
        SESSION_WRITE_COUNTER(true),

        /**
         * Total time spent writing to sessions in nano seconds.
         */
        SESSION_WRITE_DURATION(true),

        /**
         * Average time spent writing to sessions in nano seconds.
         * This is the sum of all write durations divided by the number
         * of writes in the respective time period.
         */
        SESSION_WRITE_AVERAGE(false),

        /**
         * Number of calls sessions that have been logged in.
         */
        SESSION_LOGIN_COUNTER(true),

        /**
         * Number of currently logged in sessions.
         */
        SESSION_COUNT(false),

        /**
         * Number of queries executed.
         */
        QUERY_COUNT(true),

        /**
         * Total time spent evaluating queries in milli seconds.
         */
        QUERY_DURATION(true),

        /**
         * Average time spent evaluating queries in milli seconds.
         * This is the sum of all query durations divided by the number
         * of queries in the respective time period.
         */
        QUERY_AVERAGE(true),

        /**
         * Total number of observation {@code Event} instances delivered
         * to all observation listeners.
         */
        OBSERVATION_EVENT_COUNTER(true),

        /**
         * Total time spent processing observation events by all observation
         * listeners in nano seconds.
         */
        OBSERVATION_EVENT_DURATION(true),

        /**
         * Average time spent processing observation events by all observation
         * listeners in nano seconds.
         * This is the sum of all observation durations divided by the number
         * of observation events in the respective time period.
         */
        OBSERVATION_EVENT_AVERAGE(true);

        private final boolean resetValueEachSecond;

        Type(final boolean resetValueEachSecond) {
            this.resetValueEachSecond = resetValueEachSecond;
        }

        public static Type getType(String type) {
            Type realType = null;
            try {
                realType = valueOf(type);
            } catch (IllegalArgumentException ignore) {}
            return realType;
        }

        public boolean isResetValueEachSecond() {
            return resetValueEachSecond;
        }
    }

    TimeSeries getTimeSeries(Type type);

    TimeSeries getTimeSeries(String type, boolean resetValueEachSecond);
}
