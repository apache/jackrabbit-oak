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

package org.apache.jackrabbit.oak.stats;

import org.apache.jackrabbit.api.stats.RepositoryStatistics;
import org.apache.jackrabbit.api.stats.TimeSeries;

final class NoopRepositoryStatistics implements RepositoryStatistics{
    public static final RepositoryStatistics INSTANCE = new NoopRepositoryStatistics();

    private NoopRepositoryStatistics(){

    }

    @Override
    public TimeSeries getTimeSeries(Type type) {
        return NoopTimeSeries.INSTANCE;
    }

    @Override
    public TimeSeries getTimeSeries(String s, boolean b) {
        return NoopTimeSeries.INSTANCE;
    }

    private static final class NoopTimeSeries implements TimeSeries {
        private static final long[] EMPTY_ARR = new long[0];

        static final TimeSeries INSTANCE = new NoopTimeSeries();

        @Override
        public long[] getValuePerSecond() {
            return EMPTY_ARR;
        }

        @Override
        public long[] getValuePerMinute() {
            return EMPTY_ARR;
        }

        @Override
        public long[] getValuePerHour() {
            return EMPTY_ARR;
        }

        @Override
        public long[] getValuePerWeek() {
            return EMPTY_ARR;
        }

        @Override
        public long getMissingValue() {
            return 0;
        }
    }
}
