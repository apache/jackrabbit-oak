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

/**
 * Interface to expose throttling metrics.
 *
 * Concrete implementations of this interface are required to provide implementations
 * of this class expose their throttling metrics
 */
public interface ThrottlingMetrics {

    int threshold();

    double currValue();

    long throttlingTime();


    ThrottlingMetrics DEFAULT_THROTTLING_METRIC = new ThrottlingMetrics() {

        @Override
        public int threshold() {
            return Integer.MAX_VALUE;
        }

        @Override
        public double currValue() {
            return 0;
        }

        @Override
        public long throttlingTime() {
            return 0;
        }
    };
}
