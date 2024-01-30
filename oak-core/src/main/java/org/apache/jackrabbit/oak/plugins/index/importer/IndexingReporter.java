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
package org.apache.jackrabbit.oak.plugins.index.importer;

import java.util.List;

public interface IndexingReporter {
    IndexingReporter NOOP = new IndexingReporter() {
        @Override
        public void setIndexNames(List<String> indexes) {
        }

        @Override
        public void addConfig(String key, Object value) {
        }

        @Override
        public void addTiming(String stage, String time) {
        }

        @Override
        public void addMetric(String name, long value) {
        }

        /**
         * Similar to {@link #addMetric(String, long)} but size should be logged in a human-friendly format, that is,
         * something like
         *  <pre>
         *    foo.bar    123456789 (123 MiB)
         *  </<pre>
         */
        @Override
        public void addMetricByteSize(String name, long value) {
        }

        @Override
        public String generateReport() {
            return "";
        }
    };

    void setIndexNames(List<String> indexes);

    void addConfig(String key, Object value);

    void addTiming(String stage, String time);

    void addMetric(String name, long value);

    void addMetricByteSize(String name, long value);

    String generateReport();
}
