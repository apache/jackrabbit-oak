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
package org.apache.jackrabbit.oak.plugins.index.elastic.query.inference;

import java.util.List;

public interface InferenceService {
    String DEFAULT_METRICS_LOGGING_INTERVAL_KEY = "elastic.query.inference.LoggingInterval";
    long DEFAULT_METRICS_LOGGING_INTERVAL = Long.getLong(DEFAULT_METRICS_LOGGING_INTERVAL_KEY, 10L * 60L * 1000L); // 10 minutes

    List<Float> embeddings(String text);

    List<Float> embeddings(String text, long timeoutMillis);
}
