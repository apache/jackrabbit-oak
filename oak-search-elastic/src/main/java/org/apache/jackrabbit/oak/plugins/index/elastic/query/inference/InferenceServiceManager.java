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

import java.util.concurrent.ConcurrentHashMap;

public class InferenceServiceManager {

    private static final String INFERENCE_CACHE_SIZE = "oak.inference.cache.size";

    private static final ConcurrentHashMap<String, InferenceService> SERVICES = new ConcurrentHashMap<>();

    public static InferenceService getInstance(String url) {
        return SERVICES.computeIfAbsent(url, key -> new InferenceService(key, Integer.getInteger(INFERENCE_CACHE_SIZE, 100)));
    }

}
