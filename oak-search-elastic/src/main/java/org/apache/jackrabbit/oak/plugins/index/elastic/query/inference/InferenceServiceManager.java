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

import org.apache.jackrabbit.oak.commons.properties.SystemPropertySupplier;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class InferenceServiceManager {

    private static final Logger LOGGER = Logger.getLogger(InferenceServiceManager.class.getName());

    private static final String MAX_CACHED_SERVICES_PROPERTY = "oak.inference.max.cached.services";
    private static final int MAX_CACHED_SERVICES = SystemPropertySupplier.create(MAX_CACHED_SERVICES_PROPERTY, 10).get();

    private static final String CACHE_SIZE_PROPERTY = "oak.inference.cache.size";
    private static final int CACHE_SIZE = SystemPropertySupplier.create(CACHE_SIZE_PROPERTY, 100).get();

    private static final ConcurrentHashMap<String, InferenceService> SERVICES = new ConcurrentHashMap<>();

    public static InferenceService getInstance(@NotNull String url, String model) {
        String k = model == null ? url : url + "|" + model;

        if (SERVICES.size() >= MAX_CACHED_SERVICES) {
            LOGGER.warning("InferenceServiceManager maximum cached services reached: " + MAX_CACHED_SERVICES);
            LOGGER.warning("Returning a new InferenceService instance with no cache");
            return new InferenceService(url, 0);
        }

        return SERVICES.computeIfAbsent(k, key -> new InferenceService(url, CACHE_SIZE));
    }

}
