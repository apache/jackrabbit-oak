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
package org.apache.jackrabbit.oak.security.authorization.permission;

import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;

final class CacheStrategyImpl implements CacheStrategy {

    static final String EAGER_CACHE_SIZE_PARAM = "eagerCacheSize";
    private static final long DEFAULT_SIZE = 250;

    static final String EAGER_CACHE_MAXPATHS_PARAM = "eagerCacheMaxPaths";
    private static final long DEFAULT_MAX_PATHS = 10;

    private final long maxSize;
    private final long maxPaths;
    private final long threshold;

    private boolean fullyLoaded = true;

    CacheStrategyImpl(ConfigurationParameters options, boolean isRefresh) {
        maxSize = options.getConfigValue(EAGER_CACHE_SIZE_PARAM, DEFAULT_SIZE);
        maxPaths = options.getConfigValue(EAGER_CACHE_MAXPATHS_PARAM, DEFAULT_MAX_PATHS);
        // define upper boundary for reading all permission entries:
        // - avoid repeatedly reading the entries for sessions that force a refresh by either writing (Session.save)
        //   or explicit refresh (Session.refresh). see also OAK-9203
        // - read-only sessions though that read permission entries only once, benefit from eagerly
        //   reading permission entries for principals with few access controlled paths (original behavior)
        if (isRefresh) {
            threshold = maxSize;
        } else {
            threshold = Long.MAX_VALUE;
        }
    }


    @Override
    public long maxSize() {
        return maxSize;
    }

    @Override
    public boolean loadFully(long numEntriesSize, long cnt) {
        if (numEntriesSize <= maxPaths && cnt < threshold) {
            return true;
        } else {
            fullyLoaded = false;
            return false;
        }
    }

    @Override
    public boolean usePathEntryMap(long cnt) {
        if (cnt > 0) {
            return fullyLoaded || cnt < maxSize;
        } else {
            return false;
        }
    }
}
