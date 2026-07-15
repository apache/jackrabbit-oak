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
package org.apache.jackrabbit.oak.cache;

import org.apache.jackrabbit.oak.cache.api.Weigher;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Determines the weight of object based on the memory taken by them. The memory estimates
 * are based on empirical data and not exact
 */
public class EmpiricalWeigher extends GuavaCompatibleEmpiricalWeigher
        implements Weigher<CacheValue, CacheValue> {

    static final Logger LOG = LoggerFactory.getLogger(EmpiricalWeigher.class);

    @Override
    public int weigh(@NotNull CacheValue key, @NotNull CacheValue value) {
        long size = 168;                // overhead for each cache entry
        size += key.getMemory();        // key
        size += value.getMemory();      // value
        if (size > Integer.MAX_VALUE) {
            LOG.debug("Calculated weight larger than Integer.MAX_VALUE: {}.", size);
            size = Integer.MAX_VALUE;
        }
        return (int) size;
    }
    
}

/**
 * Compatibility base class that keeps {@link EmpiricalWeigher} assignable to the
 * legacy Guava-shim {@link org.apache.jackrabbit.guava.common.cache.Weigher} type while the public API migrates to
 * {@link Weigher}.
 *
 * <p>TODO OAK-12162: remove this compatibility base in
 * OAK-12162 once downstream callers no longer require {@link org.apache.jackrabbit.guava.common.cache.Weigher}
 * assignability.</p>
 */
abstract class GuavaCompatibleEmpiricalWeigher implements org.apache.jackrabbit.guava.common.cache.Weigher<CacheValue, CacheValue> {
}
