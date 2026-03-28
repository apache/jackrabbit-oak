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
package org.apache.jackrabbit.oak.cache;

/**
 * Selects the backing cache implementation used by {@link OakCacheBuilder}.
 *
 * <p>Pass to {@link OakCacheBuilder#implementation(CacheImplementation)} to pin a specific
 * cache to one backend, overriding the global {@code oak.cache.type} system property.
 * When no per-instance override is set, the builder resolves the implementation from
 * {@code System.getProperty("oak.cache.type", "lirs")}.</p>
 */
public enum CacheImplementation {

    /** LIRS (Low Inter-reference Recency Set) eviction, backed by {@code CacheLIRS}. */
    LIRS,

    /** W-TinyLFU eviction, backed by Caffeine. */
    CAFFEINE
}
