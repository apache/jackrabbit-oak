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
 * The reason an entry was removed from the cache.
 *
 * <p>Passed to {@link OakRemovalListener#onRemoval(Object, Object, OakRemovalCause)}
 * when an entry is evicted or invalidated. Covers the common subset of removal
 * causes across CacheLIRS and Caffeine without exposing either.</p>
 */
public enum OakRemovalCause {

    /** The entry was manually removed via {@code invalidate}. */
    EXPLICIT,

    /** The entry was replaced by a new value for the same key. */
    REPLACED,

    /** The entry was evicted due to a size or weight constraint. */
    SIZE,

    /** The entry expired. */
    EXPIRED,

    /** The entry was collected by the garbage collector (weak/soft reference). */
    COLLECTED
}
