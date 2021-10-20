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
package org.apache.jackrabbit.oak.plugins.document.util;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Utility class to silence log output based on a specific key.
 * <p>
 * The key, together with the timeout, will be put into a small LRU cache
 * and later used to determine silencing or not.
 */
public class LogSilencer {

    public static final String SILENCING_POSTFIX = " (similar log silenced for a while)";
    private static final long DEFAULT_SILENCE_MILLIS = Duration.ofMinutes(15).toMillis();
    private static final int DEFAULT_CACHE_SIZE = 64;
    private final int cacheSize;
    private final long silenceMillis;

    @SuppressWarnings("serial")
    private final Map<String, Long> silences = Collections.synchronizedMap(
            new LinkedHashMap<String, Long>() {

        protected final boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
            return size() > cacheSize;
        }
    });

    public LogSilencer() {
        this(DEFAULT_SILENCE_MILLIS, DEFAULT_CACHE_SIZE);
    }

    /**
     * Create a new LogSilencer
     * @param silenceMillis milliseconds after which the silences herein should time out.
     * If the value is <0 it means no timeout, if it is ==0 it is silenced only
     * for the very same millisecond, and >0 the silence is active for that specified
     * amount of time.
     * @param cacheSize the size of the cache held by the LogSilencer. The cache is
     * used to store the keys and timeout values for each of them.
     */
    public LogSilencer(long silenceMillis, int cacheSize) {
        if (cacheSize <= 0) {
            throw new IllegalArgumentException("cacheSize must be > 0, is: " + cacheSize);
        }
        this.silenceMillis = silenceMillis;
        this.cacheSize = cacheSize;
    }

    /**
     * Determine whether based on a provided key logging about that key should be silenced.
     * <p>
     * The actual scope and context of the provided key is entirely up to the caller
     * and not relevant for the LogSilencer. All the LogSilencer is trying to do
     * is to provide a mechanism to "silence based on a key"
     * @param key a key within the caller's context which identified some log
     * @return whether a silence for the provided key is in place or not.
     * The silence times out after a certain, configurable amount of time.
     */
    public final boolean silence(String key) {
        // this is only "approximately now", since we only get the timestamp
        // at the beginning of this method and don't repeat that further down.
        // hence the time used within this method we approximate away - hence 'approxNow'.
        final Long approxNow = System.currentTimeMillis();
        final Long prevOrNull = silences.putIfAbsent(key, approxNow);
        if (prevOrNull == null) {
            // then we did not have this key in the silences map
            // which means no silence yet
            return false;
        } else {
            // otherwise we did have this key already in the silence,
            // let's compare the time for silence-timeout
            if (silenceMillis < 0) {
                // that means there is no timeout, ever.
                // so we silence
                return true;
            } else if (approxNow <= prevOrNull + silenceMillis) {
                // note that 'silenceMillis' can be 0
                // in which case the silence is only applied in the very same millisecond.
                return true;
            }
            // otherwise the silence expired, so we have to re-add it.
            // concurrency note : if another thread comes in between,
            // it should not do any harm, since both will race to
            // put the 'approxNow' into the cache - and more or less
            // both will have a similar value for 'approxNow'
            silences.put(key, approxNow);
            return false;
        }
    }
}
