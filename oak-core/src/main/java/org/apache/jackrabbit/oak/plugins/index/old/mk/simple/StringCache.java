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
package org.apache.jackrabbit.oak.plugins.index.old.mk.simple;

import java.lang.ref.SoftReference;

import org.apache.jackrabbit.mk.util.IOUtils;

/**
 * A simple string cache.
 */
class StringCache {

    public static final boolean OBJECT_CACHE = getBooleanSetting("mk.objectCache", true);
    public static final int OBJECT_CACHE_SIZE = IOUtils.nextPowerOf2(getIntSetting("mk.objectCacheSize", 1024));

    private static SoftReference<String[]> softCache = new SoftReference<String[]>(null);

    private StringCache() {
        // utility class
    }

    private static boolean getBooleanSetting(String name, boolean defaultValue) {
        String s = getProperty(name);
        if (s != null) {
            try {
                return Boolean.valueOf(s).booleanValue();
            } catch (NumberFormatException e) {
                // ignore
            }
        }
        return defaultValue;
    }

    private static int getIntSetting(String name, int defaultValue) {
        String s = getProperty(name);
        if (s != null) {
            try {
                return Integer.decode(s).intValue();
            } catch (NumberFormatException e) {
                // ignore
            }
        }
        return defaultValue;
    }

    private static String getProperty(String name) {
        try {
            return System.getProperty(name);
        } catch (Exception e) {
            // SecurityException
            // applets may not do that - ignore
            return null;
        }
    }

    private static String[] getCache() {
        String[] cache;
        // softCache can be null due to a Tomcat problem
        // a workaround is disable the system property org.apache.
        // catalina.loader.WebappClassLoader.ENABLE_CLEAR_REFERENCES
        if (softCache != null) {
            cache = softCache.get();
            if (cache != null) {
                return cache;
            }
        }
        try {
            cache = new String[OBJECT_CACHE_SIZE];
        } catch (OutOfMemoryError e) {
            return null;
        }
        softCache = new SoftReference<String[]>(cache);
        return cache;
    }

    /**
     * Get the string from the cache if possible. If the string has not been
     * found, it is added to the cache. If there is such a string in the cache,
     * that one is returned.
     *
     * @param s the original string
     * @return a string with the same content, if possible from the cache
     */
    public static String cache(String s) {
        if (!OBJECT_CACHE) {
            return s;
        }
        if (s == null) {
            return s;
        } else if (s.length() == 0) {
            return "";
        }
        int hash = s.hashCode();
        String[] cache = getCache();
        if (cache != null) {
            int index = hash & (OBJECT_CACHE_SIZE - 1);
            String cached = cache[index];
            if (cached != null) {
                if (s.equals(cached)) {
                    return cached;
                }
            }
            cache[index] = s;
        }
        return s;
    }

    /**
     * Get a string from the cache, and if no such string has been found, create
     * a new one with only this content. This solves out of memory problems if
     * the string is a substring of another, large string. In Java, strings are
     * shared, which could lead to memory problems. This avoid such problems.
     *
     * @param s the string
     * @return a string that is guaranteed not be a substring of a large string
     */
    public static String fromCacheOrNew(String s) {
        if (!OBJECT_CACHE) {
            return s;
        }
        if (s == null) {
            return s;
        } else if (s.length() == 0) {
            return "";
        }
        int hash = s.hashCode();
        String[] cache = getCache();
        int index = hash & (OBJECT_CACHE_SIZE - 1);
        if (cache == null) {
            return s;
        }
        String cached = cache[index];
        if (cached != null) {
            if (s.equals(cached)) {
                return cached;
            }
        }
        // create a new object that is not shared
        // (to avoid out of memory if it is a substring of a big String)
        // NOPMD
        s = new String(s);
        cache[index] = s;
        return s;
    }

    /**
     * Clear the cache. This method is used for testing.
     */
    public static void clearCache() {
        softCache = new SoftReference<String[]>(null);
    }

}
