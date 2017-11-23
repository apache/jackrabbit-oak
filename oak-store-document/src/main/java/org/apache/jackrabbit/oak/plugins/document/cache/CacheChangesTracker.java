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
package org.apache.jackrabbit.oak.plugins.document.cache;

import com.google.common.base.Predicate;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CacheChangesTracker {

    private static final Logger LOG = LoggerFactory.getLogger(CacheChangesTracker.class);

    static final int ENTRIES_SCOPED = 1000;

    static final int ENTRIES_OPEN = 10000;

    private final List<CacheChangesTracker> changeTrackers;

    private final Predicate<String> keyFilter;

    private final LazyBloomFilter lazyBloomFilter;

    CacheChangesTracker(Predicate<String> keyFilter, List<CacheChangesTracker> changeTrackers, int bloomFilterSize) {
        this.changeTrackers = changeTrackers;
        this.keyFilter = keyFilter;
        this.lazyBloomFilter = new LazyBloomFilter(bloomFilterSize);
        changeTrackers.add(this);
    }

    public void putDocument(String key) {
        if (keyFilter.apply(key)) {
            lazyBloomFilter.put(key);
        }
    }

    public void invalidateDocument(String key) {
        if (keyFilter.apply(key)) {
            lazyBloomFilter.put(key);
        }
    }

    public boolean mightBeenAffected(String key) {
        return keyFilter.apply(key) && lazyBloomFilter.mightContain(key);
    }

    public void close() {
        changeTrackers.remove(this);

        if (LOG.isDebugEnabled()) {
            if (lazyBloomFilter.filter == null) {
                LOG.debug("Disposing CacheChangesTracker for {}, no filter was needed", keyFilter);
            } else {
                LOG.debug("Disposing CacheChangesTracker for {}, filter fpp was: {}", keyFilter, lazyBloomFilter.filter.expectedFpp());
            }
        }
    }

    public static class LazyBloomFilter {

        private static final double FPP = 0.01d;

        private final int entries;

        private volatile BloomFilter<String> filter;

        public LazyBloomFilter(int entries) {
            this.entries = entries;
        }

        public synchronized void put(String entry) {
            getFilter().put(entry);
        }

        public boolean mightContain(String entry) {
            if (filter == null) {
                return false;
            } else {
                synchronized (this) {
                    return filter.mightContain(entry);
                }
            }
        }

        private BloomFilter<String> getFilter() {
            if (filter == null) {
                filter = BloomFilter.create(new Funnel<String>() {
                    private static final long serialVersionUID = -7114267990225941161L;

                    @Override
                    public void funnel(String from, PrimitiveSink into) {
                        into.putUnencodedChars(from);
                    }
                }, entries, FPP);
            }
            return filter;
        }

    }
}
