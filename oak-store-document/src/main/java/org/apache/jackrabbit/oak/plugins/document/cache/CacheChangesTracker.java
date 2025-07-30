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

import org.apache.jackrabbit.oak.commons.collections.BloomFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

public class CacheChangesTracker implements Closeable {

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

    public void invalidateDocument(String key) {
        if (keyFilter.test(key)) {
            lazyBloomFilter.put(key);
        }
    }

    public boolean mightBeenAffected(String key) {
        return keyFilter.test(key) && lazyBloomFilter.mightContain(key);
    }

    @Override
    public void close() {
        changeTrackers.remove(this);

        if (LOG.isDebugEnabled()) {
            if (lazyBloomFilter.filterRef.get() == null) {
                LOG.debug("Disposing CacheChangesTracker for {}, no filter was needed", keyFilter);
            } else {
                LOG.debug("Disposing CacheChangesTracker for {}, filter fpp was: {}", keyFilter, LazyBloomFilter.FPP);
            }
        }
    }

    public static class LazyBloomFilter {

        private static final double FPP = 0.01d;

        private final int entries;

        private final AtomicReference<BloomFilter> filterRef;

        public LazyBloomFilter(int entries) {
            this.entries = entries;
            this.filterRef = new AtomicReference<>();
        }

        public synchronized void put(String entry) {
            getFilter().add(entry);
        }

        public boolean mightContain(String entry) {
            BloomFilter f = filterRef.get();
            return f != null && f.mayContain(entry);
        }

        private BloomFilter getFilter() {
            BloomFilter result = filterRef.get();
            if (result == null) {
                BloomFilter newFilter = BloomFilter.construct(entries, FPP);
                if (filterRef.compareAndSet(null, newFilter)) {
                    result = newFilter;
                } else {
                    result = filterRef.get();
                }
            }
            return result;
        }
    }
}
