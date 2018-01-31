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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.Collections;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.cache.Cache;
import com.google.common.collect.Maps;

import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.plugins.document.util.RevisionsKey;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A diff cache, which is pro-actively filled after a commit.
 */
public class LocalDiffCache extends DiffCache {
    private static final Logger LOG = LoggerFactory.getLogger(LocalDiffCache.class);

    /**
     * Limit is arbitrary for now i.e. 16 MB. Same as in MongoDiffCache
     */
    private static int MAX_ENTRY_SIZE = 16 * 1024 * 1024;

    private final Cache<RevisionsKey, Diff> diffCache;
    private final CacheStats diffCacheStats;

    LocalDiffCache(DocumentNodeStoreBuilder<?> builder) {
        this.diffCache = builder.buildLocalDiffCache();
        this.diffCacheStats = new CacheStats(diffCache,
                "Document-LocalDiff",
                builder.getWeigher(), builder.getLocalDiffCacheSize());
    }

    @Override
    public String getChanges(@Nonnull RevisionVector from,
                             @Nonnull RevisionVector to,
                             @Nonnull String path,
                             @Nullable Loader loader) {
        RevisionsKey key = new RevisionsKey(from, to);
        Diff diff = diffCache.getIfPresent(key);
        if (diff != null) {
            String result = diff.get(path);
            return result != null ? result : "";
        }
        if (loader != null) {
            return loader.call();
        }
        return null;
    }

    @Nonnull
    @Override
    public Entry newEntry(final @Nonnull RevisionVector from,
                          final @Nonnull RevisionVector to,
                          boolean local /*ignored*/) {
        return new Entry() {
            private final Map<String, String> changesPerPath = Maps.newHashMap();
            private long size;
            @Override
            public void append(@Nonnull String path, @Nonnull String changes) {
                if (exceedsSize()){
                    return;
                }
                size += size(path) + size(changes);
                changesPerPath.put(path, changes);
            }

            @Override
            public boolean done() {
                if (exceedsSize()){
                    return false;
                }
                diffCache.put(new RevisionsKey(from, to),
                        new Diff(changesPerPath, size));
                LOG.debug("Adding cache entry from {} to {}", from, to);
                return true;
            }

            private boolean exceedsSize(){
                return size > MAX_ENTRY_SIZE;
            }
        };
    }

    @Nonnull
    @Override
    public Iterable<CacheStats> getStats() {
        return Collections.singleton(diffCacheStats);
    }

    //-----------------------------< internal >---------------------------------


    public static final class Diff implements CacheValue {

        private final Map<String, String> changes;
        private long memory;

        public Diff(Map<String, String> changes, long memory) {
            this.changes = changes;
            this.memory = memory;
        }

        public static Diff fromString(String value) {
            Map<String, String> map = Maps.newHashMap();
            JsopReader reader = new JsopTokenizer(value);
            while (true) {
                if (reader.matches(JsopReader.END)) {
                    break;
                }
                String k = reader.readString();
                reader.read(':');
                String v = reader.readString();
                map.put(k, v);
                if (reader.matches(JsopReader.END)) {
                    break;
                }
                reader.read(',');
            }
            return new Diff(map, 0);
        }

        public String asString(){
            JsopBuilder builder = new JsopBuilder();
            for (Map.Entry<String, String> entry : changes.entrySet()) {
                builder.key(entry.getKey());
                builder.value(entry.getValue());
            }
            return builder.toString();
        }

        public Map<String, String> getChanges() {
            return Collections.unmodifiableMap(changes);
        }

        @Override
        public int getMemory() {
            if (memory == 0) {
                long m = 0;
                for (Map.Entry<String, String> e : changes.entrySet()){
                    m += size(e.getKey()) + size(e.getValue());
                }
                memory = m;
            }
            if (memory > Integer.MAX_VALUE) {
                LOG.debug("Estimated memory footprint larger than Integer.MAX_VALUE: {}.", memory);
                return Integer.MAX_VALUE;
            } else {
                return (int) memory;
            }
        }

        String get(String path) {
            return changes.get(path);
        }

        @Override
        public String toString() {
            return asString();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj instanceof Diff) {
                Diff other = (Diff) obj;
                return changes.equals(other.changes);
            }
            return false;
        }
    }

    private static long size(String s){
        return StringValue.getMemory(s);
    }
}
