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

package org.apache.jackrabbit.oak.plugins.document;

import java.util.Collections;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.cache.Cache;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LocalDiffCache implements DiffCache {
    /**
     * Limit is arbitrary for now i.e. 16 MB. Same as in MongoDiffCache
     */
    private static int MAX_ENTRY_SIZE = 16 * 1024 * 1024;
    private static final String NO_DIFF = "";
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Cache<StringValue, ConsolidatedDiff> diffCache;
    private final CacheStats diffCacheStats;

    public LocalDiffCache(DocumentMK.Builder builder) {
        diffCache = builder.buildConsolidatedDiffCache();
        diffCacheStats = new CacheStats(diffCache, "Document-Diff2",
                builder.getWeigher(), builder.getDiffCacheSize());
    }

    @Override
    public String getChanges(@Nonnull Revision from,
                             @Nonnull Revision to,
                             @Nonnull String path,
                             @Nullable Loader loader) {
        ConsolidatedDiff diff = diffCache.getIfPresent(new StringValue(to.toString()));
        if (diff != null){
            String result = diff.get(path);
            if (result == null){
                return NO_DIFF;
            }
            return result;
        } else {
            log.debug("Did not got the diff for local change in the cache for change {} => {} ", from, to);
        }
        return null;
    }

    ConsolidatedDiff getDiff(@Nonnull Revision from,
                             @Nonnull Revision to){
        return diffCache.getIfPresent(new StringValue(to.toString()));
    }

    @Nonnull
    @Override
    public Entry newEntry(@Nonnull Revision from, final @Nonnull Revision to) {
        return new Entry() {
            private final Map<String, String> changesPerPath = Maps.newHashMap();
            private int size;
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
                diffCache.put(new StringValue(to.toString()), new ConsolidatedDiff(changesPerPath, size));
                return true;
            }

            private boolean exceedsSize(){
                return size > MAX_ENTRY_SIZE;
            }
        };
    }

    public CacheStats getDiffCacheStats() {
        return diffCacheStats;
    }

    public static final class ConsolidatedDiff implements CacheValue{
        //TODO need to come up with better serialization strategy as changes are json themselves
        //cannot use JSON. '/' and '*' are considered invalid chars so would not
        //cause issue
        static final Joiner.MapJoiner mapJoiner = Joiner.on("//").withKeyValueSeparator("**");
        static final Splitter.MapSplitter splitter = Splitter.on("//").withKeyValueSeparator("**");
        private final Map<String, String> changes;
        private int memory;

        public ConsolidatedDiff(Map<String, String> changes, int memory) {
            this.changes = changes;
            this.memory = memory;
        }

        public static ConsolidatedDiff fromString(String value){
            if (value.isEmpty()){
                return new ConsolidatedDiff(Collections.<String, String>emptyMap(), 0);
            }
            return new ConsolidatedDiff(splitter.split(value), 0);
        }

        public String asString(){
            return mapJoiner.join(changes);
        }

        @Override
        public int getMemory() {
            if (memory == 0) {
                int m = 0;
                for (Map.Entry<String, String> e : changes.entrySet()){
                    m += size(e.getKey()) + size(e.getValue());
                }
                memory = m;
            }
            return memory;
        }

        @Override
        public String toString() {
            return changes.toString();
        }

        String get(String path) {
            return changes.get(path);
        }

        @SuppressWarnings("RedundantIfStatement")
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ConsolidatedDiff that = (ConsolidatedDiff) o;

            if (!changes.equals(that.changes)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return changes.hashCode();
        }
    }

    private static int size(String s){
        //Taken from StringValue
        return 16                           // shallow size
                + 40 + s.length() * 2;
    }

}
