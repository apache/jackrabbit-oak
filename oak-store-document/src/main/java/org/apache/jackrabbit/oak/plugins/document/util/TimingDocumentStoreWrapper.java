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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.apache.jackrabbit.oak.plugins.document.RevisionListener;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.apache.jackrabbit.oak.plugins.document.cache.CacheInvalidationStats;

/**
 * A DocumentStore wrapper that can be used to log and also time DocumentStore
 * calls.
 */
public class TimingDocumentStoreWrapper implements DocumentStore, RevisionListener {

    private static final boolean DEBUG = Boolean.parseBoolean(System.getProperty("base.debug", "true"));
    private static final AtomicInteger NEXT_ID = new AtomicInteger();

    private final DocumentStore base;
    private final int id = NEXT_ID.getAndIncrement();

    private long startTime;
    private final Map<String, Count> counts = new HashMap<String, Count>();
    private long lastLogTime;
    private long totalLogTime;
    private final Map<String, Integer> slowCalls = new ConcurrentHashMap<String, Integer>();

    private int callCount;

    /**
     * A class that keeps track of timing data and call counts.
     */
    static class Count {
        public long count;
        public long max;
        public long total;
        public long paramSize;
        public long resultSize;

        void update(long time, int paramSize, int resultSize) {
            count++;
            if (time > max) {
                max = time;
            }
            total += time;
            this.paramSize += paramSize;
            this.resultSize += resultSize;
        }
    }

    public TimingDocumentStoreWrapper(DocumentStore base) {
        this.base = base;
        lastLogTime = now();
    }

    private boolean logCommonCall() {
        return callCount % 10 == 0;
    }

    @Override
    @CheckForNull
    public <T extends Document> T find(Collection<T> collection, String key) {
        try {
            long start = now();
            T result = base.find(collection, key);
            updateAndLogTimes("find", start, 0, size(result));
            if (logCommonCall()) {
                logCommonCall(start, "find " + collection + " " + key);
            }
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    @CheckForNull
    public <T extends Document> T find(Collection<T> collection, String key, int maxCacheAge) {
        try {
            long start = now();
            T result = base.find(collection, key, maxCacheAge);
            updateAndLogTimes("find2", start, 0, size(result));
            if (logCommonCall()) {
                logCommonCall(start, "find2 " + collection + " " + key);
            }
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    @Nonnull
    public <T extends Document> List<T> query(Collection<T> collection,
                                                String fromKey,
                                                String toKey,
                                                int limit) {
        try {
            long start = now();
            List<T> result = base.query(collection, fromKey, toKey, limit);
            if (result.size() == 0) {
                updateAndLogTimes("query, result=0", start, 0, size(result));
            } else if (result.size() == 1) {
                updateAndLogTimes("query, result=1", start, 0, size(result));
            } else {
                updateAndLogTimes("query, result>1", start, 0, size(result));
            }
            if (logCommonCall()) {
                logCommonCall(start, "query " + collection + " " + fromKey + " " + toKey + " " + limit);
            }
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    @Nonnull
    public <T extends Document> List<T> query(Collection<T> collection,
                                              String fromKey,
                                              String toKey,
                                              String indexedProperty,
                                              long startValue,
                                              int limit) {
        try {
            long start = now();
            List<T> result = base.query(collection, fromKey, toKey, indexedProperty, startValue, limit);
            updateAndLogTimes("query2", start, 0, size(result));
            if (logCommonCall()) {
                logCommonCall(start, "query2 " + collection + " " + fromKey + " " + toKey + " " + indexedProperty + " " + startValue + " " + limit);
            }
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, String key) {
        try {
            long start = now();
            base.remove(collection, key);
            updateAndLogTimes("remove", start, 0, 0);
            if (logCommonCall()) {
                logCommonCall(start, "remove " + collection + " " + key);
            }
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, List<String> keys) {
        try {
            long start = now();
            base.remove(collection, keys);
            updateAndLogTimes("remove", start, 0, 0);
            if (logCommonCall()) {
                logCommonCall(start, "remove " + collection + " " + keys);
            }
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public <T extends Document> int remove(Collection<T> collection,
                                           Map<String, Long> toRemove) {
        try {
            long start = now();
            int result = base.remove(collection, toRemove);
            updateAndLogTimes("remove", start, 0, 0);
            if (logCommonCall()) {
                logCommonCall(start, "remove " + collection + " " + toRemove);
            }
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public <T extends Document> int remove(Collection<T> collection,
                                               String indexedProperty, long startValue, long endValue)
            throws DocumentStoreException {
        try {
            long start = now();
            int result = base.remove(collection, indexedProperty, startValue, endValue);
            updateAndLogTimes("remove", start, 0, 0);
            if (logCommonCall()) {
                logCommonCall(start, "remove " + collection + "; indexedProperty" + indexedProperty +
                    "; range - (" + startValue + ", " + endValue + ")");
            }
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }


    @Override
    public <T extends Document> boolean create(Collection<T> collection, List<UpdateOp> updateOps) {
        try {
            long start = now();
            boolean result = base.create(collection, updateOps);
            updateAndLogTimes("create", start, 0, 0);
            if (logCommonCall()) {
                logCommonCall(start, "create " + collection);
            }
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    @CheckForNull
    public <T extends Document> T createOrUpdate(Collection<T> collection, UpdateOp update) {
        try {
            long start = now();
            T result = base.createOrUpdate(collection, update);
            updateAndLogTimes("createOrUpdate", start, 0, size(result));
            if (logCommonCall()) {
                logCommonCall(start, "createOrUpdate " + collection + " " + update.getId());
            }
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public <T extends Document> List<T> createOrUpdate(Collection<T> collection, List<UpdateOp> updateOps) {
        try {
            long start = now();
            List<T> result = base.createOrUpdate(collection, updateOps);
            updateAndLogTimes("createOrUpdate", start, 0, size(result));
            if (logCommonCall()) {
                List<String> ids = new ArrayList<String>();
                for (UpdateOp op : updateOps) {
                    ids.add(op.getId());
                }
                logCommonCall(start, "createOrUpdate " + collection + " " + updateOps + " " + ids);
            }
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    @CheckForNull
    public <T extends Document> T findAndUpdate(Collection<T> collection, UpdateOp update) {
        try {
            long start = now();
            T result = base.findAndUpdate(collection, update);
            updateAndLogTimes("findAndUpdate", start, 0, size(result));
            if (logCommonCall()) {
                logCommonCall(start, "findAndUpdate " + collection + " " + update.getId());
            }
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public CacheInvalidationStats invalidateCache() {
        try {
            long start = now();
            CacheInvalidationStats result = base.invalidateCache();
            updateAndLogTimes("invalidateCache", start, 0, 0);
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }
    
    @Override
    public CacheInvalidationStats invalidateCache(Iterable<String> keys) {
        try {
            long start = now();
            CacheInvalidationStats result = base.invalidateCache(keys);
            updateAndLogTimes("invalidateCache3", start, 0, 0);
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public <T extends Document> void invalidateCache(Collection<T> collection, String key) {
        try {
            long start = now();
            base.invalidateCache(collection, key);
            updateAndLogTimes("invalidateCache2", start, 0, 0);
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public void dispose() {
        try {
            long start = now();
            base.dispose();
            updateAndLogTimes("dispose", start, 0, 0);
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public <T extends Document> T getIfCached(Collection<T> collection, String key) {
        try {
            long start = now();
            T result = base.getIfCached(collection, key);
            updateAndLogTimes("isCached", start, 0, 0);
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public void setReadWriteMode(String readWriteMode) {
        try {
            long start = now();
            base.setReadWriteMode(readWriteMode);
            updateAndLogTimes("setReadWriteMode", start, 0, 0);
        } catch (Exception e) {
            throw convert(e);
        }
    }


    @Override
    public Iterable<CacheStats> getCacheStats() {
        try {
            long start = now();
            Iterable<CacheStats> result = base.getCacheStats();
            updateAndLogTimes("getCacheStats", start, 0, 0);
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public Map<String, String> getMetadata() {
        return base.getMetadata();
    }

    @Nonnull
    @Override
    public Map<String, String> getStats() {
        try {
            long start = now();
            Map<String, String> result = base.getStats();
            updateAndLogTimes("getStats", start, 0, 0);
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public long determineServerTimeDifferenceMillis() {
        try {
            long start = now();
            long result = base.determineServerTimeDifferenceMillis();
            updateAndLogTimes("determineServerTimeDifferenceMillis", start, 0, 0);
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public void updateAccessedRevision(RevisionVector revision, int currentClusterId) {
        try {
            long start = now();
            if (base instanceof RevisionListener) {
                ((RevisionListener) base).updateAccessedRevision(revision, currentClusterId);
            }
            updateAndLogTimes("updateAccessedRevision", start, 0, 0);
        } catch (Exception e) {
            throw convert(e);
        }
   }

    private void logCommonCall(long start, String key) {
        int time = (int) (System.currentTimeMillis() - start);
        if (time <= 0) {
            return;
        }
        Map<String, Integer> map = slowCalls;
        Integer oldCount = map.get(key);
        if (oldCount == null) {
            map.put(key, time);
        } else {
            map.put(key, oldCount + time);
        }
        int maxElements = 1000;
        int minCount = 1;
        while (map.size() > maxElements) {
            for (Iterator<Map.Entry<String, Integer>> ei = map.entrySet().iterator(); ei.hasNext();) {
                Map.Entry<String, Integer> e = ei.next();
                if (e.getValue() <= minCount) {
                    ei.remove();
                }
            }
            if (map.size() > maxElements) {
                minCount++;
            }
        }
    }

    private static RuntimeException convert(Exception e) {
        if (e instanceof RuntimeException) {
            return (RuntimeException) e;
        }
        return new DocumentStoreException("Unexpected exception: " + e.toString(), e);
    }

    private void log(String message) {
        if (DEBUG) {
            System.out.println("[" + id + "] " + message);
        }
    }

    private static <T extends Document> int size(List<T> list) {
        int result = 0;
        for (T doc : list) {
            result += size(doc);
        }
        return result;
    }

    private static int size(@Nullable Document document) {
        if (document == null) {
            return 0;
        } else {
            return document.getMemory();
        }
    }

    private static long now() {
        return System.currentTimeMillis();
    }

    private void updateAndLogTimes(String operation, long start, int paramSize, int resultSize) {
        long now = now();
        if (startTime == 0) {
            startTime = now;
        }
        Count c = counts.get(operation);
        if (c == null) {
            c = new Count();
            counts.put(operation, c);
        }
        c.update(now - start, paramSize, resultSize);
        long t = now - lastLogTime;
        if (t >= 10000) {
            totalLogTime += t;
            lastLogTime = now;
            long totalCount = 0, totalTime = 0;
            for (Count count : counts.values()) {
                totalCount += count.count;
                totalTime += count.total;
            }
            totalCount = Math.max(1, totalCount);
            totalTime = Math.max(1, totalTime);
            for (Entry<String, Count> e : counts.entrySet()) {
                c = e.getValue();
                long count = c.count;
                long total = c.total;
                long in = c.paramSize / 1024 / 1024;
                long out = c.resultSize / 1024 / 1024;
                if (count > 0) {
                    log(e.getKey() +
                            " count " + count +
                            " " + (100 * count / totalCount) + "%" +
                            " in " + in + " out " + out +
                            " time " + total +
                            " " + (100 * total / totalTime) + "%");
                }
            }
            log("all count " + totalCount + " time " + totalTime + " " +
                    (100 * totalTime / totalLogTime) + "%");

            Map<String, Integer> map = slowCalls;
            int top = 10;
            int max = Integer.MAX_VALUE;
            for (int i = 0; i < top;) {
                int best = 0;
                for (int x : map.values()) {
                    if (x < max && x > best) {
                        best = x;
                    }
                }
                for (Entry<String, Integer> e : map.entrySet()) {
                    if (e.getValue() >= best && e.getValue() < max) {
                        log("slow call " + e.getValue() + " millis: " + e.getKey());
                        i++;
                        if (i >= top) {
                            break;
                        }
                    }
                }
                if (i >= map.size()) {
                    break;
                }
                max = best;
            }
            slowCalls.clear();

            log("------");

        }
    }
}
