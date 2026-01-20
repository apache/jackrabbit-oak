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
package org.apache.jackrabbit.oak.plugins.index.elastic.query;

import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.json.JsonpUtils;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.async.ElasticResultRowAsyncIterator;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexUtils;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Caches frequents requests (rate > MIN_CACHE_RATE_SECONDS executions per second) for a maximum of KEEP_ALIVE after a request has been made.
 * Frequency is approximated over a sliding window of WINDOW.
 *
 * Only cache results whose estimated number of hits is below MAX_CACHEABLE_HITS.
 */
public class ElasticRequestCache {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticRequestCache.class);
    private static final Duration WINDOW = Duration.ofMinutes(1);
    private static final Duration KEEP_ALIVE = Duration.ofSeconds(10);
    private static final int MIN_CACHE_RATE_SECONDS = 10;
    private static final int MAX_CACHEABLE_HITS = 10000;

    private static final ScheduledExecutorService EVICTION_EXECUTOR = Executors.newScheduledThreadPool(1);


    private final ConcurrentHashMap<String, CacheEntry> cache = new ConcurrentHashMap<>();

    public ElasticRequestCache() {
        EVICTION_EXECUTOR.schedule(this::evictExpired, KEEP_ALIVE.toSeconds(), java.util.concurrent.TimeUnit.SECONDS);
    }

    private void evictExpired() {
        Instant now = Instant.now();
        Collection<CacheEntry> values = cache.values();
        values.removeIf(f -> f.expiry.isBefore(now));
    }

    /**
     * Returns an iterator for the given request handler.
     * @param requestHandler the request handler
     * @param queryIterator a supplier for the iterator to eventually cache
     * @return either the raw iterator or a cached copy iterator
     */
    public ElasticQueryIterator iteratorFor(ElasticRequestHandler requestHandler, ElasticQueryIterator queryIterator) {
        if(!(queryIterator instanceof ElasticResultRowAsyncIterator)) {
            return queryIterator;
        }
        if(requestHandler.requiresFacets()) {
            // Don't cache when aggregations are required
            return queryIterator;
        }
        ElasticResultRowAsyncIterator baseIterator = (ElasticResultRowAsyncIterator) queryIterator;
        SearchRequest baseRequest = baseRequestOf(requestHandler);
        String serializedRequest = JsonpUtils.toString(baseRequest);
        String postFilterString = postFilterStringOf(requestHandler.getPlanFilter());
        String key = ElasticIndexUtils.sha256Hash((postFilterString + serializedRequest).getBytes());

        CacheEntry cacheEntry = cache.compute(key, (String ignored, CacheEntry currentEntry) -> this.computeEntry(key, currentEntry, baseIterator));
        if(cacheEntry.iterator == null) {
            return queryIterator;
        } else {
            if(cacheEntry.totalHits == null) {
                synchronized (baseIterator) {
                    cacheEntry.totalHits = baseIterator.getTotalHits();
                }
            }
            if(cacheEntry.totalHits > MAX_CACHEABLE_HITS) {
                return queryIterator;
            }
            return cacheEntry.iterator.getIterator();
        }
    }

    private String postFilterStringOf(Filter planFilter) {
        return Stream.concat(
                Stream.of(planFilter.getPath(),planFilter.getPathRestriction().name()),
                        planFilter.getPropertyRestrictions().stream().map(p -> p.propertyName + "=" + p)
        ).collect(Collectors.joining(":"));
    }

    private SearchRequest baseRequestOf(ElasticRequestHandler requestHandler) {
        return  SearchRequest.of(req -> req
                    .sort(requestHandler.baseSorts())
                    .query(requestHandler.baseQuery())
                    .highlight(requestHandler.highlight())
        );
    }

    private CacheEntry computeEntry(String key, CacheEntry currentEntry, @NotNull ElasticResultRowAsyncIterator baseIterator) {
        if(currentEntry == null) {
            return new CacheEntry(new MovingAverage(Instant.now()));
        }
        currentEntry.average.step(WINDOW);
        if(currentEntry.average.currentRate() > MIN_CACHE_RATE_SECONDS) {
            if(currentEntry.iterator == null || currentEntry.expiry.isBefore(Instant.now())) {
                LOG.info("Frequent query with key {}: caching", key);
                currentEntry.iterator = new SharedElasticQueryIteratorProvider(baseIterator);
                currentEntry.expiry = Instant.now().plus(KEEP_ALIVE);
            }
        } else {
            if(currentEntry.iterator != null) {
                LOG.info("Frequency returned below threshold for key {}: no longer caching", key);
            }
            currentEntry.iterator = null;
            currentEntry.expiry = Instant.now().plus(KEEP_ALIVE);
        }
        return currentEntry;
    }

    private static class CacheEntry {
        public Long totalHits;
        private SharedElasticQueryIteratorProvider iterator = null;
        private Instant expiry;
        private final MovingAverage average;

        public CacheEntry(MovingAverage average) {
            this.average = average;
            this.totalHits = null;
            this.expiry = Instant.now().plus(KEEP_ALIVE);
        }
    }

    static class MovingAverage {
        Instant lastStart;
        Instant lastEnd;
        long approxCount;

        public MovingAverage(Instant start) {
            this.lastEnd = start;
            this.approxCount = 0;
            this.lastStart = null;
        }

        public void step(Duration window) {
            step(window, Instant.now());
        }

        protected void step(Duration window, Instant newTime) {
            var windowStart = newTime.minus(window);
            if(lastEnd.isBefore(windowStart)) {
                // Reset
                lastStart = null;
                lastEnd = newTime;
                approxCount = 0;
                return;
            }
            if(lastStart == null) {
                // First event since init or reset
                lastStart = lastEnd;
                lastEnd = newTime;
                approxCount = 1;
                return;
            }
            if(lastStart.isAfter(windowStart)) {
                // Still haven't met a full window
                approxCount++;
                lastEnd = newTime;
                return;
            }
            // Estimation
            // |-----------------|---|
            // t0                lE  nT
            // t0 = nT - window
            // we have fp = approxCount / (lE - lS)
            // we suppose fp was equally distributed between lS and lE (and therefore between t0 and lE)
            var prevInWindowCount = approxCount * Duration.between(windowStart, lastEnd).toMillis() / Duration.between(lastStart, lastEnd).toMillis();
            approxCount = prevInWindowCount + 1;
            lastStart = windowStart;
            lastEnd = newTime;
        }

        long currentRate() {
            if(lastStart == null) {
                return 0;
            }
            long duration = Duration.between(lastStart, lastEnd).toMillis();
            if(duration == 0) {
                return Long.MAX_VALUE;
            }
            return 1000 * approxCount / duration;
        }
    }
}
