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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.plugins.index.fulltext.ExtractedText;
import org.apache.jackrabbit.oak.plugins.index.fulltext.ExtractedText.ExtractionResult;
import org.apache.jackrabbit.oak.plugins.index.fulltext.PreExtractedTextProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.commons.PathUtils.concat;

public class ExtractedTextCache {
    private static final boolean CACHE_ONLY_SUCCESS =
            Boolean.getBoolean("oak.extracted.cacheOnlySuccess");
    private static final int EXTRACTION_TIMEOUT_SECONDS =
            Integer.getInteger("oak.extraction.timeoutSeconds", 60);
    private static final int EXTRACTION_MAX_THREADS =
            Integer.getInteger("oak.extraction.maxThreads", 10);
    private static final boolean EXTRACT_IN_CALLER_THREAD =
            Boolean.getBoolean("oak.extraction.inCallerThread");
    private static final boolean EXTRACT_FORGET_TIMEOUT =
            Boolean.getBoolean("oak.extraction.forgetTimeout");

    private static final String TIMEOUT_MAP = "textExtractionTimeout.properties";
    private static final String EMPTY_STRING = "";
    private static final Logger log = LoggerFactory.getLogger(ExtractedTextCache.class);
    private volatile PreExtractedTextProvider extractedTextProvider;
    private int textExtractionCount;
    private long totalBytesRead;
    private long totalTextSize;
    private long totalTime;
    private int preFetchedCount;
    private final Cache<String, String> cache;
    private final ConcurrentHashMap<String, String> timeoutMap;
    private final File indexDir;
    private final CacheStats cacheStats;
    private final boolean alwaysUsePreExtractedCache;
    private volatile ExecutorService executorService;
    private volatile int timeoutCount;
    private long extractionTimeoutMillis = EXTRACTION_TIMEOUT_SECONDS * 1000;

    public ExtractedTextCache(long maxWeight, long expiryTimeInSecs){
        this(maxWeight, expiryTimeInSecs, false, null);
    }

    public ExtractedTextCache(long maxWeight, long expiryTimeInSecs, boolean alwaysUsePreExtractedCache,
            File indexDir) {
        if (maxWeight > 0) {
            cache = CacheBuilder.newBuilder()
                    .weigher(EmpiricalWeigher.INSTANCE)
                    .maximumWeight(maxWeight)
                    .expireAfterAccess(expiryTimeInSecs, TimeUnit.SECONDS)
                    .recordStats()
                    .build();
            cacheStats = new CacheStats(cache, "ExtractedTextCache",
                    EmpiricalWeigher.INSTANCE, maxWeight);
        } else {
            cache = null;
            cacheStats = null;
        }
        this.alwaysUsePreExtractedCache = alwaysUsePreExtractedCache;
        this.timeoutMap = new ConcurrentHashMap<String, String>();
        this.indexDir = indexDir;
        loadTimeoutMap();
    }

    /**
     * Get the pre extracted text for given blob
     * @return null if no pre extracted text entry found. Otherwise returns the pre extracted
     *  text
     */
    @CheckForNull
    public String get(String nodePath, String propertyName, Blob blob, boolean reindexMode){
        String result = null;
        //Consult the PreExtractedTextProvider only in reindex mode and not in
        //incremental indexing mode. As that would only contain older entries
        //That also avoid loading on various state (See DataStoreTextWriter)
        String propertyPath = concat(nodePath, propertyName);
        log.trace("Looking for extracted text for [{}] with blobId [{}]", propertyPath, blob.getContentIdentity());
        if ((reindexMode || alwaysUsePreExtractedCache) && extractedTextProvider != null){
            try {
                ExtractedText text = extractedTextProvider.getText(propertyPath, blob);
                if (text != null) {
                    preFetchedCount++;
                    result = getText(text);
                }
            } catch (IOException e) {
                log.warn("Error occurred while fetching pre extracted text for {}", propertyPath, e);
            }
        }
        String id = blob.getContentIdentity();
        if (cache != null && id != null && result == null) {
            result = cache.getIfPresent(id);
        }
        if (result == null && id != null) {
            result = timeoutMap.get(id);
        }
        return result;
    }

    public void put(@Nonnull Blob blob, @Nonnull ExtractedText extractedText) {
        String id = blob.getContentIdentity();
        if (cache != null && id != null) {
            if (extractedText.getExtractionResult() == ExtractionResult.SUCCESS
                    || !CACHE_ONLY_SUCCESS) {
                cache.put(id, getText(extractedText));
            }
        }
    }

    public void putTimeout(@Nonnull Blob blob, @Nonnull ExtractedText extractedText) {
        if (EXTRACT_FORGET_TIMEOUT) {
            return;
        }
        String id = blob.getContentIdentity();
        timeoutMap.put(id, getText(extractedText));
        storeTimeoutMap();
    }

    private static String getText(ExtractedText text) {
        switch (text.getExtractionResult()) {
        case SUCCESS:
            return text.getExtractedText().toString();
        case ERROR:
            return LuceneIndexEditor.TEXT_EXTRACTION_ERROR;
        case EMPTY:
            return EMPTY_STRING;
        }
        throw new IllegalArgumentException();
    }

    public void addStats(int count, long timeInMillis, long bytesRead, long textLength){
        this.textExtractionCount += count;
        this.totalTime += timeInMillis;
        this.totalBytesRead += bytesRead;
        this.totalTextSize += textLength;
    }

    public TextExtractionStatsMBean getStatsMBean() {
        return new TextExtractionStatsMBean() {
            @Override
            public boolean isPreExtractedTextProviderConfigured() {
                return extractedTextProvider != null;
            }

            @Override
            public int getTextExtractionCount() {
                return textExtractionCount;
            }

            @Override
            public long getTotalTime() {
                return totalTime;
            }

            @Override
            public int getPreFetchedCount() {
                return preFetchedCount;
            }

            @Override
            public String getExtractedTextSize() {
                return IOUtils.humanReadableByteCount(totalTextSize);
            }

            @Override
            public String getBytesRead() {
                return IOUtils.humanReadableByteCount(totalBytesRead);
            }

            @Override
            public boolean isAlwaysUsePreExtractedCache() {
                return alwaysUsePreExtractedCache;
            }

            @Override
            public int getTimeoutCount() {
                return timeoutCount;
            }
        };
    }

    @CheckForNull
    public CacheStats getCacheStats() {
        return cacheStats;
    }

    public void setExtractedTextProvider(PreExtractedTextProvider extractedTextProvider) {
        this.extractedTextProvider = extractedTextProvider;
    }

    public PreExtractedTextProvider getExtractedTextProvider() {
        return extractedTextProvider;
    }

    void resetCache(){
        if (cache != null){
            cache.invalidateAll();
        }
    }

    boolean isAlwaysUsePreExtractedCache() {
        return alwaysUsePreExtractedCache;
    }

    //Taken from DocumentNodeStore and cache packages as they are private
    private static class EmpiricalWeigher implements Weigher<String, String> {
        public static final EmpiricalWeigher INSTANCE = new EmpiricalWeigher();

        private EmpiricalWeigher() {
        }

        private static long getMemory(@Nonnull String s) {
            return 16                              // shallow size
                    + 40 + (long)s.length() * 2;   // value
        }

        @Override
        public int weigh(String key, String value) {
            long size = 168;               // overhead for each cache entry
            size += getMemory(key);        // key
            size += getMemory(value);      // value
            if (size > Integer.MAX_VALUE) {
                log.debug("Calculated weight larger than Integer.MAX_VALUE: {}.", size);
                size = Integer.MAX_VALUE;
            }
            return (int) size;
        }
    }

    public void close() {
        resetCache();
        // don't clean the persistent map on purpose, so we don't re-try
        // after restarting the service or so
        closeExecutorService();
    }

    public void process(String name, Callable<Void> callable) throws InterruptedException, Throwable {
        Callable<Void> callable2 = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                Thread t = Thread.currentThread();
                String oldThreadName = t.getName();
                t.setName(oldThreadName + ": " + name);
                try {
                    return callable.call();
                } finally {
                    Thread.currentThread().setName(oldThreadName);
                }
            }
        };
        try {
            if (EXTRACT_IN_CALLER_THREAD) {
                callable2.call();
            } else {
                Future<Void> future = getExecutor().submit(callable2);
                future.get(extractionTimeoutMillis, TimeUnit.MILLISECONDS);
            }
        } catch (TimeoutException e) {
            timeoutCount++;
            throw e;
        } catch (InterruptedException e) {
            throw e;
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    public void setExtractionTimeoutMillis(int extractionTimeoutMillis) {
        this.extractionTimeoutMillis = extractionTimeoutMillis;
    }

    private ExecutorService getExecutor() {
        if (executorService == null) {
            createExecutor();
        }
        return executorService;
    }

    private synchronized void createExecutor() {
        if (executorService != null) {
            return;
        }
        log.debug("ExtractedTextCache createExecutor " + this);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(1, EXTRACTION_MAX_THREADS,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(), new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger();
            private final Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {
                @Override
                public void uncaughtException(Thread t, Throwable e) {
                    log.warn("Error occurred in asynchronous processing ", e);
                }
            };
            @Override
            public Thread newThread(@Nonnull Runnable r) {
                Thread thread = new Thread(r, createName());
                thread.setDaemon(true);
                thread.setPriority(Thread.MIN_PRIORITY);
                thread.setUncaughtExceptionHandler(handler);
                return thread;
            }

            private String createName() {
                int index = counter.getAndIncrement();
                return "oak binary text extractor" + (index == 0 ? "" : " " + index);
            }
        });
        executor.setKeepAliveTime(1, TimeUnit.MINUTES);
        executor.allowCoreThreadTimeOut(true);
        executorService = executor;
    }

    private synchronized void closeExecutorService() {
        if (executorService != null) {
            log.debug("ExtractedTextCache closeExecutorService " + this);
            executorService.shutdown();
            try {
                executorService.awaitTermination(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                log.warn("Interrupted", e);
            }
            executorService = null;
        }
    }

    private synchronized void loadTimeoutMap() {
        if (indexDir == null || !indexDir.exists()) {
            return;
        }
        File file = new File(indexDir, TIMEOUT_MAP);
        if (!file.exists()) {
            return;
        }
        try (FileInputStream in = new FileInputStream(file)) {
            Properties prop = new Properties();
            prop.load(in);
            for(Entry<Object, Object> e : prop.entrySet()) {
                timeoutMap.put(e.getKey().toString(), e.getValue().toString());
            }
        } catch (Exception e) {
            log.warn("Could not load timeout map {} from {}",
                    TIMEOUT_MAP, indexDir, e);
        }
    }

    private synchronized void storeTimeoutMap() {
        if (indexDir == null || !indexDir.exists()) {
            return;
        }
        File file = new File(indexDir, TIMEOUT_MAP);
        try (FileOutputStream out = new FileOutputStream(file)) {
            Properties prop = new Properties();
            prop.putAll(timeoutMap);
            prop.store(out, "Text extraction timed out for the following binaries, and will not be retried");
        } catch (Exception e) {
            log.warn("Could not store timeout map {} from {}",
                    TIMEOUT_MAP, indexDir, e);
        }
    }

}
