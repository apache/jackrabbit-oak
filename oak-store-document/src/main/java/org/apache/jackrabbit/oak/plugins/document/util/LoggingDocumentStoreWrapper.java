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

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.apache.jackrabbit.oak.plugins.document.RevisionListener;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.apache.jackrabbit.oak.plugins.document.cache.CacheInvalidationStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements a <code>DocumentStore</code> wrapper and logs all calls.
 */
public class LoggingDocumentStoreWrapper implements DocumentStore, RevisionListener {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingDocumentStoreWrapper.class);

    private static final boolean DEBUG = Boolean.parseBoolean(System.getProperty("ds.debug", "true"));

    final DocumentStore store;
    private boolean logThread;

    public LoggingDocumentStoreWrapper(DocumentStore store) {
        this.store = store;
    }

    public LoggingDocumentStoreWrapper withThreadNameLogging() {
        this.logThread = true;
        return this;
    }

    @Override
    public <T extends Document> T find(final Collection<T> collection,
                                       final String key) {
        try {
            logMethod("find", collection, key);
            return logResult(new Callable<T>() {
                @Override
                public T call() throws Exception {
                    return store.find(collection, key);
                }
            });
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public <T extends Document> T find(final Collection<T> collection,
                                       final String key,
                                       final int maxCacheAge) {
        try {
            logMethod("find", collection, key, maxCacheAge);
            return logResult(new Callable<T>() {
                @Override
                public T call() throws Exception {
                    return store.find(collection, key, maxCacheAge);
                }
            });
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Nonnull
    @Override
    public <T extends Document> List<T> query(final Collection<T> collection,
                                final String fromKey,
                                final String toKey,
                                final int limit) {
        try {
            logMethod("query", collection, fromKey, toKey, limit);
            return logResult(new Callable<List<T>>() {
                @Override
                public List<T> call() throws Exception {
                    return store.query(collection, fromKey, toKey, limit);
                }
            });
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    @Nonnull
    public <T extends Document> List<T> query(final Collection<T> collection,
                                final String fromKey,
                                final String toKey,
                                final String indexedProperty,
                                final long startValue,
                                final int limit) {
        try {
            logMethod("query", collection, fromKey, toKey, indexedProperty, startValue, limit);
            return logResult(new Callable<List<T>>() {
                @Override
                public List<T> call() throws Exception {
                    return store.query(collection, fromKey, toKey, indexedProperty, startValue, limit);
                }
            });
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, String key) {
        try {
            logMethod("remove", collection, key);
            store.remove(collection, key);
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, List<String> keys) {
        try {
            logMethod("remove", collection, keys);
            store.remove(collection, keys);
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public <T extends Document> int remove(final Collection<T> collection,
                                           final Map<String, Long> toRemove) {
        try {
            logMethod("remove", collection, toRemove);
            return logResult(new Callable<Integer>() {
                @Override
                public Integer call() throws Exception {
                    return store.remove(collection, toRemove);
                }
            });
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public <T extends Document> int remove(final Collection<T> collection,
                                           final String indexedProperty, final long startValue, final long endValue)
            throws DocumentStoreException {
        try {
            logMethod("remove", collection, indexedProperty, startValue, endValue);
            return logResult(new Callable<Integer>() {
                @Override
                public Integer call() throws Exception {
                    return store.remove(collection, indexedProperty, startValue, endValue);
                }
            });
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public <T extends Document> boolean create(final Collection<T> collection,
                                               final List<UpdateOp> updateOps) {
        try {
            logMethod("create", collection, updateOps);
            return logResult(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    return store.create(collection, updateOps);
                }
            });
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Nonnull
    @Override
    public <T extends Document> T createOrUpdate(final Collection<T> collection,
                                                 final UpdateOp update) {
        try {
            logMethod("createOrUpdate", collection, update);
            return logResult(new Callable<T>() {
                @Override
                public T call() throws Exception {
                    return store.createOrUpdate(collection, update);
                }
            });
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public <T extends Document> List<T> createOrUpdate(final Collection<T> collection,
                                                       final List<UpdateOp> updateOps) {
        try {
            logMethod("createOrUpdate", collection, updateOps);
            return logResult(new Callable<List<T>>() {
                @Override
                public List<T> call() throws Exception {
                    return store.createOrUpdate(collection, updateOps);
                }
            });
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public <T extends Document> T findAndUpdate(final Collection<T> collection,
                                                final UpdateOp update) {
        try {
            logMethod("findAndUpdate", collection, update);
            return logResult(new Callable<T>() {
                @Override
                public T call() throws Exception {
                    return store.findAndUpdate(collection, update);
                }
            });
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public CacheInvalidationStats invalidateCache() {
        try {
            logMethod("invalidateCache");
            return store.invalidateCache();
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }
    
    @Override
    public CacheInvalidationStats invalidateCache(Iterable<String> keys) {
        try {
            logMethod("invalidateCache", keys);
            return store.invalidateCache(keys);
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public <T extends Document> void invalidateCache(Collection<T> collection, String key) {
        try {
            logMethod("invalidateCache", collection, key);
            store.invalidateCache(collection, key);
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public void dispose() {
        try {
            logMethod("dispose");
            store.dispose();
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public <T extends Document> T getIfCached(final Collection<T> collection,
                                              final String key) {
        try {
            logMethod("getIfCached", collection, key);
            return logResult(new Callable<T>() {
                @Override
                public T call() throws Exception {
                    return store.getIfCached(collection, key);
                }
            });
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public void setReadWriteMode(String readWriteMode) {
        try {
            logMethod("setReadWriteMode", readWriteMode);
            store.setReadWriteMode(readWriteMode);
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public Iterable<CacheStats> getCacheStats() {
        try {
            logMethod("getCacheStats");
            return logResult(new Callable<Iterable<CacheStats>>() {
                @Override
                public Iterable<CacheStats> call() throws Exception {
                    return store.getCacheStats();
                }
            });
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public Map<String, String> getMetadata() {
        return store.getMetadata();
    }

    @Nonnull
    @Override
    public Map<String, String> getStats() {
        Map<String, String> result = store.getStats();
        logMethod("getStats", result);
        return result;
    }

    @Override
    public long determineServerTimeDifferenceMillis() {
        logMethod("determineServerTimeDifferenceMillis", "start");
        long result = store.determineServerTimeDifferenceMillis();
        logMethod("determineServerTimeDifferenceMillis", "end", result);
        return result;
    }

    private void logMethod(String methodName, Object... args) {
        StringBuilder buff = new StringBuilder("ds");
        buff.append('.').append(methodName).append('(');
        for (int i = 0; i < args.length; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            buff.append(quote(args[i]));
        }
        buff.append(");");
        log(buff.toString());
    }

    public static String quote(Object o) {
        if (o == null) {
            return "null";
        } else if (o instanceof String) {
            return JsopBuilder.encode((String) o);
        }
        return o.toString();
    }

    private RuntimeException convert(Exception e) {
        if (e instanceof RuntimeException) {
            return (RuntimeException) e;
        }
        log("// unexpected exception type: " + e.getClass().getName());
        return new DocumentStoreException("Unexpected exception: " + e.toString(), e);
    }

    private void logException(Exception e) {
        log("// exception: " + e.toString());
    }

    private <T> T logResult(Callable<T> callable) throws Exception {
        long time = System.nanoTime();
        T result = callable.call();
        time = System.nanoTime() - time;
        log("// " + (time / 1000) + " us\t" + quote(result));
        return result;
    }

    private void log(String message) {
        String out = this.logThread ? (Thread.currentThread() + " " + message) : message;
        if (DEBUG) {
            System.out.println(out);
        }
        LOG.info(out);
    }

    @Override
    public void updateAccessedRevision(RevisionVector revision, int currentClusterId) {
        logMethod("updateAccessedRevision", revision);
        if (store instanceof RevisionListener) {
            ((RevisionListener) store).updateAccessedRevision(revision, currentClusterId);
        }
    }
}
