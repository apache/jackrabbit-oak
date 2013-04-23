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
package org.apache.jackrabbit.mongomk.util;

import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.mongomk.DocumentStore;
import org.apache.jackrabbit.mongomk.UpdateOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements a <code>DocumentStore</code> wrapper and logs all calls.
 */
public class LoggingDocumentStoreWrapper implements DocumentStore {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingDocumentStoreWrapper.class);

    private static final boolean DEBUG = Boolean.parseBoolean(System.getProperty("ds.debug", "true"));

    private final DocumentStore store;

    public LoggingDocumentStoreWrapper(DocumentStore store) {
        this.store = store;
    }

    @Override
    public Map<String, Object> find(Collection collection, String key) {
        try {
            logMethod("find", collection, key);
            return logResult(store.find(collection, key));
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public Map<String, Object> find(Collection collection,
                                    String key,
                                    int maxCacheAge) {
        try {
            logMethod("find", collection, key, maxCacheAge);
            return logResult(store.find(collection, key, maxCacheAge));
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Nonnull
    @Override
    public List<Map<String, Object>> query(Collection collection,
                                           String fromKey,
                                           String toKey,
                                           int limit) {
        try {
            logMethod("query", collection, fromKey, toKey, limit);
            return logResult(store.query(collection, fromKey, toKey, limit));
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public void remove(Collection collection, String key) {
        try {
            logMethod("remove", collection, key);
            store.remove(collection, key);
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public boolean create(Collection collection, List<UpdateOp> updateOps) {
        try {
            logMethod("create", collection, updateOps);
            return logResult(store.create(collection, updateOps));
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Nonnull
    @Override
    public Map<String, Object> createOrUpdate(Collection collection,
                                              UpdateOp update)
            throws MicroKernelException {
        try {
            logMethod("createOrUpdate", collection, update);
            return logResult(store.createOrUpdate(collection, update));
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public void invalidateCache() {
        try {
            logMethod("invalidateCache");
            store.invalidateCache();
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
    public boolean isCached(Collection collection, String key) {
        try {
            logMethod("isCached", collection, key);
            return logResult(store.isCached(collection, key));
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    private static void logMethod(String methodName, Object... args) {
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

    private static RuntimeException convert(Exception e) {
        if (e instanceof RuntimeException) {
            return (RuntimeException) e;
        }
        log("// unexpected exception type: " + e.getClass().getName());
        return new MicroKernelException("Unexpected exception: " + e.toString(), e);
    }

    private static void logException(Exception e) {
        log("// exception: " + e.toString());
    }

    private static <T> T logResult(T result) {
        log("// " + quote(result));
        return result;
    }

    private static void log(String message) {
        if (DEBUG) {
            System.out.println(message);
        }
        LOG.info(message);
    }
}
