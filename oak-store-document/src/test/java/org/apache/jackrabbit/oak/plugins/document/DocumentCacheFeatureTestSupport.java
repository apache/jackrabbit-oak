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

/** Scoped feature opt-in for production ASYNC cache tests. */
public final class DocumentCacheFeatureTestSupport {
    private DocumentCacheFeatureTestSupport() {
    }

    /**
     * Enables both Document cache opt-ins until the returned scope is closed.
     * @return a scope that restores the incoming feature states
     */
    public static AutoCloseable enableAsyncMaintenance() {
        boolean caffeine = DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.getAndSet(true);
        boolean async = DocumentNodeStoreBuilder.FT_DOCUMENT_CACHE_ASYNC_MAINTENANCE_ENABLED.getAndSet(true);
        return () -> {
            DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.set(caffeine);
            DocumentNodeStoreBuilder.FT_DOCUMENT_CACHE_ASYNC_MAINTENANCE_ENABLED.set(async);
        };
    }
}
