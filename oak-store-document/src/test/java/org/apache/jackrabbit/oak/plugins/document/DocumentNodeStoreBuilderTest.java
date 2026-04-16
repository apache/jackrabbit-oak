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

import java.lang.reflect.Method;

import org.apache.jackrabbit.oak.cache.api.CacheStatsAdapter;
import org.apache.jackrabbit.oak.plugins.document.cache.NodeDocumentCache;
import org.apache.jackrabbit.oak.plugins.document.locks.StripedNodeDocumentLocks;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link DocumentNodeStoreBuilder} cache configuration.
 * These assertions intentionally avoid third-party cache types so the same
 * tests can run across cache implementation changes.
 */
public class DocumentNodeStoreBuilderTest {

    @Test
    public void buildNodeDocumentCacheReturnsNonNull() {
        // Verify the builder can construct the node-document cache with the default
        // in-memory configuration and a plain in-memory document store.
        DocumentStore store = new MemoryDocumentStore();
        NodeDocumentCache cache = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                .buildNodeDocumentCache(store, new StripedNodeDocumentLocks());
        Assert.assertNotNull(cache);
    }

    @Test
    public void buildNodeDocumentCacheStatsAreNonEmpty() {
        // The builder wires cache stats as part of construction, so the returned
        // cache should already expose at least one stats entry.
        DocumentStore store = new MemoryDocumentStore();
        NodeDocumentCache cache = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                .buildNodeDocumentCache(store, new StripedNodeDocumentLocks());
        Iterable<CacheStatsAdapter> stats = cache.getCacheStats();
        Assert.assertNotNull(stats);
        Assert.assertTrue(stats.iterator().hasNext());
    }

    @Test
    public void buildNodeDocumentCacheIsUsable() throws Exception {
        // Round-trip a document through the built cache so this test checks
        // observable put/get behavior instead of just construction.
        DocumentStore docStore = new MemoryDocumentStore();
        NodeDocumentCache cache = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                .buildNodeDocumentCache(docStore, new StripedNodeDocumentLocks());
        // put a document and verify it can be retrieved
        NodeDocument doc = new NodeDocument(docStore, 1L);
        doc.put(Document.ID, "test-id");
        doc.put(Document.MOD_COUNT, 1L);
        cache.put(doc);
        NodeDocument result = cache.getIfPresent("test-id");
        Assert.assertNotNull(result);
        Assert.assertEquals(doc.getModCount(), result.getModCount());
    }

    @Test
    public void buildNodeDocumentCacheWithZeroMemoryDistributionStillReturnsUsableCache() throws Exception {
        DocumentStore docStore = new MemoryDocumentStore();
        // This verifies builder behavior when all memory cache buckets are disabled.
        // It does not assert cache-capacity semantics.
        NodeDocumentCache cache = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                .memoryCacheDistribution(0, 0, 0, 0, 0)
                .buildNodeDocumentCache(docStore, new StripedNodeDocumentLocks());
        NodeDocument doc = new NodeDocument(docStore, 2L);
        doc.put(Document.ID, "zero-distribution-id");
        doc.put(Document.MOD_COUNT, 2L);
        cache.put(doc);

        NodeDocument result = cache.getIfPresent("zero-distribution-id");
        Assert.assertNotNull(result);
        Assert.assertEquals(doc.getModCount(), result.getModCount());
    }

    @Test
    public void buildDocumentCacheStoresAndRetrievesDocuments() throws Exception {
        // buildDocumentCache() currently returns an implementation-specific cache type,
        // so this test uses reflection and checks only the observable put/get contract.
        DocumentStore store = new MemoryDocumentStore();
        Object cache = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder().buildDocumentCache(store);
        NodeDocument document = new NodeDocument(store, 1L);
        StringValue key = StringValue.fromString("document-cache-id");
        document.put(Document.ID, key.toString());
        document.put(Document.MOD_COUNT, 7L);

        invoke(cache, "put", Object.class, Object.class, key, document);
        Object cached = invoke(cache, "getIfPresent", Object.class, key);

        Assert.assertNotNull(cached);
        Assert.assertTrue(cached instanceof NodeDocument);
        Assert.assertEquals(document.getModCount(), ((NodeDocument) cached).getModCount());
    }

    private static Object invoke(Object target, String methodName, Class<?> parameterType, Object argument)
            throws Exception {
        Method method = target.getClass().getMethod(methodName, parameterType);
        return method.invoke(target, argument);
    }

    private static Object invoke(Object target,
                                 String methodName,
                                 Class<?> firstType,
                                 Class<?> secondType,
                                 Object firstArgument,
                                 Object secondArgument) throws Exception {
        Method method = target.getClass().getMethod(methodName, firstType, secondType);
        return method.invoke(target, firstArgument, secondArgument);
    }
}
