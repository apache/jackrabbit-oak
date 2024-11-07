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

import org.apache.jackrabbit.guava.common.collect.Iterables;
import com.mongodb.MongoClient;

import org.apache.jackrabbit.oak.cache.CacheStats;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.jackrabbit.oak.plugins.document.DocumentMK.Builder.DEFAULT_CHILDREN_CACHE_PERCENTAGE;
import static org.apache.jackrabbit.oak.plugins.document.DocumentMK.Builder.DEFAULT_DIFF_CACHE_PERCENTAGE;
import static org.apache.jackrabbit.oak.plugins.document.DocumentMK.Builder.DEFAULT_NODE_CACHE_PERCENTAGE;
import static org.apache.jackrabbit.oak.plugins.document.DocumentMK.Builder.DEFAULT_PREV_DOC_CACHE_PERCENTAGE;
import static org.apache.jackrabbit.oak.plugins.document.DocumentMK.Builder.DEFAULT_PREV_NO_PROP_CACHE_PERCENTAGE;
import static org.apache.jackrabbit.oak.plugins.document.util.UtilsTest.createFeature;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;

@RunWith(Parameterized.class)
public class DocumentMKBuilderTest extends AbstractMongoConnectionTest {

    private static final long CACHE_SIZE = 8 * 1024 * 1024;
    private static final long PREV_DOC_CACHE_SIZE = cacheSize(DEFAULT_PREV_DOC_CACHE_PERCENTAGE);
    private static final long DOC_CACHE_SIZE_DEFAULT = CACHE_SIZE -
            cacheSize(DEFAULT_CHILDREN_CACHE_PERCENTAGE) -
            cacheSize(DEFAULT_DIFF_CACHE_PERCENTAGE) -
            cacheSize(DEFAULT_NODE_CACHE_PERCENTAGE) -
            cacheSize(DEFAULT_PREV_DOC_CACHE_PERCENTAGE);
    private static final long DOC_CACHE_SIZE_PREV_NO_PROP_ENABLED = DOC_CACHE_SIZE_DEFAULT -
            cacheSize(DEFAULT_PREV_NO_PROP_CACHE_PERCENTAGE);

    @Parameterized.Parameters(name="{index}: prevNoPropEnabled : {0}")
    public static java.util.Collection<Boolean> params() {
        return Arrays.asList(true,  false);
    }

    boolean prevNoPropEnabled;

    long expectedDocCacheSize;

    public DocumentMKBuilderTest(boolean prevNoPropEnabled) {
        this.prevNoPropEnabled = prevNoPropEnabled;
        if (prevNoPropEnabled) {
            expectedDocCacheSize = DOC_CACHE_SIZE_PREV_NO_PROP_ENABLED;
        } else {
            expectedDocCacheSize = DOC_CACHE_SIZE_DEFAULT;
        }
    }

    @Override
    protected DocumentMK.Builder newBuilder(MongoClient client, String dbName) throws Exception {
        return super.newBuilder(client, dbName).memoryCacheSize(CACHE_SIZE)
                .setPrevNoPropCacheFeature(createFeature(prevNoPropEnabled));
    }

    @Test
    public void lazyInit() throws Exception {
        Iterable<CacheStats> cacheStats = mk.getDocumentStore().getCacheStats();
        assertNotNull(cacheStats);
        assertEquals(2, Iterables.size(cacheStats));
        CacheStats docCacheStats = Iterables.get(cacheStats, 0);
        CacheStats prevDocCacheStats = Iterables.get(cacheStats, 1);
        assertEquals("Document-Documents", docCacheStats.getName());
        assertEquals("Document-PrevDocuments", prevDocCacheStats.getName());
        assertEquals(expectedDocCacheSize, docCacheStats.getMaxTotalWeight());
        assertEquals(PREV_DOC_CACHE_SIZE, prevDocCacheStats.getMaxTotalWeight());
    }

    private static long cacheSize(int percentage) {
        return CACHE_SIZE * percentage / 100;
    }
}
