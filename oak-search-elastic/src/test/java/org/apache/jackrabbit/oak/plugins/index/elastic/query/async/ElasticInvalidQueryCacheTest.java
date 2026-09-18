/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.elastic.query.async;

import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ElasticInvalidQueryCacheTest {

    @After
    public void reset() {
        ElasticInvalidQueryCache.FT_OAK_70592_ENABLE.set(false);
        ElasticInvalidQueryCache.clear();
    }

    @Test
    public void disabledByDefault() {
        assertFalse(ElasticInvalidQueryCache.FT_OAK_70592_ENABLE.get());
        ElasticInvalidQueryCache.FT_OAK_70592_ENABLE.set(true);
        ElasticInvalidQueryCache.markInvalid("some-query");
        ElasticInvalidQueryCache.FT_OAK_70592_ENABLE.set(false);

        // toggle is off: even a previously marked entry must not be reported as invalid
        assertFalse(ElasticInvalidQueryCache.isKnownInvalid("some-query"));
    }

    @Test
    public void marksAndReportsKnownInvalidQuery() {
        ElasticInvalidQueryCache.FT_OAK_70592_ENABLE.set(true);

        assertFalse(ElasticInvalidQueryCache.isKnownInvalid("my-index::query-dsl"));

        ElasticInvalidQueryCache.markInvalid("my-index::query-dsl");

        assertTrue(ElasticInvalidQueryCache.isKnownInvalid("my-index::query-dsl"));
        // a different key must not be affected
        assertFalse(ElasticInvalidQueryCache.isKnownInvalid("my-index::other-query-dsl"));
    }

    @Test
    public void nullKeyIsNeverKnownInvalid() {
        ElasticInvalidQueryCache.FT_OAK_70592_ENABLE.set(true);
        ElasticInvalidQueryCache.markInvalid(null);
        assertFalse(ElasticInvalidQueryCache.isKnownInvalid(null));
    }
}
