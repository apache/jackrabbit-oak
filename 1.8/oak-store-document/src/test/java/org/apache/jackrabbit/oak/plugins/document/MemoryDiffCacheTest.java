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

import java.util.UUID;

import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class MemoryDiffCacheTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    @Test
    public void limit() throws Exception {
        DiffCache cache = new MemoryDiffCache(builderProvider.newBuilder()
                .setCacheSegmentCount(1)
                .memoryCacheDistribution(0, 0, 0, 99));
        RevisionVector from = new RevisionVector(Revision.newRevision(1));
        RevisionVector to = new RevisionVector(Revision.newRevision(1));
        DiffCache.Entry entry = cache.newEntry(from, to, false);
        entry.append("/", "^\"foo\":{}");
        entry.append("/foo", changes(MemoryDiffCache.CACHE_VALUE_LIMIT));
        entry.done();
        assertNotNull(cache.getChanges(from, to, "/", null));
        assertNull(cache.getChanges(from, to, "/foo", null));
    }

    private static String changes(int minLength) {
        StringBuilder sb = new StringBuilder();
        while (sb.length() < minLength) {
            sb.append("^\"").append(UUID.randomUUID()).append("\":{}");
        }
        return sb.toString();
    }
}
