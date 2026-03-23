/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Compatibility tests for {@link LocalDiffCache}.
 * These assertions stay at the {@link DiffCache} surface and do not reference
 * the underlying cache implementation.
 */
public class LocalDiffCacheCompatibilityTest {

    private static final int CLUSTER_ID = 1;

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private LocalDiffCache buildCache() {
        return new LocalDiffCache(builderProvider.newBuilder()
                .setCacheSegmentCount(1)
                .memoryCacheDistribution(0, 0, 0, 99, 0));
    }

    @Test
    public void getChangesReturnsEmptyStringForMissingPathInsideCachedDiff() {
        LocalDiffCache cache = buildCache();
        RevisionVector from = new RevisionVector(Revision.newRevision(CLUSTER_ID));
        RevisionVector to = new RevisionVector(Revision.newRevision(CLUSTER_ID));

        DiffCache.Entry entry = cache.newEntry(from, to, true);
        entry.append(Path.ROOT, "^\"root\":{}");
        entry.done();

        assertEquals("", cache.getChanges(from, to, Path.fromString("/missing"), null));
    }

    @Test
    public void getChangesDelegatesToLoaderWhenRevisionPairIsNotCached() {
        LocalDiffCache cache = buildCache();
        RevisionVector from = new RevisionVector(Revision.newRevision(CLUSTER_ID));
        RevisionVector to = new RevisionVector(Revision.newRevision(CLUSTER_ID));
        AtomicBoolean loaderCalled = new AtomicBoolean();

        String result = cache.getChanges(from, to, Path.ROOT, () -> {
            loaderCalled.set(true);
            return "^\"loaded\":{}";
        });

        assertTrue(loaderCalled.get());
        assertEquals("^\"loaded\":{}", result);
        assertNull(cache.getChanges(from, to, Path.ROOT, null));
    }
}
