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
package org.apache.jackrabbit.oak.segment.file;

import org.apache.jackrabbit.oak.segment.SegmentCache;
import org.apache.jackrabbit.oak.segment.spi.persistence.GCGeneration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests for {@link AbstractCompactionStrategy#notifyCompactionSucceeded}.
 *
 * <p>Only full compaction success calls this hook. Partial success skips the cache clear to avoid
 * evicting newly committed segments.
 *
 * <p>Verifies that the segment cache is cleared when {@link SegmentCache#FT_OAK_12216_ENABLE} is
 * enabled (default), and that no clear occurs when the toggle is disabled.
 */
public class CompactionSegmentCacheClearTest {

    @Before
    public void resetToggleBefore() {
        SegmentCache.FT_OAK_12216_ENABLE.set(true);
    }

    @After
    public void restoreToggle() {
        SegmentCache.FT_OAK_12216_ENABLE.set(true);
    }

    @Test
    public void clearsSegmentCacheByDefaultOnCompactionSuccess() {
        SegmentCache cache = Mockito.mock(SegmentCache.class);
        GCListener gcListener = Mockito.mock(GCListener.class);
        CompactionStrategy.Context context = Mockito.mock(CompactionStrategy.Context.class);
        Mockito.when(context.getSegmentCache()).thenReturn(cache);
        Mockito.when(context.getGCListener()).thenReturn(gcListener);

        AbstractCompactionStrategy.notifyCompactionSucceeded(context, Mockito.mock(GCGeneration.class));

        Mockito.verify(cache).clear();
        Mockito.verify(gcListener).compactionSucceeded(Mockito.any());
    }

    @Test
    public void skipsClearWhenToggleDisabled() {
        SegmentCache.FT_OAK_12216_ENABLE.set(false);

        SegmentCache cache = Mockito.mock(SegmentCache.class);
        GCListener gcListener = Mockito.mock(GCListener.class);
        CompactionStrategy.Context context = Mockito.mock(CompactionStrategy.Context.class);
        Mockito.when(context.getSegmentCache()).thenReturn(cache);
        Mockito.when(context.getGCListener()).thenReturn(gcListener);

        AbstractCompactionStrategy.notifyCompactionSucceeded(context, Mockito.mock(GCGeneration.class));

        Mockito.verify(cache, Mockito.never()).clear();
        Mockito.verify(gcListener).compactionSucceeded(Mockito.any());
    }
}
