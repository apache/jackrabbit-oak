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
package org.apache.jackrabbit.oak.segment.file;

import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.spi.persistence.GCGeneration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;

/**
 * Verifies that {@link SynchronizedGarbageCollectionStrategy} correctly
 * delegates all method calls to the wrapped {@link GarbageCollectionStrategy}.
 */
public class SynchronizedGarbageCollectionStrategyTest {

    private GarbageCollectionStrategy delegate;
    private SynchronizedGarbageCollectionStrategy strategy;

    @Before
    public void setUp() {
        delegate = Mockito.mock(GarbageCollectionStrategy.class);
        strategy = new SynchronizedGarbageCollectionStrategy(delegate);
    }

    @Test
    public void testCleanupWithCompactionResultDelegatesToWrappedStrategy() throws IOException {
        GarbageCollectionStrategy.Context context = Mockito.mock(GarbageCollectionStrategy.Context.class);
        CompactionResult compactionResult = CompactionResult.succeeded(
                SegmentGCOptions.GCType.FULL,
                GCGeneration.NULL,
                SegmentGCOptions.defaultGCOptions(),
                RecordId.NULL,
                0);
        Mockito.when(delegate.cleanup(context, compactionResult)).thenReturn(Collections.emptyList());

        strategy.cleanup(context, compactionResult);

        Mockito.verify(delegate).cleanup(context, compactionResult);
    }

    @Test
    public void testCleanupDelegatesToWrappedStrategy() throws IOException {
        GarbageCollectionStrategy.Context context = Mockito.mock(GarbageCollectionStrategy.Context.class);
        Mockito.when(delegate.cleanup(context)).thenReturn(Collections.emptyList());

        strategy.cleanup(context);

        Mockito.verify(delegate).cleanup(context);
    }

    @Test
    public void testCompactFullDelegatesToWrappedStrategy() throws IOException {
        GarbageCollectionStrategy.Context context = Mockito.mock(GarbageCollectionStrategy.Context.class);
        CompactionResult expected = CompactionResult.aborted(GCGeneration.NULL, 0);
        Mockito.when(delegate.compactFull(context)).thenReturn(expected);

        CompactionResult actual = strategy.compactFull(context);

        Mockito.verify(delegate).compactFull(context);
        org.junit.Assert.assertSame(expected, actual);
    }

    @Test
    public void testCompactTailDelegatesToWrappedStrategy() throws IOException {
        GarbageCollectionStrategy.Context context = Mockito.mock(GarbageCollectionStrategy.Context.class);
        CompactionResult expected = CompactionResult.aborted(GCGeneration.NULL, 0);
        Mockito.when(delegate.compactTail(context)).thenReturn(expected);

        CompactionResult actual = strategy.compactTail(context);

        Mockito.verify(delegate).compactTail(context);
        org.junit.Assert.assertSame(expected, actual);
    }
}
