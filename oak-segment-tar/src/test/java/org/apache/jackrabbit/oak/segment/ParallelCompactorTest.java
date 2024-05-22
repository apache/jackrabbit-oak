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
 *
 */

package org.apache.jackrabbit.oak.segment;

import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.GCIncrement;
import org.apache.jackrabbit.oak.segment.file.GCNodeWriteMonitor;
import org.apache.jackrabbit.oak.segment.file.CompactionWriter;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.jetbrains.annotations.NotNull;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;

import static org.apache.jackrabbit.oak.segment.DefaultSegmentWriterBuilder.defaultSegmentWriterBuilder;
import static org.apache.jackrabbit.oak.segment.CompactorTestUtils.SimpleCompactorFactory;

@RunWith(Parameterized.class)
public class ParallelCompactorTest extends AbstractCompactorTest {

    private final int concurrency;

    @Parameterized.Parameters
    public static List<Object[]> parameters() {
        Integer[] concurrencyLevels = {1, 2, 4, 8, 16};

        List<Object[]> parameters = new ArrayList<>();
        for (SimpleCompactorFactory factory : AbstractCompactorExternalBlobTest.compactorFactories()) {
            for (int concurrency : concurrencyLevels) {
                parameters.add(new Object[]{factory, concurrency});
            }
        }
        return parameters;
    }

    public ParallelCompactorTest(@NotNull SimpleCompactorFactory compactorFactory, int concurrency) {
        super(compactorFactory);
        this.concurrency = concurrency;
    }

    @Override
    protected ParallelCompactor createCompactor(
            @NotNull FileStore fileStore,
            @NotNull GCIncrement increment,
            @NotNull GCNodeWriteMonitor compactionMonitor
    ) {
        SegmentWriterFactory writerFactory = generation -> defaultSegmentWriterBuilder("c")
                .withGeneration(generation)
                .withWriterPool(SegmentBufferWriterPool.PoolType.THREAD_SPECIFIC)
                .build(fileStore);
        CompactionWriter compactionWriter = new CompactionWriter(fileStore.getReader(), fileStore.getBlobStore(), increment, writerFactory);
        return new ParallelCompactor(GCMonitor.EMPTY, compactionWriter, compactionMonitor, concurrency);
    }
}
