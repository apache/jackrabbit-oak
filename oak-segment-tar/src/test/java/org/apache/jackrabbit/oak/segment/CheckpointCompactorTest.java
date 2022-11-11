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

import static org.apache.jackrabbit.oak.segment.DefaultSegmentWriterBuilder.defaultSegmentWriterBuilder;
import static org.apache.jackrabbit.oak.segment.CompactorTestUtils.SimpleCompactorFactory;

@RunWith(Parameterized.class)
public class CheckpointCompactorTest extends AbstractCompactorTest {
    public CheckpointCompactorTest(@NotNull SimpleCompactorFactory compactorFactory) {
        super(compactorFactory);
    }

    @Override
    protected CheckpointCompactor createCompactor(
            @NotNull FileStore fileStore,
            @NotNull GCIncrement increment,
            @NotNull GCNodeWriteMonitor compactionMonitor
    ) {
        SegmentWriterFactory writerFactory = generation ->  defaultSegmentWriterBuilder("c")
                .withGeneration(generation)
                .build(fileStore);
        CompactionWriter compactionWriter = new CompactionWriter(fileStore.getReader(), fileStore.getBlobStore(), increment, writerFactory);
        return new CheckpointCompactor(GCMonitor.EMPTY, compactionWriter, compactionMonitor);
    }
}
