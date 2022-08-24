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

import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

public class CompactionStrategyTest {

    private static final Throwable MARKER_THROWABLE =
            new RuntimeException("We pretend that something went horribly wrong.");

    @Test
    public void compactionIsAbortedOnAnyThrowable() throws IOException {
        MemoryStore store = new MemoryStore();
        CompactionStrategy.Context throwingContext = Mockito.mock(CompactionStrategy.Context.class);
        when(throwingContext.getGCListener()).thenReturn(Mockito.mock(GCListener.class));
        when(throwingContext.getRevisions()).thenReturn(store.getRevisions());
        when(throwingContext.getGCOptions()).thenThrow(MARKER_THROWABLE);

        try {
            final CompactionResult compactionResult = new FullCompactionStrategy().compact(throwingContext);
            assertThat("Compaction should be properly aborted.", compactionResult.isSuccess(), is(false));
        } catch (Throwable e) {
            if (e == MARKER_THROWABLE) {
                fail("The marker throwable was not caught by the CompactionStrategy and therefore not properly aborted.");
            } else {
                throw new IllegalStateException("The test likely needs to be adjusted.", e);
            }
        }
    }
}