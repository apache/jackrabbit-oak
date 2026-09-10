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
package org.apache.jackrabbit.oak.plugins.index.luceneNg.directory;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertNotEquals;

import java.io.IOException;

import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.junit.Test;

public class OakDirectoryTempFileNamingTest {

    /** Freezes the naming seed so two calls are guaranteed to observe the same
     *  value — this makes the collision reproducible on every run, rather than
     *  depending on System.nanoTime() happening to repeat. */
    private static class FrozenSeedDirectory extends OakDirectory {
        FrozenSeedDirectory(NodeBuilder storageBuilder, String indexName, boolean readOnly) {
            super(storageBuilder, indexName, readOnly);
        }

        @Override
        long nextTempFileId() {
            return 42L;
        }
    }

    @Test
    public void tempFileNamesAreUniqueEvenWhenTheNamingSeedDoesNotChange() throws IOException {
        NodeBuilder builder = EMPTY_NODE.builder();
        OakDirectory directory = new FrozenSeedDirectory(builder, "test-index", false);

        String name1;
        try (IndexOutput out1 = directory.createTempOutput("tmp", "seg", IOContext.DEFAULT)) {
            name1 = out1.getName();
        }
        String name2;
        try (IndexOutput out2 = directory.createTempOutput("tmp", "seg", IOContext.DEFAULT)) {
            name2 = out2.getName();
        }

        assertNotEquals("two temp files created while the naming seed is frozen "
                + "must still get distinct names — uniqueness must not depend on the seed changing",
                name1, name2);
        directory.close();
    }
}
