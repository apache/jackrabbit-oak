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
package org.apache.jackrabbit.oak.plugins.index.importer;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.IndexDefinitionAudit;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

public class NodeStoreUtilsTest {

    /**
     * Only the commit of the imported index definitions is marked as an import
     * (for the index definition audit), see OAK-12458.
     */
    @Test
    public void onlyTheImportCommitHasTheImporterOrigin() throws Exception {
        List<String> sessionIds = new ArrayList<>();
        MemoryNodeStore store = new MemoryNodeStore() {
            @Override
            public synchronized NodeState merge(@NotNull NodeBuilder builder, @NotNull CommitHook commitHook,
                    @NotNull CommitInfo info) throws CommitFailedException {
                sessionIds.add(info.getSessionId());
                return super.merge(builder, commitHook, info);
            }
        };

        NodeStoreUtils.mergeWithConcurrentCheck(store, store.getRoot().builder());
        NodeStoreUtils.mergeWithConcurrentCheck(store, store.getRoot().builder(), new PropertyIndexEditorProvider(), false);

        assertEquals(List.of(CommitInfo.OAK_UNKNOWN, IndexDefinitionAudit.ORIGIN_INDEX_IMPORTER), sessionIds);
    }
}
