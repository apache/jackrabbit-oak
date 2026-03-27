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
package org.apache.jackrabbit.oak.plugins.index.luceneNg;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;

/**
 * Physical location of Lucene 9 index files under the Oak repository.
 * <p>
 * Segments and supporting files live in a single visible child of the index
 * definition node (e.g. {@code /oak:index/myIndex/lucene9}).
 */
public final class LuceneNgIndexStorage {

    /**
     * Name of the JCR child node under the index definition that holds Lucene files.
     */
    public static final String STORAGE_NODE_NAME = "lucene9";

    private LuceneNgIndexStorage() {
    }

    /**
     * Absolute repository path to the storage node for the given index definition path.
     *
     * @param indexDefinitionPath path to the index definition (e.g. {@code /oak:index/myIndex})
     * @return path to the Lucene storage root (e.g. {@code /oak:index/myIndex/lucene9})
     */
    @NotNull
    public static String storagePath(@NotNull String indexDefinitionPath) {
        return PathUtils.concat(indexDefinitionPath, STORAGE_NODE_NAME);
    }

    /**
     * Node state of the Lucene storage under an index definition snapshot.
     */
    @NotNull
    public static NodeState storageState(@NotNull NodeState indexDefinitionState) {
        return indexDefinitionState.getChildNode(STORAGE_NODE_NAME);
    }

    /**
     * Returns the storage {@link NodeBuilder}, creating the child and default primary type if needed.
     * Callers use this as the root {@link org.apache.jackrabbit.oak.plugins.index.luceneNg.directory.OakDirectory}.
     */
    @NotNull
    public static NodeBuilder getOrCreateStorageBuilder(@NotNull NodeBuilder indexDefinitionBuilder) {
        NodeBuilder storage = indexDefinitionBuilder.child(STORAGE_NODE_NAME);
        if (!storage.hasProperty(JcrConstants.JCR_PRIMARYTYPE)) {
            storage.setProperty(JcrConstants.JCR_PRIMARYTYPE, "oak:Unstructured", Type.NAME);
        }
        return storage;
    }
}
