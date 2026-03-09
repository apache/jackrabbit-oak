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

import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;

/**
 * Represents a Lucene 9 index with its definition and searcher.
 * Wraps the index definition and provides access to the Lucene index.
 */
public class LuceneNgIndexNode {
    private final String indexPath;
    private final LuceneNgIndexDefinition definition;

    /**
     * Creates a new index node.
     *
     * @param indexPath the path to the index (e.g., "/oak:index/myIndex")
     * @param root the root node state
     * @param indexState the index definition node state
     */
    public LuceneNgIndexNode(@NotNull String indexPath,
                           @NotNull NodeState root,
                           @NotNull NodeState indexState) {
        this.indexPath = indexPath;
        this.definition = new LuceneNgIndexDefinition(root, indexState, indexPath);
    }

    /**
     * Gets the index path.
     *
     * @return the index path
     */
    public String getIndexPath() {
        return indexPath;
    }

    /**
     * Gets the index definition.
     *
     * @return the index definition
     */
    public LuceneNgIndexDefinition getDefinition() {
        return definition;
    }
}
