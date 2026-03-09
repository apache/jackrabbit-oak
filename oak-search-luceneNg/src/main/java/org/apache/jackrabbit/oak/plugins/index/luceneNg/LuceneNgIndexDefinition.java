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

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;

/**
 * Index definition for Lucene 9 indexes.
 * Extends the base IndexDefinition with Lucene 9 specific configuration.
 */
public class LuceneNgIndexDefinition extends IndexDefinition {

    /**
     * Creates a new Lucene 9 index definition.
     *
     * @param root the root node state
     * @param defn the index definition node state
     * @param indexPath the path to this index
     */
    public LuceneNgIndexDefinition(@NotNull NodeState root,
                                  @NotNull NodeState defn,
                                  @NotNull String indexPath) {
        super(root, defn, indexPath);
    }

    @Override
    protected String getDefaultFunctionName() {
        return LuceneNgIndexConstants.TYPE_LUCENE9;
    }

    /**
     * Gets the index name (last segment of index path).
     *
     * @return the index name
     */
    public String getIndexName() {
        return PathUtils.getName(getIndexPath());
    }

    /**
     * Gets the storage path for this index in /var.
     *
     * @return the storage path (e.g., /var/indexing/lucene/myIndex)
     */
    public String getStoragePath() {
        return LuceneNgIndexConstants.VAR_INDEXING_BASE_PATH + "/" + getIndexName();
    }
}
