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

package org.apache.jackrabbit.oak.index.indexer.document;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;

import static com.google.common.base.Preconditions.checkNotNull;

public class CompositeIndexer implements NodeStateIndexer {
    private final List<NodeStateIndexer> indexers;

    public CompositeIndexer(List<NodeStateIndexer> indexers) {
        this.indexers = checkNotNull(indexers);
    }

    public boolean isEmpty() {
        return indexers.isEmpty();
    }

    @Override
    public boolean shouldInclude(String path) {
        return indexers.stream().anyMatch(indexer -> indexer.shouldInclude(path));
    }

    @Override
    public boolean shouldInclude(NodeDocument doc) {
        return indexers.stream().anyMatch(indexer -> indexer.shouldInclude(doc));
    }

    @Override
    public void index(NodeStateEntry entry) throws IOException, CommitFailedException {
        for (NodeStateIndexer indexer : indexers) {
            indexer.index(entry);
        }
    }

    @Override
    public boolean indexesRelativeNodes() {
        for (NodeStateIndexer indexer : indexers) {
            if (indexer.indexesRelativeNodes()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Set<String> getRelativeIndexedNodeNames() {
        Set<String> result = new HashSet<>();
        for (NodeStateIndexer indexer : indexers) {
            result.addAll(indexer.getRelativeIndexedNodeNames());
        }
        return result;
    }

    @Override
    public void close() throws IOException {

    }
}
