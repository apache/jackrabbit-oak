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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Many methods in this class call themselves recursively, and are susceptible to infinite recursion if a composite
 * indexer contains itself, directly or indirectly. In this case, the methods will throw a StackOverflowException.
 */
public class CompositeIndexer implements NodeStateIndexer {

    private final static Logger LOG = LoggerFactory.getLogger(CompositeIndexer.class);

    private final List<NodeStateIndexer> indexers;

    public CompositeIndexer(List<NodeStateIndexer> indexers) {
        this.indexers = requireNonNull(indexers);
    }

    public boolean isEmpty() {
        return indexers.isEmpty();
    }

    @Override
    public void onIndexingStarting() {
        for (NodeStateIndexer indexer : indexers) {
            indexer.onIndexingStarting();
        }
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
    public boolean index(NodeStateEntry entry) throws IOException, CommitFailedException {
        boolean result = false;
        for (NodeStateIndexer indexer : indexers) {
            result |= indexer.index(entry);
        }
        return result;
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
    public String getIndexName() {
        return indexers.stream().map(NodeStateIndexer::getIndexName).collect(Collectors.joining(",", "CompositeIndexer[", "]"));
    }

    @Override
    public void close() throws IOException {
        for (NodeStateIndexer indexer : indexers) {
            try {
                indexer.close();
            } catch (IOException e) {
                LOG.warn("Error closing indexer {}. Suppressing exception.", indexer, e);
            }
        }
    }

    public List<NodeStateIndexer> getIndexers() {
        return indexers;
    }
}
