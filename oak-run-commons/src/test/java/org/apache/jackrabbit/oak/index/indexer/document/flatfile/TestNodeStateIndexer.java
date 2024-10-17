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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateIndexer;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;

import java.io.IOException;
import java.util.List;
import java.util.Set;

class TestNodeStateIndexer implements NodeStateIndexer {

    private final String name;
    private final List<String> includedPaths;

    public TestNodeStateIndexer(List<String> includedPaths) {
        this("test-index", includedPaths);
    }

    public TestNodeStateIndexer(String name, List<String> includedPaths) {
        this.name = name;
        this.includedPaths = includedPaths;
    }

    @Override
    public void close() {
    }

    @Override
    public boolean shouldInclude(String path) {
        return includedPaths.stream().anyMatch(path::startsWith);
    }

    @Override
    public boolean shouldInclude(NodeDocument doc) {
        return false;
    }

    @Override
    public boolean index(NodeStateEntry entry) throws IOException, CommitFailedException {
        return false;
    }

    @Override
    public boolean indexesRelativeNodes() {
        return false;
    }

    @Override
    public Set<String> getRelativeIndexedNodeNames() {
        return null;
    }

    @Override
    public String getIndexName() {
        return name;
    }
}