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

import org.apache.jackrabbit.oak.plugins.index.luceneNg.directory.OakDirectory;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

/**
 * Manages IndexSearcher lifecycle for a Lucene 9 index.
 * Opens the index from {@code /var/indexing/lucene/<indexName>} in the repository.
 */
public class IndexSearcherHolder implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(IndexSearcherHolder.class);

    private final String indexName;
    private DirectoryReader reader;
    private IndexSearcher searcher;

    /**
     * @param storageState the NodeState at the index storage path
     *                     (e.g. {@code root.getChildNode("var")...getChildNode(indexName)})
     * @param indexName    the index name, used only for logging/error messages
     */
    public IndexSearcherHolder(NodeState storageState, String indexName) throws IOException {
        this.indexName = indexName;
        this.reader = openReader(storageState);
        this.searcher = new IndexSearcher(reader);
    }

    private DirectoryReader openReader(NodeState storageState) throws IOException {
        OakDirectory directory = new OakDirectory(storageState.builder(), indexName, true);
        return DirectoryReader.open(directory);
    }

    public IndexSearcher getSearcher() {
        return searcher;
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }
}
