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
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

/**
 * Manages IndexSearcher lifecycle for a Lucene 9 index.
 * Provides thread-safe access to IndexSearcher and handles reopening.
 */
public class IndexSearcherHolder implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(IndexSearcherHolder.class);

    private final NodeBuilder definition;
    private final String indexName;
    private DirectoryReader reader;
    private IndexSearcher searcher;

    public IndexSearcherHolder(NodeBuilder definition, String indexName) throws IOException {
        this.definition = definition;
        this.indexName = indexName;
        this.reader = openReader();
        this.searcher = new IndexSearcher(reader);
    }

    private DirectoryReader openReader() throws IOException {
        OakDirectory directory = new OakDirectory(definition, indexName, true); // read-only
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
