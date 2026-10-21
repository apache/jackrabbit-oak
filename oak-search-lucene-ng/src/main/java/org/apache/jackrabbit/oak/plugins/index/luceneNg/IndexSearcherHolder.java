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
import org.apache.lucene.facet.sortedset.DefaultSortedSetDocValuesReaderState;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Manages IndexSearcher lifecycle for a Lucene 9 index.
 * Opens the index from the {@link LuceneNgIndexStorage} node state passed in (typically the
 * {@link LuceneNgIndexStorage#STORAGE_NODE_NAME} child under the index definition).
 */
public class IndexSearcherHolder implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(IndexSearcherHolder.class);

    private final String indexName;
    private DirectoryReader reader;
    private IndexSearcher searcher;
    private OakDirectory directory;
    private final ConcurrentMap<String, DefaultSortedSetDocValuesReaderState> facetStateCache =
            new ConcurrentHashMap<>();

    /**
     * @param storageState {@link LuceneNgIndexStorage#storageState(NodeState)} for the index definition
     * @param indexName    the index name, used only for logging/error messages
     */

    public IndexSearcherHolder(NodeState storageState, String indexName) throws IOException {
        this.indexName = indexName;
        this.directory = new OakDirectory(storageState.builder(), indexName, true);
        try {
            this.reader = DirectoryReader.open(directory);
        } catch (IOException e) {
            directory.close();
            throw e;
        }
        this.searcher = new IndexSearcher(reader);
    }

    public DirectoryReader getReader() {
        return reader;
    }

    public IndexSearcher getSearcher() {
        return searcher;
    }

    /**
     * Returns a cached {@link DefaultSortedSetDocValuesReaderState} for {@code fieldName},
     * constructing and caching it on first access. The cache is scoped to this holder instance,
     * so it is discarded when the index is refreshed and a new holder is created.
     *
     * @throws IllegalArgumentException if {@code fieldName} is not a sortedset field in this index
     */
    public DefaultSortedSetDocValuesReaderState getFacetReaderState(String fieldName) throws IOException {
        DefaultSortedSetDocValuesReaderState state = facetStateCache.get(fieldName);
        if (state == null) {
            state = new DefaultSortedSetDocValuesReaderState(reader, fieldName);
            DefaultSortedSetDocValuesReaderState existing = facetStateCache.putIfAbsent(fieldName, state);
            if (existing != null) {
                state = existing;
            }
        }
        return state;
    }

    @Override
    public void close() throws IOException {
        try {
            if (reader != null) {
                reader.close();
            }
        } finally {
            if (directory != null) {
                directory.close();
            }
        }
    }
}
