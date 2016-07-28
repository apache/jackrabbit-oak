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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INDEX_DATA_CHILD_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PERSISTENCE_FILE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PERSISTENCE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PERSISTENCE_PATH;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.SuggestHelper;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.ReadOnlyBuilder;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.suggest.analyzing.AnalyzingInfixSuggester;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

class IndexNode {

    static IndexNode open(String indexPath, NodeState root, NodeState defnNodeState, @Nullable IndexCopier cloner)
            throws IOException {
        Directory directory = null;
        IndexDefinition definition = new IndexDefinition(root, defnNodeState);
        NodeState data = defnNodeState.getChildNode(INDEX_DATA_CHILD_NAME);
        if (data.exists()) {
            directory = new OakDirectory(new ReadOnlyBuilder(defnNodeState), definition, true);
            if (cloner != null) {
                directory = cloner.wrapForRead(indexPath, definition, directory, LuceneIndexConstants.INDEX_DATA_CHILD_NAME);
            }
        } else if (PERSISTENCE_FILE.equalsIgnoreCase(defnNodeState.getString(PERSISTENCE_NAME))) {
            String path = defnNodeState.getString(PERSISTENCE_PATH);
            if (path != null && new File(path).exists()) {
                directory = FSDirectory.open(new File(path));
            }
        }

        if (directory != null) {
            try {
                OakDirectory suggestDirectory = null;
                if (definition.isSuggestEnabled()) {
                    suggestDirectory = new OakDirectory(new ReadOnlyBuilder(defnNodeState), ":suggest-data", definition, true);
                }

                IndexNode index = new IndexNode(PathUtils.getName(indexPath), definition, directory, suggestDirectory);
                directory = null; // closed in Index.close()
                return index;
            } finally {
                if (directory != null) {
                    directory.close();
                }
            }
        }

        return null;
    }

    private final String name;

    private final IndexDefinition definition;

    private final Directory directory;

    private final Directory suggestDirectory;

    private final IndexReader reader;

    private final IndexSearcher searcher;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final AnalyzingInfixSuggester lookup;

    private boolean closed = false;

    IndexNode(String name, IndexDefinition definition, Directory directory, final OakDirectory suggestDirectory)
            throws IOException {
        this.name = name;
        this.definition = definition;
        this.directory = directory;
        this.reader = DirectoryReader.open(directory);
        this.searcher = new IndexSearcher(reader);
        this.suggestDirectory = suggestDirectory;
        if (suggestDirectory != null) {
            this.lookup = SuggestHelper.getLookup(suggestDirectory, definition.getAnalyzer());
        } else {
            this.lookup = null;
        }
    }

    String getName() {
        return name;
    }

    IndexDefinition getDefinition() {
        return definition;
    }

    IndexSearcher getSearcher() {
        return searcher;
    }

    Directory getSuggestDirectory() {
        return suggestDirectory;
    }

    AnalyzingInfixSuggester getLookup() {
        return lookup;
    }

    boolean acquire() {
        lock.readLock().lock();
        if (closed) {
            lock.readLock().unlock();
            return false;
        } else {
            return true;
        }
    }

    void release() {
        lock.readLock().unlock();
    }

    void close() throws IOException {
        lock.writeLock().lock();
        try {
            checkState(!closed);
            closed = true;
        } finally {
            lock.writeLock().unlock();
        }

        try {
            reader.close();
        } finally {
            directory.close();
        }
    }

}
