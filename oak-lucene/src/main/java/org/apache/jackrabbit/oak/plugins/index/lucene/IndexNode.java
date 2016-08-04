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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


import com.google.common.base.Preconditions;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.LuceneIndexReader;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.LuceneIndexReaderFactory;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.suggest.analyzing.AnalyzingInfixSuggester;
import org.apache.lucene.store.Directory;

class IndexNode {

    static IndexNode open(String indexPath, NodeState root, NodeState defnNodeState, LuceneIndexReaderFactory readerFactory)
            throws IOException {
        IndexDefinition definition = new IndexDefinition(root, defnNodeState);
        List<LuceneIndexReader> readers = readerFactory.createReaders(definition, defnNodeState, indexPath);
        if (!readers.isEmpty()){
            return new IndexNode(PathUtils.getName(indexPath), definition, readers);
        }
        return null;
    }

    private final List<LuceneIndexReader> readers;

    private final String name;

    private final IndexDefinition definition;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final IndexSearcher indexSearcher;

    private boolean closed = false;

    IndexNode(String name, IndexDefinition definition, List<LuceneIndexReader> readers)
            throws IOException {
        checkArgument(!readers.isEmpty());
        this.name = name;
        this.definition = definition;
        this.readers = readers;
        this.indexSearcher = new IndexSearcher(createReader(readers));
    }

    String getName() {
        return name;
    }

    IndexDefinition getDefinition() {
        return definition;
    }

    IndexSearcher getSearcher() {
        return indexSearcher;
    }

    Directory getSuggestDirectory() {
        return getDefaultReader().getSuggestDirectory();
    }

    AnalyzingInfixSuggester getLookup() {
        return getDefaultReader().getLookup();
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

       for (LuceneIndexReader reader : readers){
           reader.close();
       }
    }

    private LuceneIndexReader getDefaultReader(){
        //TODO This is still required to support Suggester, Spellcheck etc OAK-4643
        return readers.get(0);
    }

    private IndexReader createReader(List<LuceneIndexReader> readers) {
        if (readers.size() == 1){
            return readers.get(0).getReader();
        }
        IndexReader[] readerArr = new IndexReader[readers.size()];
        for (int i = 0; i < readerArr.length; i++) {
            readerArr[i] = readers.get(i).getReader();
        }
        return new MultiReader(readerArr, true);
    }
}
