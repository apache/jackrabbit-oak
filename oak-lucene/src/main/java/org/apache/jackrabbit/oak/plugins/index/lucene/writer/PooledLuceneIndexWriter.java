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
package org.apache.jackrabbit.oak.plugins.index.lucene.writer;

import org.apache.lucene.index.IndexableField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Index writer that delegates write operations to an IndexWriterPool.
 */
public class PooledLuceneIndexWriter implements LuceneIndexWriter {
    private final static Logger LOG = LoggerFactory.getLogger(PooledLuceneIndexWriter.class);

    private final String indexName;
    private final LuceneIndexWriter delegateWriter;
    private final IndexWriterPool writerPool;

    private long updateCount = 0;
    private long deleteCount = 0;

    public PooledLuceneIndexWriter(IndexWriterPool writerPool, LuceneIndexWriter delegateWriter, String indexName) {
        this.writerPool = writerPool;
        this.delegateWriter = delegateWriter;
        this.indexName = indexName;
        LOG.debug("[{}] Created writer", indexName);
    }

    @Override
    public void updateDocument(String path, Iterable<? extends IndexableField> doc) throws IOException {
        writerPool.updateDocument(delegateWriter, path, doc);
        updateCount++;
    }

    @Override
    public void deleteDocuments(String path) throws IOException {
        writerPool.deleteDocuments(delegateWriter, path);
        deleteCount++;
    }

    @Override
    public boolean close(long timestamp) throws IOException {
        LOG.debug("[{}] Shutting down writer", indexName);
        return writerPool.closeWriter(delegateWriter, timestamp);
    }

    public long getUpdateCount() {
        return updateCount;
    }

    public long getDeleteCount() {
        return deleteCount;
    }

    public String formatStatistics() {
        return "PooledLuceneIndexWriter(" + indexName + ")[" +
                "updates: " + updateCount + ", " +
                "deletes: " + deleteCount +
                "]";
    }
}
