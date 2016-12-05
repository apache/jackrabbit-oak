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

package org.apache.jackrabbit.oak.plugins.index.lucene.hybrid;

import java.io.IOException;

import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriter;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriterFactory;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.index.IndexableField;

public class LocalIndexWriterFactory implements LuceneIndexWriterFactory {
    private final LuceneDocumentHolder documentHolder;
    private final String indexPath;

    public LocalIndexWriterFactory(LuceneDocumentHolder documentHolder, String indexPath) {
        this.documentHolder = documentHolder;
        this.indexPath = indexPath;
    }

    @Override
    public LuceneIndexWriter newInstance(IndexDefinition definition, NodeBuilder definitionBuilder, boolean reindex) {
        return new LocalIndexWriter(definition);
    }

    private class LocalIndexWriter implements LuceneIndexWriter {
        private final IndexDefinition definition;

        public LocalIndexWriter(IndexDefinition definition) {
            this.definition = definition;
        }

        @Override
        public void updateDocument(String path, Iterable<? extends IndexableField> doc) throws IOException {
            addLuceneDoc(LuceneDoc.forUpdate(definition.getIndexPath(), path, doc));
        }

        @Override
        public void deleteDocuments(String path) throws IOException {
            //Hybrid index logic drops the deletes. So no use to
            //add them to the list
            //addLuceneDoc(LuceneDoc.forDelete(definition.getIndexPathFromConfig(), path));
        }

        @Override
        public boolean close(long timestamp) throws IOException {
            documentHolder.done(indexPath);
            //always return false as nothing gets written to the index
            return false;
        }

        private void addLuceneDoc(LuceneDoc luceneDoc) {
            documentHolder.add(definition.isSyncIndexingEnabled(), luceneDoc);
        }
    }
}
