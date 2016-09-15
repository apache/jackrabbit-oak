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
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.jackrabbit.oak.plugins.index.IndexingContext;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriter;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriterFactory;
import org.apache.jackrabbit.oak.spi.commit.CommitContext;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.index.IndexableField;

public class LocalIndexWriterFactory implements LuceneIndexWriterFactory {
    public static final String COMMIT_PROCESSED_BY_LOCAL_LUCENE_EDITOR = "commitProcessedByLocalLuceneEditor";
    private final IndexingContext indexingContext;
    private final CommitContext commitContext;

    public LocalIndexWriterFactory(IndexingContext indexingContext) {
        this.indexingContext = indexingContext;
        this.commitContext = getCommitContext(indexingContext);
    }

    private LuceneDocumentHolder getDocumentHolder(){
        LuceneDocumentHolder holder = (LuceneDocumentHolder) commitContext.get(LuceneDocumentHolder.NAME);
        if (holder == null) {
            //lazily initialize the holder
            holder = new LuceneDocumentHolder();
            commitContext.set(LuceneDocumentHolder.NAME, holder);
        }
        return holder;
    }

    private static CommitContext getCommitContext(IndexingContext indexingContext) {
        CommitContext commitContext = (CommitContext) indexingContext.getCommitInfo().getInfo().get(CommitContext.NAME);
        return Preconditions.checkNotNull(commitContext, "No commit context found in commit info");
    }

    @Override
    public LuceneIndexWriter newInstance(IndexDefinition definition, NodeBuilder definitionBuilder, boolean reindex) {
        return new LocalIndexWriter(definition);
    }

    private class LocalIndexWriter implements LuceneIndexWriter {
        private final IndexDefinition definition;
        private List<LuceneDoc> docList;

        public LocalIndexWriter(IndexDefinition definition) {
            this.definition = definition;
        }

        @Override
        public void updateDocument(String path, Iterable<? extends IndexableField> doc) throws IOException {
            addLuceneDoc(LuceneDoc.forUpdate(definition.getIndexPathFromConfig(), path, doc));
        }

        @Override
        public void deleteDocuments(String path) throws IOException {
            addLuceneDoc(LuceneDoc.forDelete(definition.getIndexPathFromConfig(), path));
        }

        @Override
        public boolean close(long timestamp) throws IOException {
            //This is used by testcase
            commitContext.set(COMMIT_PROCESSED_BY_LOCAL_LUCENE_EDITOR, Boolean.TRUE);
            //always return false as nothing gets written to the index
            return false;
        }

        private void addLuceneDoc(LuceneDoc luceneDoc) {
            if (docList == null){
                docList = getDocumentHolder().getAsyncIndexedDocList(indexingContext.getIndexPath());
            }
            //TODO [hybrid] checks about the size. If too many drop
            //However for truly sync case hold on
            docList.add(luceneDoc);
        }
    }
}
