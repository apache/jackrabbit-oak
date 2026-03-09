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

import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Cursor over Lucene 9 search results.
 */
public class LuceneNgCursor implements Cursor {

    private static final Logger LOG = LoggerFactory.getLogger(LuceneNgCursor.class);

    private final TopDocs docs;
    private final IndexSearcher searcher;
    private final IndexSearcherHolder holder;
    private int currentIndex = 0;

    public LuceneNgCursor(TopDocs docs, IndexSearcher searcher, IndexSearcherHolder holder) {
        this.docs = docs;
        this.searcher = searcher;
        this.holder = holder;
    }

    @Override
    public boolean hasNext() {
        return currentIndex < docs.scoreDocs.length;
    }

    @Override
    public IndexRow next() {
        ScoreDoc scoreDoc = docs.scoreDocs[currentIndex++];

        try {
            Document doc = searcher.doc(scoreDoc.doc);
            String path = doc.get("path");

            return new LuceneNgIndexRow(path, scoreDoc.score);

        } catch (IOException e) {
            LOG.error("Error reading document", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getSize(org.apache.jackrabbit.oak.api.Result.SizePrecision precision, long max) {
        return docs.totalHits.value;
    }
}
