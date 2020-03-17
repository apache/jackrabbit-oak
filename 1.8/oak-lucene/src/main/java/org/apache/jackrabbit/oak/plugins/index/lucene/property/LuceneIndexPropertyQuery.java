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

package org.apache.jackrabbit.oak.plugins.index.lucene.property;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.oak.plugins.index.lucene.IndexNode;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexTracker;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.PathStoredFieldVisitor;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performs simple property=value query against a Lucene index
 */
public class LuceneIndexPropertyQuery implements PropertyQuery {
    private static final Logger log = LoggerFactory.getLogger(LuceneIndexPropertyQuery.class);
    private final IndexTracker tracker;
    private final String indexPath;

    public LuceneIndexPropertyQuery(IndexTracker tracker, String indexPath) {
        this.tracker = tracker;
        this.indexPath = indexPath;
    }

    @Override
    public Iterable<String> getIndexedPaths(String propertyRelativePath, String value) {
        List<String> indexPaths = new ArrayList<>(2);
        IndexNode indexNode = tracker.acquireIndexNode(indexPath);
        if (indexNode != null) {
            try {
                TermQuery query = new TermQuery(new Term(propertyRelativePath, value));
                //By design such query should not result in more than 1 result.
                //So just use 10 as batch size
                TopDocs docs = indexNode.getSearcher().search(query, 10);

                IndexReader reader = indexNode.getSearcher().getIndexReader();
                for (ScoreDoc d : docs.scoreDocs) {
                    PathStoredFieldVisitor visitor = new PathStoredFieldVisitor();
                    reader.document(d.doc, visitor);
                    indexPaths.add(visitor.getPath());
                }
            } catch (IOException e) {
                log.warn("Error occurred while checking index {} for unique value " +
                        "[{}] for [{}]", indexPath,value, propertyRelativePath, e);
            } finally {
                indexNode.release();
            }
        }
        return indexPaths;
    }
}
