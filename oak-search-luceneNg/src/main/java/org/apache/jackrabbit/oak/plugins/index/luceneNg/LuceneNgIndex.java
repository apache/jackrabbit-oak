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

import org.apache.jackrabbit.oak.plugins.index.cursor.Cursors;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;

/**
 * Lucene 9 query index implementation.
 * Executes queries against Lucene 9 indexes.
 */
public class LuceneNgIndex implements QueryIndex {

    private static final Logger LOG = LoggerFactory.getLogger(LuceneNgIndex.class);

    private final LuceneNgIndexTracker tracker;
    private final String indexPath;

    public LuceneNgIndex(LuceneNgIndexTracker tracker, String indexPath) {
        this.tracker = tracker;
        this.indexPath = indexPath;
    }

    @Override
    public double getMinimumCost() {
        return 2.0; // Better than traversal (1000+) but not as good as unique lookup (1.0)
    }

    @Override
    public double getCost(Filter filter, NodeState rootState) {
        // Simple cost estimation for now
        FullTextExpression ft = filter.getFullTextConstraint();
        if (ft == null) {
            return Double.POSITIVE_INFINITY; // Can't handle non-fulltext queries yet
        }

        // Assume reasonable cost for fulltext queries
        return 100.0;
    }

    @Override
    public Cursor query(Filter filter, NodeState rootState) {
        try {
            LuceneNgIndexNode indexNode = tracker.acquireIndexNode(indexPath);
            if (indexNode == null) {
                LOG.warn("Index node not found: {}", indexPath);
                return Cursors.newPathCursor(Collections.emptyList(), filter.getQueryLimits());
            }

            // Get root builder from rootState for reading index data
            NodeBuilder rootBuilder = rootState.builder();

            // Get searcher - pass root builder so OakDirectory can access /var/indexing/lucene/...
            IndexSearcherHolder holder = new IndexSearcherHolder(
                rootBuilder,
                indexNode.getDefinition().getIndexName()
            );
            IndexSearcher searcher = holder.getSearcher();

            // Build Lucene query from filter
            Query query = buildQuery(filter);

            // Execute query
            TopDocs docs = searcher.search(query, 100); // Limit to 100 for now

            // Return cursor
            return new LuceneNgCursor(docs, searcher, holder);

        } catch (IOException e) {
            LOG.error("Error executing query on index: " + indexPath, e);
            return Cursors.newPathCursor(Collections.emptyList(), filter.getQueryLimits());
        }
    }

    private Query buildQuery(Filter filter) {
        FullTextExpression ft = filter.getFullTextConstraint();
        if (ft == null) {
            throw new IllegalArgumentException("No fulltext constraint");
        }

        // Simple term query for now - extract term from fulltext expression
        // FullTextExpression returns a FullTextTerm which has getValue()
        String queryText = extractSearchTerm(ft);
        LOG.debug("Building query for term: {}", queryText);
        return new TermQuery(new Term("text", queryText.toLowerCase()));
    }

    private String extractSearchTerm(FullTextExpression ft) {
        // For simple case, get the string representation and extract the term
        // Format from FullTextParser is "term" (quoted) - remove quotes
        String ftString = ft.toString();
        // Remove surrounding quotes if present
        if (ftString.startsWith("\"") && ftString.endsWith("\"") && ftString.length() > 2) {
            ftString = ftString.substring(1, ftString.length() - 1);
        }
        return ftString;
    }

    @Override
    public String getPlan(Filter filter, NodeState rootState) {
        return "lucene9:" + indexPath + " ft=" + filter.getFullTextConstraint();
    }

    @Override
    public String getIndexName() {
        return "luceneNg";
    }
}
