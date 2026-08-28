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
package org.apache.jackrabbit.oak.plugins.index.luceneNg.internal;

import org.apache.jackrabbit.oak.plugins.index.search.IndexStatistics;
import org.apache.lucene.index.IndexReader;

import java.io.IOException;

/**
 * {@link IndexStatistics} backed directly by the {@link IndexReader} of a {@code lucene9} index's
 * cached searcher.
 *
 * <p>Unlike {@code LuceneIndexStatistics} (the {@code oak-lucene} equivalent, which pre-computes a
 * per-field doc-count map up front via {@code MultiFields.getFields(reader)} — an API Lucene 9
 * removed), this implementation computes {@link #getDocCountFor(String)} lazily, one field at a
 * time, straight off {@link IndexReader#getDocCount(String)}. That method already aggregates
 * across all segments (and, for deleted-but-not-merged docs, across live vs. all docs) with no
 * extra I/O beyond what opening the reader already did, so there is nothing to gain from an eager
 * full-field scan: {@code FulltextIndexPlanner} (the only caller, via cost/plan estimation) asks
 * for specific field names one at a time and never enumerates "all fields with stats", so eager
 * pre-computation would do strictly more work for no benefit.</p>
 */
public class LuceneNgIndexStatistics implements IndexStatistics {

    private final int numDocs;
    private final IndexReader reader;

    LuceneNgIndexStatistics(IndexReader reader) {
        this.reader = reader;
        this.numDocs = reader.numDocs();
    }

    @Override
    public int numDocs() {
        return numDocs;
    }

    /**
     * @param field field to return the doc count for
     * @return the number of documents that have at least one term for {@code field}, or
     *         {@code -1} if that count could not be determined (matching
     *         {@link IndexReader#getDocCount(String)}'s own "unavailable" sentinel, and the
     *         {@code oak-lucene} {@code LuceneIndexStatistics} convention of returning {@code -1}
     *         when the reader can't answer the question). Callers ({@code FulltextIndexPlanner})
     *         already treat {@code -1} as "no information, skip this field" rather than as "zero
     *         documents" -- collapsing a read failure to {@code 0} instead would make the planner
     *         think the field matches nothing, which is a materially different (and wrong) signal.
     */
    @Override
    public int getDocCountFor(String field) {
        try {
            return reader.getDocCount(field);
        } catch (IOException e) {
            return -1;
        }
    }
}
