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

import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.plugins.index.cursor.AbstractCursor;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.ref.Cleaner;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Cursor over Lucene 9 search results.
 */
public class LuceneNgCursor extends AbstractCursor {

    private static final Logger LOG = LoggerFactory.getLogger(LuceneNgCursor.class);
    private static final int DEFAULT_FACET_TOP_CHILDREN = 10;
    private static final Cleaner CLEANER = Cleaner.create();

    private final TopDocs docs;
    private final IndexSearcher searcher;
    private final Map<String, String> facetColumns; // rep:facet(dim) -> JSON
    private final Map<Integer, String> excerptMap;  // docId -> highlighted excerpt
    private final int facetTopChildren;
    private final Cleaner.Cleanable cleanable;
    private int currentIndex = 0;

    public LuceneNgCursor(TopDocs docs, IndexSearcher searcher) {
        this(docs, searcher, null, Collections.emptyMap(), DEFAULT_FACET_TOP_CHILDREN, null);
    }

    public LuceneNgCursor(TopDocs docs, IndexSearcher searcher,
                          LuceneNgIndexNode.AcquiredNode indexNode) {
        this(docs, searcher, null, Collections.emptyMap(), DEFAULT_FACET_TOP_CHILDREN, indexNode);
    }

    public LuceneNgCursor(TopDocs docs, IndexSearcher searcher, Map<String, Facets> facetsMap) {
        this(docs, searcher, facetsMap, Collections.emptyMap(), DEFAULT_FACET_TOP_CHILDREN, null);
    }

    public LuceneNgCursor(TopDocs docs, IndexSearcher searcher,
                          Map<String, Facets> facetsMap, Map<Integer, String> excerptMap,
                          int facetTopChildren, LuceneNgIndexNode.AcquiredNode indexNode) {
        this.docs = docs;
        this.searcher = searcher;
        this.facetTopChildren = Math.max(1, facetTopChildren);
        this.facetColumns = buildFacetColumns(facetsMap != null ? facetsMap : Collections.emptyMap());
        this.excerptMap = excerptMap != null ? excerptMap : Collections.emptyMap();
        // Fires on cursor GC if not already released via hasNext()==false or close().
        Runnable release = indexNode != null ? indexNode::release : () -> {};
        this.cleanable = CLEANER.register(this, release);
    }

    private Map<String, String> buildFacetColumns(Map<String, Facets> facetsMap) {
        if (facetsMap.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, Facets> entry : facetsMap.entrySet()) {
            String dimension = entry.getKey();
            try {
                // Dimension is the Oak property name (matches legacy lucene index / rep:facet(foo)).
                String luceneFieldName = FieldNames.createFacetFieldName(dimension);
                FacetResult fr = entry.getValue().getTopChildren(facetTopChildren, dimension);
                if (fr == null || fr.labelValues == null) {
                    fr = entry.getValue().getTopChildren(facetTopChildren, luceneFieldName);
                }
                if (fr != null && fr.labelValues != null) {
                    JsopBuilder json = new JsopBuilder();
                    json.object();
                    for (org.apache.lucene.facet.LabelAndValue lv : fr.labelValues) {
                        json.key(lv.label);
                        json.value(lv.value.intValue());
                    }
                    json.endObject();
                    result.put(QueryConstants.REP_FACET + "(" + dimension + ")", json.toString());
                }
            } catch (IOException e) {
                LOG.error("Failed to build facets for {}: {}", dimension, e.getMessage());
            }
        }
        return Collections.unmodifiableMap(result);
    }

    @Override
    public boolean hasNext() {
        boolean more = currentIndex < docs.scoreDocs.length;
        if (!more) {
            cleanable.clean();
        }
        return more;
    }

    @Override
    public IndexRow next() {
        ScoreDoc scoreDoc = docs.scoreDocs[currentIndex++];

        try {
            // Use Lucene 9 API for reading stored fields
            Document doc = searcher.storedFields().document(scoreDoc.doc);
            String path = doc.get(FieldNames.PATH);
            String excerpt = excerptMap.get(scoreDoc.doc);

            return new LuceneNgIndexRow(path, scoreDoc.score, facetColumns, excerpt);

        } catch (IOException e) {
            LOG.error("Error reading document", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getSize(org.apache.jackrabbit.oak.api.Result.SizePrecision precision, long max) {
        return docs.totalHits.value;
    }

    public void close() {
        cleanable.clean();
    }
}
