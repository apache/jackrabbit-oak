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
package org.apache.jackrabbit.oak.plugins.index.lucene.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.jackrabbit.oak.plugins.index.lucene.FieldNames;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.sortedset.DefaultSortedSetDocValuesReaderState;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetCounts;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;

/**
 * ACL filtered version of {@link SortedSetDocValuesFacetCounts}
 */
class FilteredSortedSetDocValuesFacetCounts extends SortedSetDocValuesFacetCounts {

    private final TopDocs docs;
    private final Filter filter;
    private final IndexReader reader;
    private final SortedSetDocValuesReaderState state;

    public FilteredSortedSetDocValuesFacetCounts(DefaultSortedSetDocValuesReaderState state, FacetsCollector facetsCollector, Filter filter, TopDocs docs) throws IOException {
        super(state, facetsCollector);
        this.reader = state.origReader;
        this.filter = filter;
        this.docs = docs;
        this.state = state;
    }

    @Override
    public FacetResult getTopChildren(int topN, String dim, String... path) throws IOException {
        FacetResult topChildren = super.getTopChildren(topN, dim, path);

        LabelAndValue[] labelAndValues = topChildren.labelValues;

        for (ScoreDoc scoreDoc : docs.scoreDocs) {
            labelAndValues = filterFacet(scoreDoc.doc, dim, labelAndValues);
        }

        int childCount = labelAndValues.length;
        Number value = 0;
        for (LabelAndValue lv : labelAndValues) {
            value = value.longValue() + lv.value.longValue();
        }

        return new FacetResult(dim, path, value, labelAndValues, childCount);
    }

    private LabelAndValue[] filterFacet(int docId, String dimension, LabelAndValue[] labelAndValues) throws IOException {
        boolean filterd = false;
        Map<String, Long> newValues = new HashMap<String, Long>();

        Document document = reader.document(docId);
        SortedSetDocValues docValues = state.getDocValues();
        docValues.setDocument(docId);

        // filter using doc values (avoiding requiring stored values)
        if (!filter.isAccessible(document.getField(FieldNames.PATH).stringValue() + "/" + dimension)) {
            filterd = true;
            for (LabelAndValue lv : labelAndValues) {
                long existingCount = lv.value.longValue();

                BytesRef key = new BytesRef(FacetsConfig.pathToString(dimension, new String[]{lv.label}));
                long l = docValues.lookupTerm(key);
                if (l >= 0) {
                    if (existingCount > 0) {
                        newValues.put(lv.label, existingCount - 1);
                    } else {
                        if (newValues.containsKey(lv.label)) {
                            newValues.remove(lv.label);
                        }
                    }
                }
            }
        }
        LabelAndValue[] filteredLVs;
        if (filterd) {
            filteredLVs = new LabelAndValue[newValues.size()];
            int i = 0;
            for (Map.Entry<String, Long> entry : newValues.entrySet()) {
                filteredLVs[i] = new LabelAndValue(entry.getKey(), entry.getValue());
                i++;
            }
        } else {
            filteredLVs = labelAndValues;
        }

        return filteredLVs;
    }
}