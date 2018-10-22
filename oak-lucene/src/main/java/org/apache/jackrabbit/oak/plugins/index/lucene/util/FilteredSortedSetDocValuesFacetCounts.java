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
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
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
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.jetbrains.annotations.NotNull;

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

        if (topChildren == null) {
            return null;
        }

        LabelAndValue[] labelAndValues = topChildren.labelValues;
        InaccessibleFacetCountManager inaccessibleFacetCountManager = new InaccessibleFacetCountManager();

        for (ScoreDoc scoreDoc : docs.scoreDocs) {
            labelAndValues = filterFacet(scoreDoc.doc, dim, labelAndValues, inaccessibleFacetCountManager);
        }

        int childCount = labelAndValues.length;
        Number value = 0;
        for (LabelAndValue lv : labelAndValues) {
            value = value.longValue() + lv.value.longValue();
        }

        return new FacetResult(dim, path, value, labelAndValues, childCount);
    }

    private LabelAndValue[] filterFacet(int docId, String dimension, LabelAndValue[] labelAndValues,
                                        InaccessibleFacetCountManager inaccessibleFacetCountManager) throws IOException {
        Document document = reader.document(docId);

        // filter using doc values (avoiding requiring stored values)
        if (!filter.isAccessible(document.getField(FieldNames.PATH).stringValue() + "/" + dimension)) {

            SortedSetDocValues docValues = state.getDocValues();
            docValues.setDocument(docId);
            TermsEnum termsEnum = docValues.termsEnum();

            long ord = docValues.nextOrd();

            while (ord != SortedSetDocValues.NO_MORE_ORDS) {
                termsEnum.seekExact(ord);
                String facetDVTerm = termsEnum.term().utf8ToString();
                String [] facetDVDimPaths = FacetsConfig.stringToPath(facetDVTerm);

                for (int i = 1; i < facetDVDimPaths.length; i++) {
                    inaccessibleFacetCountManager.markInaccessbile(facetDVDimPaths[i]);
                }

                ord = docValues.nextOrd();
            }
        }

        return inaccessibleFacetCountManager.updateLabelAndValue(labelAndValues);
    }

    static class InaccessibleFacetCountManager {
        Map<String, Long> inaccessbileCounts = Maps.newHashMap();

        void markInaccessbile(@NotNull String label) {
            Long count = inaccessbileCounts.get(label);
            if (count == null) {
                count = 1L;
            } else {
                count--;
            }
            inaccessbileCounts.put(label, count);
        }

        LabelAndValue[] updateLabelAndValue(LabelAndValue[] currentLabels) {
            int numZeros = 0;

            LabelAndValue[] newValues;

            for (int i = 0; i < currentLabels.length; i++) {
                LabelAndValue lv = currentLabels[i];
                Long inaccessibleCount = inaccessbileCounts.get(lv.label);

                if (inaccessibleCount != null) {
                    long newValue = lv.value.longValue() - inaccessibleCount;

                    if (newValue <= 0) {
                        newValue = 0;
                        numZeros++;
                    }

                    currentLabels[i] = new LabelAndValue(lv.label, newValue);
                }
            }

            if (numZeros > 0) {
                newValues = new LabelAndValue[currentLabels.length - numZeros];
                int i = 0;
                for (LabelAndValue lv : currentLabels) {
                    if (lv.value.longValue() > 0) {
                        newValues[i++] = lv;
                    }
                }
            } else {
                newValues = currentLabels;
            }

            return newValues;
        }
    }
}