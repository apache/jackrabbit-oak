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
package org.apache.jackrabbit.oak.plugins.index.luceneNg.internal;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.apache.lucene.search.DocIdSetIterator;
import org.jetbrains.annotations.NotNull;

/**
 * ACL-filtered variant of {@link SortedSetDocValuesFacetCounts} for Lucene 9,
 * mirroring {@code oak-lucene}'s secure facet behaviour.
 */
public class LuceneNgSecureSortedSetDocValuesFacetCounts extends SortedSetDocValuesFacetCounts {

    private final FacetsCollector facetsCollector;
    private final Filter filter;
    private final IndexReader reader;
    private final SortedSetDocValuesReaderState state;
    private FacetResult facetResult;

    public LuceneNgSecureSortedSetDocValuesFacetCounts(DefaultSortedSetDocValuesReaderState state,
                                                FacetsCollector facetsCollector,
                                                Filter filter) throws IOException {
        super(state, facetsCollector);
        this.reader = state.reader;
        this.facetsCollector = facetsCollector;
        this.filter = filter;
        this.state = state;
    }

    @Override
    public FacetResult getTopChildren(int topN, String dim, String... path) throws IOException {
        if (facetResult == null) {
            facetResult = getTopChildren0(topN, dim, path);
        }
        return facetResult;
    }

    private FacetResult getTopChildren0(int topN, String dim, String... path) throws IOException {
        FacetResult topChildren = super.getTopChildren(topN, dim, path);
        if (topChildren == null) {
            return null;
        }
        InaccessibleFacetCountManager inaccessibleFacetCountManager =
                new InaccessibleFacetCountManager(dim, reader, filter, state, facetsCollector, topChildren.labelValues);
        inaccessibleFacetCountManager.filterFacets();
        LabelAndValue[] labelAndValues = inaccessibleFacetCountManager.updateLabelAndValue();

        int childCount = labelAndValues.length;
        Number value = 0;
        for (LabelAndValue lv : labelAndValues) {
            value = value.longValue() + lv.value.longValue();
        }
        return new FacetResult(dim, path, value, labelAndValues, childCount);
    }

    /**
     * Returns {@code true} if the document at {@code docId} is accessible under {@code dim}
     * according to the query filter. Returns {@code false} when the document has no PATH field
     * (treated as inaccessible). Shared with
     * {@link LuceneNgStatisticalSortedSetDocValuesFacetCounts} to keep the ACL-check logic in one place.
     */
    static boolean isDocAccessible(IndexReader reader, Filter filter, int docId, String dim)
            throws IOException {
        Document document = reader.storedFields().document(docId);
        org.apache.lucene.index.IndexableField pathField = document.getField(FieldNames.PATH);
        if (pathField == null) {
            return false;
        }
        return filter.isAccessible(pathField.stringValue() + "/" + dim);
    }

    static class InaccessibleFacetCountManager {
        private final String dimension;
        private final IndexReader reader;
        private final Filter filter;
        private final SortedSetDocValuesReaderState state;
        private final FacetsCollector facetsCollector;
        private final LabelAndValue[] labelAndValues;
        private final Map<String, Integer> labelToIndexMap;
        private final long[] inaccessibleCounts;

        InaccessibleFacetCountManager(String dimension,
                                      IndexReader reader,
                                      Filter filter,
                                      SortedSetDocValuesReaderState state,
                                      FacetsCollector facetsCollector,
                                      LabelAndValue[] labelAndValues) {
            this.dimension = dimension;
            this.reader = reader;
            this.filter = filter;
            this.state = state;
            this.facetsCollector = facetsCollector;
            this.labelAndValues = labelAndValues;
            inaccessibleCounts = new long[labelAndValues.length];

            Map<String, Integer> map = new HashMap<>();
            for (int i = 0; i < labelAndValues.length; i++) {
                LabelAndValue lv = labelAndValues[i];
                map.put(lv.label, i);
            }
            labelToIndexMap = Collections.unmodifiableMap(map);
        }

        void filterFacets() throws IOException {
            List<FacetsCollector.MatchingDocs> matchingDocsList = facetsCollector.getMatchingDocs();
            for (FacetsCollector.MatchingDocs matchingDocs : matchingDocsList) {
                if (matchingDocs.bits == null) {
                    continue;
                }
                DocIdSetIterator docIdSetIterator = matchingDocs.bits.iterator();
                int doc = docIdSetIterator.nextDoc();
                while (doc != DocIdSetIterator.NO_MORE_DOCS) {
                    int docId = matchingDocs.context.docBase + doc;
                    filterFacet(docId);
                    doc = docIdSetIterator.nextDoc();
                }
            }
        }

        private void filterFacet(int docId) throws IOException {
            if (isDocAccessible(reader, filter, docId, dimension)) {
                return;
            }
            SortedSetDocValues docValues = state.getDocValues();
            if (!docValues.advanceExact(docId)) {
                return;
            }
            TermsEnum termsEnum = docValues.termsEnum();
            long ord = docValues.nextOrd();
            while (ord != SortedSetDocValues.NO_MORE_ORDS) {
                termsEnum.seekExact(ord);
                String facetDVTerm = termsEnum.term().utf8ToString();
                String[] facetDVDimPaths = FacetsConfig.stringToPath(facetDVTerm);
                for (int i = 1; i < facetDVDimPaths.length; i++) {
                    markInaccessible(facetDVDimPaths[i]);
                }
                ord = docValues.nextOrd();
            }
        }

        void markInaccessible(@NotNull String label) {
            Integer index = labelToIndexMap.get(label);
            if (index != null) {
                inaccessibleCounts[index]++;
            }
        }

        LabelAndValue[] updateLabelAndValue() {
            int numZeros = 0;
            LabelAndValue[] newValues;
            for (int i = 0; i < labelAndValues.length; i++) {
                LabelAndValue lv = labelAndValues[i];
                long inaccessibleCount = inaccessibleCounts[labelToIndexMap.get(lv.label)];

                if (inaccessibleCount > 0) {
                    long newValue = lv.value.longValue() - inaccessibleCount;
                    if (newValue <= 0) {
                        newValue = 0;
                        numZeros++;
                    }
                    labelAndValues[i] = new LabelAndValue(lv.label, newValue);
                }
            }
            if (numZeros > 0) {
                newValues = new LabelAndValue[labelAndValues.length - numZeros];
                int i = 0;
                for (LabelAndValue lv : labelAndValues) {
                    if (lv.value.longValue() > 0) {
                        newValues[i++] = lv;
                    }
                }
            } else {
                newValues = labelAndValues;
            }
            return newValues;
        }
    }
}
