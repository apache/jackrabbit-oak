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

import com.google.common.base.Stopwatch;
import com.google.common.collect.AbstractIterator;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition.SecureFacetConfiguration;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.sortedset.DefaultSortedSetDocValuesReaderState;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetCounts;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * ACL filtered version of {@link SortedSetDocValuesFacetCounts}
 */
class StatisticalSortedSetDocValuesFacetCounts extends SortedSetDocValuesFacetCounts {
    private static final Logger LOG = LoggerFactory.getLogger(StatisticalSortedSetDocValuesFacetCounts.class);

    private final FacetsCollector facetsCollector;
    private final Filter filter;
    private final IndexReader reader;
    private final SecureFacetConfiguration secureFacetConfiguration;
    private final DefaultSortedSetDocValuesReaderState state;
    private FacetResult facetResult = null;

    StatisticalSortedSetDocValuesFacetCounts(DefaultSortedSetDocValuesReaderState state,
                                                    FacetsCollector facetsCollector, Filter filter,
                                                    SecureFacetConfiguration secureFacetConfiguration) throws IOException {
        super(state, facetsCollector);
        this.state = state;
        this.reader = state.origReader;
        this.facetsCollector = facetsCollector;
        this.filter = filter;
        this.secureFacetConfiguration = secureFacetConfiguration;
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

        LabelAndValue[] labelAndValues = topChildren.labelValues;

        List<MatchingDocs> matchingDocsList = facetsCollector.getMatchingDocs();

        int hitCount = 0;
        for (MatchingDocs matchingDocs : matchingDocsList) {
            hitCount += matchingDocs.totalHits;
        }
        int sampleSize = secureFacetConfiguration.getStatisticalFacetSampleSize();
        // In case the hit count is less than sample size(A very small reposiotry perhaps)
        // Delegate getting FacetResults to SecureSortedSetDocValuesFacetCounts to get the exact count
        // instead of statistical count. <OAK-8138>
        if (hitCount < sampleSize) {
            return new SecureSortedSetDocValuesFacetCounts(state, facetsCollector, filter).getTopChildren(topN, dim, path);
        }

        long randomSeed = secureFacetConfiguration.getRandomSeed();

        LOG.debug("Sampling facet dim {}; hitCount: {}, sampleSize: {}, seed: {}", dim, hitCount, sampleSize, randomSeed);

        Stopwatch w = Stopwatch.createStarted();
        Iterator<Integer> docIterator = getMatchingDocIterator(matchingDocsList);
        Iterator<Integer> sampleIterator = docIterator;
        if (sampleSize < hitCount) {
            sampleIterator = getSampledMatchingDocIterator(docIterator, randomSeed, hitCount, sampleSize);
        } else {
            sampleSize = hitCount;
        }
        int accessibleSampleCount = getAccessibleSampleCount(dim, sampleIterator);
        w.stop();

        LOG.debug("Evaluated accessible samples {} in {}", accessibleSampleCount, w);

        labelAndValues = updateLabelAndValueIfRequired(labelAndValues, sampleSize, accessibleSampleCount);

        int childCount = labelAndValues.length;
        Number value = 0;
        for (LabelAndValue lv : labelAndValues) {
            value = value.longValue() + lv.value.longValue();
        }

        return new FacetResult(dim, path, value, labelAndValues, childCount);
    }

    private Iterator<Integer> getMatchingDocIterator(final List<MatchingDocs> matchingDocsList) {
        Iterator<MatchingDocs> matchingDocsListIterator = matchingDocsList.iterator();

        return new AbstractIterator<Integer>() {
            MatchingDocs matchingDocs = null;
            DocIdSetIterator docIdSetIterator = null;
            int nextDocId = NO_MORE_DOCS;
            @Override
            protected Integer computeNext() {
                try {
                    loadNextMatchingDocsIfRequired();

                    if (nextDocId == NO_MORE_DOCS) {
                        return endOfData();
                    } else {
                        int ret = nextDocId;
                        nextDocId = docIdSetIterator.nextDoc();
                        return matchingDocs.context.docBase + ret;
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            private void loadNextMatchingDocsIfRequired() throws IOException {
                while (nextDocId == NO_MORE_DOCS) {
                    if (matchingDocsListIterator.hasNext()) {
                        matchingDocs = matchingDocsListIterator.next();
                        docIdSetIterator = matchingDocs.bits.iterator();
                        nextDocId = docIdSetIterator.nextDoc();
                    } else {
                        return;
                    }
                }
            }
        };
    }

    private Iterator<Integer> getSampledMatchingDocIterator(Iterator<Integer> matchingDocs,
                                                            long randomdSeed, int hitCount, int sampleSize) {
        TapeSampling<Integer> tapeSampling = new TapeSampling<>(new Random(randomdSeed), matchingDocs, hitCount, sampleSize);

        return tapeSampling.getSamples();
    }

    private int getAccessibleSampleCount(String dim, Iterator<Integer> sampleIterator) throws IOException {
        int count = 0;
        while (sampleIterator.hasNext()) {
            int docId = sampleIterator.next();
            Document doc = reader.document(docId);

            if (filter.isAccessible(doc.getField(FieldNames.PATH).stringValue() + "/" + dim)) {
                count++;
            }
        }

        return count;
    }

    private LabelAndValue[] updateLabelAndValueIfRequired(LabelAndValue[] labelAndValues,
                                                          int sampleSize, int accessibleCount) {
        if (accessibleCount < sampleSize) {
            int numZeros = 0;

            LabelAndValue[] newValues;

            {
                LabelAndValue[] proportionedLVs = new LabelAndValue[labelAndValues.length];

                for (int i = 0; i < labelAndValues.length; i++) {
                    LabelAndValue lv = labelAndValues[i];
                    long count = lv.value.longValue() * accessibleCount / sampleSize;
                    if (count == 0) {
                        numZeros++;
                    }
                    proportionedLVs[i] = new LabelAndValue(lv.label, count);
                }

                labelAndValues = proportionedLVs;
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
        } else {
            return labelAndValues;
        }
    }
}