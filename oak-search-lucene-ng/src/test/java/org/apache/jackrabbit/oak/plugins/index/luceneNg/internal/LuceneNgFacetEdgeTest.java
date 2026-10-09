/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition.SecureFacetConfiguration;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.sortedset.DefaultSortedSetDocValuesReaderState;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LuceneNgFacetEdgeTest {
    private static ByteBuffersDirectory index() throws Exception {
        ByteBuffersDirectory directory = new ByteBuffersDirectory();
        FacetsConfig config = new FacetsConfig();
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
            for (int i = 0; i < 4; i++) {
                Document doc = new Document();
                if (i < 3) {
                    doc.add(new StringField(FieldNames.PATH, "/doc" + i, Field.Store.YES));
                }
                doc.add(new SortedSetDocValuesFacetField("tags", i < 3 ? "common" : "rare"));
                writer.addDocument(config.build(doc));
            }
        }
        return directory;
    }

    private static Filter access() {
        Filter filter = mock(Filter.class);
        when(filter.isAccessible("/doc0/tags")).thenReturn(true);
        when(filter.isAccessible("/doc1/tags")).thenReturn(true);
        return filter;
    }

    @Test
    public void missingStoredPathIsInaccessibleAndCannotExposeALabel() throws Exception {
        try (ByteBuffersDirectory directory = index();
             DirectoryReader reader = DirectoryReader.open(directory)) {
            Filter filter = access();
            assertFalse(LuceneNgSecureSortedSetDocValuesFacetCounts.isDocAccessible(reader, filter, 3, "tags"));
            FacetsCollector collector = new FacetsCollector();
            new IndexSearcher(reader).search(new MatchAllDocsQuery(), collector);
            FacetResult result = new LuceneNgSecureSortedSetDocValuesFacetCounts(
                    new DefaultSortedSetDocValuesReaderState(reader), collector, filter).getTopChildren(10, "tags");
            assertEquals(1, result.childCount);
            assertEquals("common", result.labelValues[0].label);
            assertEquals(2, result.labelValues[0].value.intValue());
            assertEquals(2, result.value.intValue());
        }
    }

    @Test
    public void emptyCollectorsAndNoHitsHaveNoFacets() throws Exception {
        try (ByteBuffersDirectory directory = index();
             DirectoryReader reader = DirectoryReader.open(directory)) {
            DefaultSortedSetDocValuesReaderState state = new DefaultSortedSetDocValuesReaderState(reader);
            FacetsCollector empty = new FacetsCollector();
            FacetsCollector noHits = new FacetsCollector();
            new IndexSearcher(reader).search(new MatchNoDocsQuery(), noHits);
            SecureFacetConfiguration configuration = mock(SecureFacetConfiguration.class);
            when(configuration.getStatisticalFacetSampleSize()).thenReturn(4);
            for (FacetsCollector collector : List.of(empty, noHits)) {
                assertNull(new LuceneNgSecureSortedSetDocValuesFacetCounts(
                        state, collector, access()).getTopChildren(10, "tags"));
                assertNull(new LuceneNgStatisticalSortedSetDocValuesFacetCounts(
                        state, collector, access(), configuration).getTopChildren(10, "tags"));
            }
        }
    }

    @Test
    public void fullSampleRoundsDownAndRemovesZeroLabelsWhileSmallSetsUseExactCounts() throws Exception {
        try (ByteBuffersDirectory directory = index();
             DirectoryReader reader = DirectoryReader.open(directory)) {
            DefaultSortedSetDocValuesReaderState state = new DefaultSortedSetDocValuesReaderState(reader);
            FacetsCollector collector = new FacetsCollector();
            new IndexSearcher(reader).search(new MatchAllDocsQuery(), collector);
            SecureFacetConfiguration configuration = mock(SecureFacetConfiguration.class);
            when(configuration.getStatisticalFacetSampleSize()).thenReturn(4);
            FacetResult result = new LuceneNgStatisticalSortedSetDocValuesFacetCounts(
                    state, collector, access(), configuration).getTopChildren(10, "tags");
            assertEquals(1, result.childCount);
            assertEquals("common", result.labelValues[0].label);
            assertEquals(1, result.labelValues[0].value.intValue());
            assertEquals(1, result.value.intValue());
            when(configuration.getStatisticalFacetSampleSize()).thenReturn(5);
            FacetResult exact = new LuceneNgStatisticalSortedSetDocValuesFacetCounts(
                    state, collector, access(), configuration).getTopChildren(10, "tags");
            assertEquals(1, exact.childCount);
            assertEquals(2, exact.labelValues[0].value.intValue());
        }
    }

    @Test
    public void nullMatchingBitsAreSkippedByAclAndSamplingIterators() throws Exception {
        try (ByteBuffersDirectory directory = index();
             DirectoryReader reader = DirectoryReader.open(directory)) {
            DefaultSortedSetDocValuesReaderState state = new DefaultSortedSetDocValuesReaderState(reader);
            FacetsCollector.MatchingDocs empty = new FacetsCollector.MatchingDocs(
                    reader.leaves().get(0), null, 0, null);
            FacetsCollector noBits = mock(FacetsCollector.class);
            when(noBits.getMatchingDocs()).thenReturn(List.of(empty));
            LabelAndValue[] labels = {new LabelAndValue("common", 3)};
            LuceneNgSecureSortedSetDocValuesFacetCounts.InaccessibleFacetCountManager manager =
                    new LuceneNgSecureSortedSetDocValuesFacetCounts.InaccessibleFacetCountManager(
                            "tags", reader, access(), state, noBits, labels);
            manager.filterFacets();
            assertEquals(3, manager.updateLabelAndValue()[0].value.intValue());
            FacetsCollector actual = new FacetsCollector();
            new IndexSearcher(reader).search(new MatchAllDocsQuery(), actual);
            LuceneNgStatisticalSortedSetDocValuesFacetCounts counts =
                    new LuceneNgStatisticalSortedSetDocValuesFacetCounts(
                            state, actual, access(), mock(SecureFacetConfiguration.class));
            List<FacetsCollector.MatchingDocs> matches = new ArrayList<>();
            matches.add(empty);
            matches.addAll(actual.getMatchingDocs());
            Method method = counts.getClass().getDeclaredMethod("getMatchingDocIterator", List.class);
            method.setAccessible(true);
            Iterator<?> iterator = (Iterator<?>) method.invoke(counts, matches);
            for (int doc = 0; doc < 4; doc++) {
                assertEquals(doc, iterator.next());
            }
            assertFalse(iterator.hasNext());
        }
    }
}
