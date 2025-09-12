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

import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.sortedset.DefaultSortedSetDocValuesReaderState;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.Directory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.VERSION;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link SecureSortedSetDocValuesFacetCounts}
 */
public class SecureSortedSetDocValuesFacetCountsTest {

    private Directory directory;
    private IndexWriter indexWriter;
    private FacetsConfig facetsConfig;
    private static final String DIMENSION = "dimension";
    private static final String FACET_FIELD = FieldNames.createFacetFieldName(DIMENSION);

    @Before
    public void setUp() throws IOException {
        directory = new RAMDirectory();
        IndexWriterConfig config = new IndexWriterConfig(VERSION, LuceneIndexConstants.ANALYZER);
        indexWriter = new IndexWriter(directory, config);
        facetsConfig = new FacetsConfig();
        facetsConfig.setMultiValued(DIMENSION, true);
        facetsConfig.setIndexFieldName(DIMENSION, FACET_FIELD);
    }

    @After
    public void tearDown() throws IOException {
        if (indexWriter != null) {
            indexWriter.close();
        }
        if (directory != null) {
            directory.close();
        }
    }

    @Test
    public void testInaccessibleFacetsNotCounted() throws IOException {
        addDocument("/content/doc1", "apple");
        addDocument("/content/doc2", "apple");
        addDocument("/content/doc3", "banana");
        indexWriter.commit();

        Filter mockFilter = createMockFilter();
        when(mockFilter.isAccessible("/content/doc1/" + DIMENSION)).thenReturn(false);
        FacetResult result = executeSecureFacetCount(mockFilter, 2);

        assertEquals(2, result.labelValues.length);
        assertEquals("apple", result.labelValues[0].label);
        assertEquals(1, result.labelValues[0].value.intValue());
        assertEquals("banana", result.labelValues[1].label);
        assertEquals(1, result.labelValues[1].value.intValue());
    }

    @Test
    public void testInaccessibleFacetOutsideTopNAreIgnored() throws IOException {
        addDocument("/content/doc1", "apple");
        addDocument("/content/doc2", "apple");
        addDocument("/content/doc3", "banana");
        indexWriter.commit();

        Filter mockFilter = createMockFilter();
        when(mockFilter.isAccessible("/content/doc3/" + DIMENSION)).thenReturn(false);
        FacetResult result = executeSecureFacetCount(mockFilter, 1);

        assertEquals(1, result.labelValues.length);
        assertEquals("apple", result.labelValues[0].label);
        assertEquals(2, result.labelValues[0].value.intValue());
    }

    private void addDocument(String path, String facetValue) throws IOException {
        Document doc = new Document();
        doc.add(new StringField(FieldNames.PATH, path, Field.Store.YES));
        doc.add(new SortedSetDocValuesFacetField(DIMENSION, facetValue));
        doc = facetsConfig.build(doc);
        indexWriter.addDocument(doc);
    }

    /**
     * Creates a mock filter that allows all access by default
     */
    private Filter createMockFilter() {
        Filter mockFilter = Mockito.mock(Filter.class);
        when(mockFilter.isAccessible(anyString())).thenReturn(true);
        return mockFilter;
    }

    /**
     * @param filter the security filter to apply
     * @param topN   the number of top results to return
     * @return the facet results with security filtering applied
     */
    private FacetResult executeSecureFacetCount(Filter filter, int topN) throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            FacetsCollector facetsCollector = new FacetsCollector();
            FacetsCollector.search(searcher, new MatchAllDocsQuery(), 10, facetsCollector);
            DefaultSortedSetDocValuesReaderState state =
                    new DefaultSortedSetDocValuesReaderState(reader, FACET_FIELD);

            SecureSortedSetDocValuesFacetCounts secureFacets =
                    new SecureSortedSetDocValuesFacetCounts(state, facetsCollector, filter);

            return secureFacets.getTopChildren(topN, DIMENSION);
        }
    }
}
