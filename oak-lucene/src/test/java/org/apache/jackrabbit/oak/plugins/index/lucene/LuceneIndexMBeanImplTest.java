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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.junit.Before;
import org.junit.Test;

public class LuceneIndexMBeanImplTest {

    private LuceneIndexMBeanImpl luceneIndexMBean;

    private final Map<String, LuceneIndexNode> indexes = new HashMap<>();

    @Before
    public void before() throws Exception {
        IndexTracker tracker = mock(IndexTracker.class);
        when(tracker.acquireIndexNode(anyString())).thenAnswer((inv) -> {
            String path = inv.getArgument(0, String.class);
            return indexes.get(path);
        });
        NodeStore nodeStore = mock(NodeStore.class);
        luceneIndexMBean = new LuceneIndexMBeanImpl(tracker, nodeStore, null,
                new File("target/index"), null);
    }

    private IndexWriter addNodeIndex(String path) throws IOException {
        Directory directory = new RAMDirectory();
        Analyzer analyzer = new SimpleAnalyzer(Version.LUCENE_47);
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_47, analyzer);
        IndexWriter writer = new IndexWriter(directory, config);
        LuceneIndexNode indexNode = mock(LuceneIndexNode.class);
        when(indexNode.getSearcher()).thenAnswer(inv -> new IndexSearcher(DirectoryReader.open(directory)));
        indexes.put(path, indexNode);
        return writer;
    }

    private void assertTermsMatch(String expectedFile, String[] actualTermsResult) throws IOException {
        String[] expectedTermsResult = IOUtils
                .readLines(getClass().getResourceAsStream(expectedFile), StandardCharsets.UTF_8).toArray(new String[0]);
        assertArrayEquals(expectedTermsResult, actualTermsResult);
    }

    @Test
    public void testGetFieldTermsInfo_withType() throws IOException {
        String INDEX_PATH = "/oak:index/test-index";
        IndexWriter indexWriter = addNodeIndex(INDEX_PATH);
        for (int i = 0; i < 10; i++) {
            Document doc = new Document();
            doc.add(new StringField("string", "value-" + i, Store.NO));
            doc.add(new LongField("long", (long) i, Store.NO));
            doc.add(new IntField("int", i, Store.NO));
            indexWriter.addDocument(doc);
            indexWriter.addDocument(doc);
        }
        indexWriter.close();
        String[] stringValues = luceneIndexMBean.getFieldTermsInfo(INDEX_PATH, "string",
                "java.lang.String", 10);
        assertTermsMatch("LuceneIndexMBeanImplTest-expected-string-field.txt", stringValues);

        String[] intValues = luceneIndexMBean.getFieldTermsInfo(INDEX_PATH, "int",
                "int", 10);
        assertTermsMatch("LuceneIndexMBeanImplTest-expected-int-field.txt", intValues);
        intValues = luceneIndexMBean.getFieldTermsInfo(INDEX_PATH, "int",
                "java.lang.Integer", 10);
        assertTermsMatch("LuceneIndexMBeanImplTest-expected-int-field.txt", intValues);

        String[] longValues = luceneIndexMBean.getFieldTermsInfo(INDEX_PATH, "long",
                "long", 10);
        assertTermsMatch("LuceneIndexMBeanImplTest-expected-long-field.txt", longValues);
        longValues = luceneIndexMBean.getFieldTermsInfo(INDEX_PATH, "long",
                "java.lang.Long", 10);
        assertTermsMatch("LuceneIndexMBeanImplTest-expected-long-field.txt", longValues);
    }

}
