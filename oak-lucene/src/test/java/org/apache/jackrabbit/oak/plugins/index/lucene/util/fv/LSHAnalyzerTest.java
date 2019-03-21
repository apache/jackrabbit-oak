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
package org.apache.jackrabbit.oak.plugins.index.lucene.util.fv;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.index.lucene.util.fv.SimSearchUtils.getSimQuery;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link LSHAnalyzer}
 */
public class LSHAnalyzerTest {

    @Test
    public void testTextFVIndexAndSearch() throws Exception {
        String fieldName = "text";
        String[] texts = new String[]{
            "0.1,0.3,0.5,0.7,0.11,0.13,0.17,0.19,0.23,0.29",
            "0.1 0.3 0.5 0.7 0.11 0.13 0.17 0.19 0.23 0.29"
        };

        for (String text : texts) {
            LSHAnalyzer analyzer = new LSHAnalyzer();
            Directory directory = new RAMDirectory();
            IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(Version.LUCENE_47, analyzer));
            DirectoryReader reader = null;
            try {
                Document document = new Document();
                document.add(new TextField(fieldName, text, Field.Store.YES));
                writer.addDocument(document);
                writer.commit();

                reader = DirectoryReader.open(writer, false);
                assertSimQuery(analyzer, fieldName, text, reader);
            } finally {
                if (reader != null) {
                  reader.close();
                }
                writer.close();
                directory.close();
            }
        }
    }

    @Test
    public void testBinaryFVIndexAndSearch() throws Exception {
      LSHAnalyzer analyzer = new LSHAnalyzer();
      Directory directory = new RAMDirectory();
      IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(Version.LUCENE_47, analyzer));
      DirectoryReader reader = null;
      try {
          List<Double> values = new LinkedList<>();
          values.add(0.1d);
          values.add(0.3d);
          values.add(0.5d);
          values.add(0.7d);
          values.add(0.11d);
          values.add(0.13d);
          values.add(0.17d);
          values.add(0.19d);
          values.add(0.23d);
          values.add(0.29d);

          byte[] bytes = SimSearchUtils.toByteArray(values);
          String fvString = SimSearchUtils.toDoubleString(bytes);

          String fieldName = "fvs";
          Document document = new Document();
          document.add(new TextField(fieldName, fvString, Field.Store.YES));
          writer.addDocument(document);
          writer.commit();

          reader = DirectoryReader.open(writer, false);
          assertSimQuery(analyzer, fieldName, fvString, reader);
      } finally {
          if (reader != null) {
              reader.close();
          }
          writer.close();
          directory.close();
      }
    }

    private void assertSimQuery(LSHAnalyzer analyzer, String fieldName, String text, DirectoryReader reader) throws IOException {
        IndexSearcher searcher = new IndexSearcher(reader);
        Query booleanQuery = getSimQuery(analyzer, fieldName,  text);
        TopDocs topDocs = searcher.search(booleanQuery, 1);
        assertEquals(1, topDocs.totalHits);
    }

}