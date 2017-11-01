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

import com.google.common.collect.Lists;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.VERSION;

public class IndexStatisticsTest {

    @Test
    public void numDocs() throws Exception {
        Directory d = createSampleDirectory(2);
        IndexStatistics stats = getStats(d);

        Assert.assertEquals(2, stats.numDocs());
    }

    @Test
    public void numDocsWithDelele() throws Exception {
        Directory d = createSampleDirectory(2);
        {
            IndexWriter writer = getWriter(d);
            writer.deleteDocuments(new Term("foo", "bar1"));
            writer.close();
        }

        IndexStatistics stats = getStats(d);
        Assert.assertEquals(1, stats.numDocs());
    }

    @Test
    public void getSimpleFieldDocCnt() throws Exception {
        Directory d = createSampleDirectory(2);
        IndexStatistics stats = getStats(d);

        Assert.assertEquals(2, stats.getDocCountFor("foo"));
    }

    @Test
    public void getSimpleFieldDocCntWithDelete() throws Exception {
        Directory d = createSampleDirectory(2);
        {
            IndexWriter writer = getWriter(d);
            writer.deleteDocuments(new Term("foo", "bar1"));
            writer.close();
        }

        IndexStatistics stats = getStats(d);
        Assert.assertEquals("Stats don't need to get accurate result which might require reading more",
                2, stats.getDocCountFor("foo"));
    }

    @Test
    public void absentFields() throws Exception {
        Directory d = createSampleDirectory(1);
        IndexStatistics stats = getStats(d);

        Assert.assertEquals(1, stats.getDocCountFor("foo"));
        Assert.assertEquals(0, stats.getDocCountFor("absent"));
        Assert.assertEquals(-1, stats.getDocCountFor(":someHiddenField"));
        Assert.assertEquals(-1, stats.getDocCountFor(FieldNames.ANALYZED_FIELD_PREFIX + "foo"));
        Assert.assertEquals(-1, stats.getDocCountFor(FieldNames.FULLTEXT_RELATIVE_NODE + "foo"));
        Assert.assertEquals(-1, stats.getDocCountFor("foo_facet"));
    }

    @Test
    public void onlyPropertyFields() throws Exception {
        Document document = new Document();
        document.add(new StringField("foo", "manualBar", Field.Store.NO));
        document.add(new StringField(":someHiddenField", "manualBar", Field.Store.NO));
        document.add(new StringField(FieldNames.ANALYZED_FIELD_PREFIX + "foo", "manualBar", Field.Store.NO));
        document.add(new StringField(FieldNames.FULLTEXT_RELATIVE_NODE + "foo", "manualBar", Field.Store.NO));
        document.add(new StringField("foo_facet", "manualBar", Field.Store.NO));
        Directory d = createSampleDirectory(document);
        IndexStatistics stats = getStats(d);

        Assert.assertEquals(3, stats.getDocCountFor("foo"));
        Assert.assertEquals(0, stats.getDocCountFor("absent"));
        Assert.assertEquals(-1, stats.getDocCountFor(":someHiddenField"));
        Assert.assertEquals(-1, stats.getDocCountFor(FieldNames.ANALYZED_FIELD_PREFIX + "foo"));
        Assert.assertEquals(-1, stats.getDocCountFor(FieldNames.FULLTEXT_RELATIVE_NODE + "foo"));
        Assert.assertEquals(-1, stats.getDocCountFor("foo_facet"));
    }

    private static Directory createSampleDirectory(long numOfDocs) throws IOException {
        return createSampleDirectory(numOfDocs, Lists.newArrayList());
    }

    private static Directory createSampleDirectory(Document moreDoc) throws IOException {
        return createSampleDirectory(2, Collections.singleton(moreDoc));
    }

    private static Directory createSampleDirectory(long numOfDocs, Iterable<Document> moreDocs) throws IOException {
        List<Document> docs = Lists.newArrayList(moreDocs);
        for (int i = 0; i < numOfDocs; i++) {
            Document doc = new Document();
            doc.add(new StringField("foo", "bar" + i, Field.Store.NO));
            docs.add(doc);
        }

        return createSampleDirectory(docs);
    }

    private static Directory createSampleDirectory(Iterable<Document> docs) throws IOException {
        Directory dir = new RAMDirectory();
        IndexWriter writer = null;
        try {
            writer = getWriter(dir);
            for (Document doc : docs) {
                writer.addDocument(doc);
            }
            return dir;
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }

    private static IndexWriter getWriter(Directory d) throws IOException {
        IndexWriterConfig config = new IndexWriterConfig(VERSION, LuceneIndexConstants.ANALYZER);
        return new IndexWriter(d, config);
    }

    private static IndexStatistics getStats(Directory d) throws IOException {
        IndexReader reader = DirectoryReader.open(d);
        // no more reads
        d.close();

        IndexStatistics stats = new IndexStatistics(reader);
        //close reader... Index stats would read numDocs right away
        reader.close();

        return stats;
    }
}
