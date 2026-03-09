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

import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.directory.OakDirectory;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.junit.Test;

import static org.junit.Assert.*;

public class IndexSearcherHolderTest {

    @Test
    public void testGetSearcher() throws Exception {
        NodeBuilder builder = InitialContentHelper.INITIAL_CONTENT.builder();
        NodeBuilder indexDef = builder.child("oak:index").child("test");
        indexDef.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

        // Create empty index with IndexWriter
        OakDirectory directory = new OakDirectory(indexDef, "test", false);
        IndexWriterConfig config = new IndexWriterConfig();
        IndexWriter writer = new IndexWriter(directory, config);
        writer.commit(); // Create segments file
        writer.close();
        directory.close();

        IndexSearcherHolder holder = new IndexSearcherHolder(indexDef, "test");
        IndexSearcher searcher = holder.getSearcher();

        assertNotNull("Searcher should not be null", searcher);
        assertEquals("Empty index should have 0 docs", 0, searcher.getIndexReader().numDocs());

        holder.close();
    }
}
