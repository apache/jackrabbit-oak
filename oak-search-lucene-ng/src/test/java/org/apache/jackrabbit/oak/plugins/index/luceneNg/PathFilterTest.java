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
package org.apache.jackrabbit.oak.plugins.index.luceneNg;

import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.junit.Test;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.junit.Assert.assertEquals;

/**
 * Tests that the Lucene 9 index editor respects {@code includedPaths}: content under an included
 * path is indexed, and content outside it is skipped.
 *
 * <p>Task B4 migrated these from asserting on the editor's {@code childNodeAdded} return value
 * (INCLUDE vs EXCLUDE child editors) to asserting on the observable outcome of a real commit — which
 * paths end up as documents in the index.</p>
 */
public class PathFilterTest {

    private static final String IDX = "/oak:index/test";

    private static NodeState indexWithIncludedDam() throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();
        NodeBuilder defnBuilder = root.child("oak:index").child("test");
        IndexDefinitionBuilder idb = new IndexDefinitionBuilder(defnBuilder);
        idb.noAsync();
        idb.includedPaths("/content/dam");
        idb.indexRule("nt:unstructured").property("title").propertyIndex();
        defnBuilder.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

        // Node under the included path.
        NodeBuilder asset = root.child("content").child("dam").child("asset");
        asset.setProperty("jcr:primaryType", "nt:unstructured");
        asset.setProperty("title", "included");
        // Node outside the included path.
        NodeBuilder libs = root.child("libs").child("thing");
        libs.setProperty("jcr:primaryType", "nt:unstructured");
        libs.setProperty("title", "excluded");

        return LuceneNgEditorCommitUtil.reindex(root.getNodeState());
    }

    @Test
    public void contentUnderIncludedPathIsIndexed() throws Exception {
        NodeState indexed = indexWithIncludedDam();
        try (DirectoryReader reader = LuceneNgEditorCommitUtil.openReader(indexed, IDX)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            assertEquals("node under includedPaths=/content/dam must be indexed", 1,
                    searcher.search(new TermQuery(new Term(FieldNames.PATH, "/content/dam/asset")), 10).totalHits.value);
        }
    }

    @Test
    public void contentOutsideIncludedPathIsNotIndexed() throws Exception {
        NodeState indexed = indexWithIncludedDam();
        try (DirectoryReader reader = LuceneNgEditorCommitUtil.openReader(indexed, IDX)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            assertEquals("node outside includedPaths=/content/dam must be skipped", 0,
                    searcher.search(new TermQuery(new Term(FieldNames.PATH, "/libs/thing")), 10).totalHits.value);
        }
    }
}
