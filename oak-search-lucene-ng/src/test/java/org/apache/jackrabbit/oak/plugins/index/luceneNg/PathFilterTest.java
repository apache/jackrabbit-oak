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

import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Tests that LuceneNgIndexEditor respects includedPaths when deciding
 * whether to return child editors.
 */
public class PathFilterTest {

    private LuceneNgIndexEditor editorFor(String path, NodeBuilder defnBuilder,
                                          NodeState root) throws Exception {
        return new LuceneNgIndexEditor(path, defnBuilder, root);
    }

    /**
     * When the index has includedPaths=[/content/dam], a childNodeAdded call
     * for a node UNDER the included path must return a non-null editor so that
     * descendants are indexed.
     */
    @Test
    public void childEditorReturnedForIncludedPath() throws Exception {
        NodeBuilder defnBuilder = INITIAL_CONTENT.builder().child("oak:index").child("test");
        IndexDefinitionBuilder idb = new IndexDefinitionBuilder(defnBuilder);
        idb.includedPaths("/content/dam");
        idb.indexRule("nt:unstructured").property("title").propertyIndex();

        LuceneNgIndexEditor root = editorFor("/", defnBuilder, INITIAL_CONTENT);
        Editor content = root.childNodeAdded("content", EMPTY_NODE);
        assertNotNull("editor for /content must not be null (TRAVERSE path)", content);

        Editor dam = ((LuceneNgIndexEditor) content).childNodeAdded("dam", EMPTY_NODE);
        assertNotNull("editor for /content/dam must not be null (INCLUDE path)", dam);
    }

    /**
     * When the index has includedPaths=[/content/dam], a childNodeAdded call
     * for a node OUTSIDE the included path (e.g. /libs) must return null so
     * that the entire subtree is skipped.
     */
    @Test
    public void childEditorNotReturnedForExcludedPath() throws Exception {
        NodeBuilder defnBuilder = INITIAL_CONTENT.builder().child("oak:index").child("test");
        IndexDefinitionBuilder idb = new IndexDefinitionBuilder(defnBuilder);
        idb.includedPaths("/content/dam");
        idb.indexRule("nt:unstructured").property("title").propertyIndex();

        LuceneNgIndexEditor root = editorFor("/", defnBuilder, INITIAL_CONTENT);
        Editor libs = root.childNodeAdded("libs", EMPTY_NODE);
        assertNull("editor for /libs must be null (EXCLUDE path)", libs);
    }
}
