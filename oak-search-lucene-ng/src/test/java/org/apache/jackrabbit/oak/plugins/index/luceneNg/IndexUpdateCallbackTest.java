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

import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;

/**
 * Tests that LuceneNgIndexEditor calls IndexUpdateCallback once per
 * successfully indexed document.
 */
public class IndexUpdateCallbackTest {

    @Test
    public void callbackCalledOncePerIndexedDocument() throws Exception {
        AtomicInteger callCount = new AtomicInteger(0);
        IndexUpdateCallback callback = callCount::incrementAndGet;

        NodeBuilder defnBuilder = INITIAL_CONTENT.builder().child("oak:index").child("test");
        IndexDefinitionBuilder idb = new IndexDefinitionBuilder(defnBuilder);
        idb.indexRule("nt:unstructured").property("title").propertyIndex();

        // Two nodes with the indexed property
        NodeBuilder root = INITIAL_CONTENT.builder();
        NodeBuilder page1 = root.child("page1");
        page1.setProperty("jcr:primaryType", "nt:unstructured");
        page1.setProperty("title", "alpha");
        NodeBuilder page2 = root.child("page2");
        page2.setProperty("jcr:primaryType", "nt:unstructured");
        page2.setProperty("title", "beta");
        // One node whose type has no rule — must not trigger the callback
        NodeBuilder page3 = root.child("page3");
        page3.setProperty("jcr:primaryType", "nt:folder");

        LuceneNgIndexEditor editor = new LuceneNgIndexEditor("/", defnBuilder, INITIAL_CONTENT, callback);
        editor.childNodeAdded("page1", page1.getNodeState())
              .enter(EMPTY_NODE, page1.getNodeState());
        editor.childNodeAdded("page2", page2.getNodeState())
              .enter(EMPTY_NODE, page2.getNodeState());
        editor.childNodeAdded("page3", page3.getNodeState())
              .enter(EMPTY_NODE, page3.getNodeState());
        editor.leave(EMPTY_NODE, root.getNodeState());

        assertEquals("callback must be called once per indexed document", 2, callCount.get());
    }

    @Test
    public void callbackNotCalledWhenNoPropertiesIndexed() throws Exception {
        AtomicInteger callCount = new AtomicInteger(0);
        IndexUpdateCallback callback = callCount::incrementAndGet;

        NodeBuilder defnBuilder = INITIAL_CONTENT.builder().child("oak:index").child("test");
        IndexDefinitionBuilder idb = new IndexDefinitionBuilder(defnBuilder);
        idb.indexRule("nt:unstructured").property("title").propertyIndex();

        // Node matches rule but has no configured property
        NodeBuilder root = INITIAL_CONTENT.builder();
        NodeBuilder page1 = root.child("page1");
        page1.setProperty("jcr:primaryType", "nt:unstructured");
        page1.setProperty("description", "no title here");

        LuceneNgIndexEditor editor = new LuceneNgIndexEditor("/", defnBuilder, INITIAL_CONTENT, callback);
        editor.childNodeAdded("page1", page1.getNodeState())
              .enter(EMPTY_NODE, page1.getNodeState());
        editor.leave(EMPTY_NODE, root.getNodeState());

        assertEquals("callback must not be called when no properties matched", 0, callCount.get());
    }
}
