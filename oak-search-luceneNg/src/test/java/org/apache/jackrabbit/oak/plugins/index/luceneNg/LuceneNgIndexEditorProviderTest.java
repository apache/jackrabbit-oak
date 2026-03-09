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

import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class LuceneNgIndexEditorProviderTest {

    private IndexUpdateCallback callback;
    private NodeState root;
    private NodeBuilder definitionBuilder;
    private LuceneNgIndexEditorProvider provider;

    @Before
    public void setup() {
        callback = mock(IndexUpdateCallback.class);
        root = INITIAL_CONTENT;
        definitionBuilder = root.builder();
        definitionBuilder.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        provider = new LuceneNgIndexEditorProvider(tracker);
    }

    @Test
    public void testProviderCreation() {
        assertNotNull(provider);
    }

    @Test
    public void testGetEditorForOtherType() throws Exception {
        Editor editor = provider.getIndexEditor(
            "lucene",  // different type
            definitionBuilder,
            root,
            callback);

        assertNull("Editor should be null for non-lucene9 type", editor);
    }

    @Test
    public void testGetEditorForLucene9Type() throws Exception {
        Editor editor = provider.getIndexEditor(
            LuceneNgIndexConstants.TYPE_LUCENE9,
            definitionBuilder,
            root,
            callback);

        assertNotNull("Editor should be returned for lucene9 type", editor);
    }
}
