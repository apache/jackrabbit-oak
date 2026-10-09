/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  The ASF licenses this file to You under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.TestRepository;
import org.apache.jackrabbit.oak.plugins.index.TestRepositoryBuilder;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * Test repository wiring Lucene 9 index editor, tracker-backed query provider, and async indexing.
 */
public class LuceneNgTestRepositoryBuilder extends TestRepositoryBuilder {

    public LuceneNgTestRepositoryBuilder() {
        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        this.editorProvider = new LuceneNgIndexEditorProvider(tracker);
        this.indexProvider = new LuceneNgQueryIndexProvider(tracker);
        this.asyncIndexUpdate = new AsyncIndexUpdate("async", nodeStore, CompositeIndexEditorProvider.compose(
                editorProvider,
                new NodeCounterEditorProvider()));
        queryEngineSettings = new QueryEngineSettings();
        queryEngineSettings.setInferenceEnabled(true);
        asyncIndexUpdate.setCorruptIndexHandler(trackingCorruptIndexHandler);
    }

    @Override
    public TestRepository build() {
        Oak oak = new Oak(nodeStore)
                .with(getInitialContent())
                .with(securityProvider)
                .with(editorProvider)
                .with(indexProvider)
                .with(indexEditorProvider)
                .with(queryIndexProvider)
                .with(queryEngineSettings);
        if (isAsync) {
            oak.withAsyncIndexing("async", defaultAsyncIndexingTimeInSeconds);
        }
        return new TestRepository(oak).with(isAsync).with(asyncIndexUpdate);
    }

    @Override
    protected NodeStore createNodeStore(TestRepository.NodeStoreType memoryNodeStore) {
        return new MemoryNodeStore(InitialContentHelper.INITIAL_CONTENT);
    }
}
