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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.TestRepository;
import org.apache.jackrabbit.oak.plugins.index.TestRepositoryBuilder;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider.compose;

public class LuceneTestRepositoryBuilder extends TestRepositoryBuilder {

    private ResultCountingIndexProvider resultCountingIndexProvider;
    private TestUtil.OptionalEditorProvider optionalEditorProvider;

    public LuceneTestRepositoryBuilder(ExecutorService executorService, TemporaryFolder temporaryFolder) {
        IndexCopier copier = null;
        try {
            copier = new IndexCopier(executorService, temporaryFolder.getRoot());
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.editorProvider = new LuceneIndexEditorProvider(copier, new ExtractedTextCache(10 * FileUtils.ONE_MB, 100));
        this.indexProvider = new LuceneIndexProvider(copier);
        this.asyncIndexUpdate = new AsyncIndexUpdate("async", nodeStore, compose(List.of(
                editorProvider,
                new NodeCounterEditorProvider()
        )));

        resultCountingIndexProvider = new ResultCountingIndexProvider(indexProvider);
        queryEngineSettings = new QueryEngineSettings();
        optionalEditorProvider = new TestUtil.OptionalEditorProvider();
        asyncIndexUpdate.setCorruptIndexHandler(trackingCorruptIndexHandler);
    }

    public TestRepository build() {
        Oak oak = new Oak(nodeStore)
                .with(getInitialContent())
                .with(securityProvider)
                .with(resultCountingIndexProvider)
                .with((Observer) indexProvider)
                .with(editorProvider)
                .with(optionalEditorProvider)
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
