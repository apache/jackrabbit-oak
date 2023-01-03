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
package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.TestRepository;
import org.apache.jackrabbit.oak.plugins.index.TestRepositoryBuilder;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider.compose;

public class ElasticTestRepositoryBuilder extends TestRepositoryBuilder {

    private final ElasticConnection esConnection;
    private final ElasticIndexTracker indexTracker;
    private final int asyncIndexingTimeInSeconds = 1;

    public ElasticTestRepositoryBuilder(ElasticConnectionRule elasticRule) {
        this.esConnection = elasticRule.useDocker() ? elasticRule.getElasticConnectionForDocker() :
                elasticRule.getElasticConnectionFromString();
        this.indexTracker = new ElasticIndexTracker(esConnection, new ElasticMetricHandler(StatisticsProvider.NOOP));
        this.editorProvider = getIndexEditorProvider();
        this.indexProvider = new ElasticIndexProvider(indexTracker);
        this.asyncIndexUpdate = new AsyncIndexUpdate("async", nodeStore, compose(newArrayList(
                editorProvider,
                new NodeCounterEditorProvider()
        )));
        queryEngineSettings = new QueryEngineSettings();
        asyncIndexUpdate.setCorruptIndexHandler(trackingCorruptIndexHandler);
    }

    public TestRepository build() {
        Oak oak = new Oak(nodeStore)
                .with(initialContent)
                .with(securityProvider)
                .with(editorProvider)
                .with(indexTracker)
                .with(indexProvider)
                .with(indexEditorProvider)
                .with(queryIndexProvider)
                .with(queryEngineSettings);
        if (isAsync) {
            oak.withAsyncIndexing("async", asyncIndexingTimeInSeconds);
        }
        return new TestRepository(oak).with(isAsync).with(asyncIndexUpdate);
    }

    private IndexEditorProvider getIndexEditorProvider() {
        return new ElasticIndexEditorProvider(indexTracker, esConnection,
                new ExtractedTextCache(10 * FileUtils.ONE_MB, 100));
    }
}
