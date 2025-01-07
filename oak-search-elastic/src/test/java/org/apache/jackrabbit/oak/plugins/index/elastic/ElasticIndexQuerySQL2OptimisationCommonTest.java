/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.plugins.index.IndexQuerySQL2OptimisationCommonTest;
import org.apache.jackrabbit.oak.plugins.index.TestRepository;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.junit.ClassRule;

public class ElasticIndexQuerySQL2OptimisationCommonTest extends IndexQuerySQL2OptimisationCommonTest {

    protected TestRepository repositoryOptionsUtil;
    private final ElasticIndexTracker indexTracker;

    @ClassRule
    public static final ElasticConnectionRule elasticRule = new ElasticConnectionRule();

    public ElasticIndexQuerySQL2OptimisationCommonTest() {
        ElasticConnection esConnection = elasticRule.useDocker() ? elasticRule.getElasticConnectionForDocker() :
                elasticRule.getElasticConnectionFromString();
        this.indexTracker = new ElasticIndexTracker(esConnection, new ElasticMetricHandler(StatisticsProvider.NOOP));
        this.editorProvider = new ElasticIndexEditorProvider(indexTracker, esConnection,
                new ExtractedTextCache(10 * FileUtils.ONE_MB, 100));
        this.indexProvider = new ElasticIndexProvider(indexTracker);
    }

    @Override
    protected Oak getOakRepo() {
        indexOptions = new ElasticIndexOptions();

        return new Oak(new MemoryNodeStore(InitialContentHelper.INITIAL_CONTENT))
                .with(new OpenSecurityProvider())
                .with(indexProvider)
                .with(indexTracker)
                .with(editorProvider)
                .with(new QueryEngineSettings() {
                    @Override
                    public boolean isSql2Optimisation() {
                        return true;
                    }
                });
    }
}
