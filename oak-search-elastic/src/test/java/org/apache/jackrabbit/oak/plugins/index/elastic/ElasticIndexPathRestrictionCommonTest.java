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
package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.index.IndexPathRestrictionCommonTest;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.async.ElasticResultRowAsyncIterator;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.runners.Parameterized;
import org.slf4j.event.Level;

import java.util.Collection;
import java.util.Collections;

public class ElasticIndexPathRestrictionCommonTest extends IndexPathRestrictionCommonTest {

    @ClassRule
    public static final ElasticConnectionRule elasticRule = new ElasticConnectionRule();

    // Default refresh is 1 minute - so we need to lower that otherwise test would need to wait at least 1 minute
    // before it can get the estimated doc count from the remote ES index
    @Rule
    public final ProvideSystemProperty updateSystemProperties
            = new ProvideSystemProperty("oak.elastic.statsRefreshSeconds", "10");

    @Rule
    public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

    private ElasticIndexTracker indexTracker;


    public ElasticIndexPathRestrictionCommonTest(boolean evaluatePathRestrictionsInIndex) {
        super(evaluatePathRestrictionsInIndex);
    }

    @Parameterized.Parameters(name = "evaluatePathRestrictionsInIndex = {0}")
    public static Collection<Object[]> data() {
        // For Elastic - evaluatePathRestrictions is effectively moot since we always enable path restrictions by default.
        // And always index ancestors in ElasticDocumentMaker#finalizeDoc
        // So we test for only 1 combination here - evaluatePathRestrictions=true
        return Collections.singleton(doesIndexEvaluatePathRestrictions(true));
    }

    @Override
    protected void postCommitHooks() {
        indexTracker.update(root);
    }

    @Override
    protected void setupHook() {
        ElasticConnection esConnection = elasticRule.useDocker() ? elasticRule.getElasticConnectionForDocker() :
                elasticRule.getElasticConnectionFromString();
        this.indexTracker = new ElasticIndexTracker(esConnection, new ElasticMetricHandler(StatisticsProvider.NOOP));
        hook = new EditorHook(
                new IndexUpdateProvider(new ElasticIndexEditorProvider(indexTracker, esConnection,
                        new ExtractedTextCache(10 * FileUtils.ONE_MB, 100))));
    }

    @Override
    protected void setupFullTextIndex() {
        indexTracker.update(root);
        index = (FulltextIndex) new ElasticIndexProvider(indexTracker).getQueryIndexes(null).get(0);
    }

    @Override
    protected IndexDefinitionBuilder getIndexDefinitionBuilder(NodeBuilder builder) {
        return new ElasticIndexDefinitionBuilder(builder);
    }

    @Override
    protected String getExpectedLogEntryForPostPathFiltering(String path, boolean shouldBeIncluded) {
        return shouldBeIncluded ? String.format("Path %s satisfies hierarchy inclusion rules", path)
                : String.format("Path %s not included because of hierarchy inclusion rules", path);
    }

    @Override
    protected LogCustomizer getLogCustomizer() {
        return LogCustomizer.forLogger(ElasticResultRowAsyncIterator.class.getName()).enable(Level.TRACE).contains("hierarchy inclusion rules").create();
    }

}
