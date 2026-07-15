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
package org.apache.jackrabbit.oak.benchmark;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.benchmark.util.TestHelper;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexTracker;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticMetricHandler;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;

import javax.jcr.Repository;
import javax.jcr.query.Query;
import java.io.File;
import java.util.Collections;

public class ElasticPropertyTextSearchTest extends SearchTest {

    private final ElasticConnection connection;
    private String indexName;

    ElasticPropertyTextSearchTest(File dump, boolean flat, boolean doReport, Boolean storageEnabled, ElasticConnection connection) {
        super(dump, flat, doReport, storageEnabled);
        this.connection = connection;
    }

    @Override
    protected String getQuery(String word) {
        return "SELECT * FROM [nt:base] WHERE [title] = \"" + word + "\"";
    }

    @Override
    protected String queryType() {
        return Query.JCR_SQL2;
    }

    @Override
    protected boolean isFullTextSearch() {
        return false;
    }

    @Override
    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        indexName = TestHelper.getUniqueIndexName("elasticTitle");
        if (fixture instanceof OakRepositoryFixture) {
            return ((OakRepositoryFixture) fixture).setUpCluster(1, oak -> {
                ElasticIndexTracker indexTracker = new ElasticIndexTracker(connection,
                        new ElasticMetricHandler(StatisticsProvider.NOOP));
                ElasticIndexEditorProvider editorProvider = new ElasticIndexEditorProvider(indexTracker, connection,
                        new ExtractedTextCache(10 * FileUtils.ONE_MB, 100));
                ElasticIndexProvider indexProvider = new ElasticIndexProvider(indexTracker);
                oak.with(editorProvider)
                        .with(indexProvider)
                        .with(indexProvider)
                        .with(new PropertyIndexEditorProvider())
                        .with(new NodeTypeIndexProvider())
                        .with(new PropertyFullTextTest.FullTextPropertyInitialiser(indexName, Collections.singleton("title"),
                                ElasticIndexDefinition.TYPE_ELASTICSEARCH));
                return new Jcr(oak);
            });
        }
        return super.createRepository(fixture);
    }

    @Override
    protected void afterSuite() throws Exception {
        super.afterSuite();
        TestHelper.cleanupRemoteElastic(connection, indexName);
    }
}
