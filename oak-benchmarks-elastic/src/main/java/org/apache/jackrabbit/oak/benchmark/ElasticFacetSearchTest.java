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
import java.util.LinkedHashMap;
import java.util.Map;

public class ElasticFacetSearchTest extends FacetSearchTest {

    final private ElasticConnection connection;
    protected String indexName;

    ElasticFacetSearchTest(Boolean storageEnabled, ElasticConnection coordinate) {
        super(storageEnabled);
        this.connection = coordinate;
    }

    @Override
    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        indexName = TestHelper.getUniqueIndexName("elasticFacetTest");
        Map<String, Boolean> propMap = new LinkedHashMap<>();
        propMap.put(SEARCH_PROP, false);
        propMap.put(FACET_PROP_1, true);
        propMap.put(FACET_PROP_2, true);
        if (fixture instanceof OakRepositoryFixture) {
            return ((OakRepositoryFixture) fixture).setUpCluster(1, oak -> {
                ElasticIndexTracker indexTracker = new ElasticIndexTracker(connection,
                        new ElasticMetricHandler(StatisticsProvider.NOOP));
                ElasticIndexEditorProvider editorProvider = new ElasticIndexEditorProvider(indexTracker, connection,
                        new ExtractedTextCache(10 * FileUtils.ONE_MB, 100));
                ElasticIndexProvider indexProvider = new ElasticIndexProvider(indexTracker);
                oak.with(editorProvider)
                        .with(indexTracker)
                        .with(indexProvider)
                        .with(new PropertyIndexEditorProvider())
                        .with(new NodeTypeIndexProvider())
                        .with(new FacetIndexInitializer(indexName, propMap,
                                ElasticIndexDefinition.TYPE_ELASTICSEARCH, getFacetMode()));
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
