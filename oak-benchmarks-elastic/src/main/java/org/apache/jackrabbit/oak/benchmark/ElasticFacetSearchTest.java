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
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.benchmark.util.TestHelper;
import org.apache.jackrabbit.oak.fixture.JcrCreator;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;

import javax.jcr.Repository;
import java.util.LinkedHashMap;
import java.util.Map;

public class ElasticFacetSearchTest extends FacetSearchTest {

    final private ElasticConnection coordinate;
    protected String indexName;

    ElasticFacetSearchTest(Boolean storageEnabled, ElasticConnection coordinate) {
        super(storageEnabled);
        this.coordinate = coordinate;
    }

    @Override
    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        indexName = TestHelper.getUniqueIndexName("elasticFacetTest");
        Map<String, Boolean> propMap = new LinkedHashMap<>();
        propMap.put(SEARCH_PROP, false);
        propMap.put(FACET_PROP_1, true);
        propMap.put(FACET_PROP_2, true);
        if (fixture instanceof OakRepositoryFixture) {
            return ((OakRepositoryFixture) fixture).setUpCluster(1, new JcrCreator() {
                @Override
                public Jcr customize(Oak oak) {
                    ElasticIndexEditorProvider editorProvider = new ElasticIndexEditorProvider(coordinate,
                            new ExtractedTextCache(10 * FileUtils.ONE_MB, 100));
                    ElasticIndexProvider indexProvider = new ElasticIndexProvider(coordinate);
                    oak.with(editorProvider)
                            .with((Observer) indexProvider)
                            .with((QueryIndexProvider) indexProvider)
                            .with(new PropertyIndexEditorProvider())
                            .with(new NodeTypeIndexProvider())
                            .with(new FacetSearchTest.FacetIndexInitializer(indexName, propMap,
                                    ElasticIndexDefinition.TYPE_ELASTICSEARCH, getFacetMode()));
                    return new Jcr(oak);
                }
            });
        }
        return super.createRepository(fixture);

    }

    @Override
    protected void afterSuite() throws Exception {
        super.afterSuite();
        TestHelper.cleanupRemoteElastic(coordinate, indexName);
    }
}
