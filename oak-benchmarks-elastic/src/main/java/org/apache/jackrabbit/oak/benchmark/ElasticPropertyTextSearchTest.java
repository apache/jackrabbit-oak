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
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.fixture.JcrCreator;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.ElasticsearchConnection;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.ElasticsearchIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.index.ElasticsearchIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.query.ElasticsearchIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;

import javax.jcr.Repository;
import javax.jcr.query.Query;
import java.io.File;

import static com.google.common.collect.ImmutableSet.of;

public class ElasticPropertyTextSearchTest extends SearchTest {

    private ElasticsearchConnection coordinate;

    public ElasticPropertyTextSearchTest(File dump, boolean flat, boolean doReport, Boolean storageEnabled, ElasticsearchConnection coordinate) {
        super(dump, flat, doReport, storageEnabled);
        this.coordinate = coordinate;
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
        if (fixture instanceof OakRepositoryFixture) {
            return ((OakRepositoryFixture) fixture).setUpCluster(1, new JcrCreator() {
                @Override
                public Jcr customize(Oak oak) {
                    ElasticsearchIndexEditorProvider editorProvider = new ElasticsearchIndexEditorProvider(coordinate,
                            new ExtractedTextCache(10 * FileUtils.ONE_MB, 100));
                    ElasticsearchIndexProvider indexProvider = new ElasticsearchIndexProvider(coordinate);
                    oak.with(editorProvider)
                            .with(indexProvider)
                            .with(new PropertyIndexEditorProvider())
                            .with(new NodeTypeIndexProvider())
                            .with(new PropertyFullTextTest.FullTextPropertyInitialiser("elasticTitle" + System.nanoTime(), of("title"),
                                    ElasticsearchIndexDefinition.TYPE_ELASTICSEARCH));
                    return new Jcr(oak);
                }
            });
        }
        return super.createRepository(fixture);
    }
}
