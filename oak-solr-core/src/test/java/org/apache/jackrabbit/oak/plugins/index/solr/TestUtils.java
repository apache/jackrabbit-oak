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
package org.apache.jackrabbit.oak.plugins.index.solr;

import java.io.File;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.CommitPolicy;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;

import static org.junit.Assert.assertTrue;

/**
 * Utility class for tests
 */
public class TestUtils
        implements SolrServerProvider, OakSolrConfigurationProvider {

    static final String SOLR_HOME_PATH = "/solr";

    public static SolrServer createSolrServer() {
        String homePath = SolrServerProvider.class.getResource(SOLR_HOME_PATH).getFile();
        CoreContainer coreContainer = new CoreContainer(homePath);
        try {
            coreContainer.load();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        EmbeddedSolrServer server = new EmbeddedSolrServer(coreContainer, "oak");
        try {
            server.deleteByQuery("*:*");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return server;
    }

    public static void cleanDataDir() {
        String path = TestUtils.class.getResource("/solr/oak/data").getFile();
        File file = new File(path);
        if (file.exists()) {
            assertTrue(file.delete());
        }
    }

    public static OakSolrConfiguration getTestConfiguration() {
        return new OakSolrConfiguration() {
            @Override
            public String getFieldNameFor(Type<?> propertyType) {
                return null;
            }

            @Override
            public String getPathField() {
                return "path_exact";
            }

            @Override
            public String getFieldForPathRestriction(Filter.PathRestriction pathRestriction) {
                String fieldName = null;
                switch (pathRestriction) {
                    case ALL_CHILDREN: {
                        fieldName = "path_des";
                        break;
                    }
                    case DIRECT_CHILDREN: {
                        fieldName = "path_child";
                        break;
                    }
                    case EXACT: {
                        fieldName = "path_exact";
                        break;
                    }
                    case PARENT: {
                        fieldName = "path_anc";
                        break;
                    }
                    case NO_RESTRICTION:
                        break;
                    default:
                        break;

                }
                return fieldName;
            }

            @Override
            public String getFieldForPropertyRestriction(Filter.PropertyRestriction propertyRestriction) {
                return null;
            }

            @Override
            public CommitPolicy getCommitPolicy() {
                return CommitPolicy.HARD;
            }

            @Override
            public String getCatchAllField() {
                return "catch_all";
            }

        };
    }

    private final SolrServer solrServer = createSolrServer();

    private final OakSolrConfiguration configuration = getTestConfiguration();

    @Override
    public SolrServer getSolrServer() {
        return solrServer;
    }

    @Override
    public OakSolrConfiguration getConfiguration() {
        return configuration;
    }

}
