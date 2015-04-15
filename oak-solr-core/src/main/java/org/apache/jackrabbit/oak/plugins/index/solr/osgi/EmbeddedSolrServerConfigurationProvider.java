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
package org.apache.jackrabbit.oak.plugins.index.solr.osgi;

import java.io.File;
import javax.annotation.Nonnull;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.EmbeddedSolrServerConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfigurationDefaults;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.server.EmbeddedSolrServerProvider;
import org.osgi.service.component.ComponentContext;

/**
 * An OSGi service {@link org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfigurationProvider}
 */
@Component(metatype = true, immediate = true,
        label = "Apache Jackrabbit Oak Solr embedded server configuration")
@Service(value = SolrServerConfigurationProvider.class)
@Property(name = "name", value = "embedded", propertyPrivate = true)
public class EmbeddedSolrServerConfigurationProvider implements SolrServerConfigurationProvider<EmbeddedSolrServerProvider> {

    @Property(value = SolrServerConfigurationDefaults.SOLR_HOME_PATH, label = "Solr home directory")
    private static final String SOLR_HOME_PATH = "solr.home.path";

    @Property(value = SolrServerConfigurationDefaults.CORE_NAME, label = "Solr Core name")
    private static final String SOLR_CORE_NAME = "solr.core.name";

    private String solrHome;
    private String solrCoreName;

    private SolrServerConfiguration<EmbeddedSolrServerProvider> solrServerConfiguration;


    @Activate
    protected void activate(ComponentContext componentContext) throws Exception {
        solrHome = String.valueOf(componentContext.getProperties().get(SOLR_HOME_PATH));
        File file = new File(solrHome);
        if (!file.exists()) {
            assert file.createNewFile();
        }
        solrCoreName = String.valueOf(componentContext.getProperties().get(SOLR_CORE_NAME));

        solrServerConfiguration = new EmbeddedSolrServerConfiguration(solrHome, solrCoreName);
    }

    @Deactivate
    protected void deactivate() throws Exception {
        solrHome = null;
        solrCoreName = null;
    }

    @Nonnull
    @Override
    public SolrServerConfiguration<EmbeddedSolrServerProvider> getSolrServerConfiguration() {
        return solrServerConfiguration;
    }
}
