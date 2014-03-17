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
        label = "Oak Solr embedded server configuration")
@Service(value = SolrServerConfigurationProvider.class)
@Property(name = "name", value = "embedded", propertyPrivate = true)
public class EmbeddedSolrServerConfigurationProvider implements SolrServerConfigurationProvider<EmbeddedSolrServerProvider> {

    @Property(value = SolrServerConfigurationDefaults.SOLR_HOME_PATH)
    private static final String SOLR_HOME_PATH = "solr.home.path";

    @Property(value = SolrServerConfigurationDefaults.CORE_NAME)
    private static final String SOLR_CORE_NAME = "solr.core.name";

    @Property(value = SolrServerConfigurationDefaults.SOLR_CONFIG_PATH)
    private static final String SOLR_CONFIG_FILE = "solr.config.path";

    @Property(value = SolrServerConfigurationDefaults.HTTP_PORT)
    private static final String SOLR_HTTP_PORT = "solr.http.port";

    @Property(value = SolrServerConfigurationDefaults.CONTEXT)
    private static final String SOLR_CONTEXT = "solr.context";

    private String solrHome;
    private String solrConfigFile;
    private String solrCoreName;

    private Integer solrHttpPort;
    private String solrContext;

    private SolrServerConfiguration<EmbeddedSolrServerProvider> solrServerConfiguration;


    @Activate
    protected void activate(ComponentContext componentContext) throws Exception {
        solrHome = String.valueOf(componentContext.getProperties().get(SOLR_HOME_PATH));
        File file = new File(solrHome);
        if (!file.exists()) {
            assert file.createNewFile();
        }
        solrConfigFile = String.valueOf(componentContext.getProperties().get(SOLR_CONFIG_FILE));
        solrCoreName = String.valueOf(componentContext.getProperties().get(SOLR_CORE_NAME));

        String httpPort = String.valueOf(componentContext.getProperties().get(SOLR_HTTP_PORT));
        if (httpPort != null && httpPort.length() > 0) {
            solrHttpPort = Integer.valueOf(httpPort);
        }
        solrContext = String.valueOf(componentContext.getProperties().get(SOLR_CONTEXT));

        solrServerConfiguration = new EmbeddedSolrServerConfiguration(solrHome, solrConfigFile, solrCoreName).
                withHttpConfiguration(solrContext, solrHttpPort);
    }

    @Deactivate
    protected void deactivate() throws Exception {
        solrHome = null;
        solrHttpPort = null;
        solrConfigFile = null;
        solrCoreName = null;
        solrContext = null;
    }

    @Override
    public SolrServerConfiguration<EmbeddedSolrServerProvider> getSolrServerConfiguration() {
        return solrServerConfiguration;
    }
}
