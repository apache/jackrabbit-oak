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
package org.apache.jackrabbit.oak.plugins.index.solr.embedded.osgi;

import java.io.File;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.plugins.index.solr.embedded.SolrServerConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.embedded.SolrServerConfigurationDefaults;
import org.apache.jackrabbit.oak.plugins.index.solr.embedded.SolrServerConfigurationProvider;
import org.apache.solr.client.solrj.SolrServer;
import org.osgi.service.component.ComponentContext;

/**
 * An OSGi service {@link SolrServerConfigurationProvider}
 */
@Component(metatype = true,
        label = "OSGi Solr server configuration provider")
@Service(value = SolrServerConfigurationProvider.class)
public class OsgiSolrServerConfigurationProvider implements SolrServerConfigurationProvider {

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

    private static SolrServer solrServer;

    private String solrHome;
    private String solrConfigFile;
    private String solrCoreName;

    private Integer solrHttpPort;
    private String solrContext;

    private SolrServerConfiguration solrServerConfiguration;


    @Activate
    protected void activate(ComponentContext componentContext) throws Exception {
        solrHome = String.valueOf(componentContext.getProperties().get(SOLR_HOME_PATH));
        File file = new File(solrHome);
        if (!file.exists()) {
            assert file.createNewFile();
        }
        solrConfigFile = String.valueOf(componentContext.getProperties().get(SOLR_CONFIG_FILE));
        solrCoreName = String.valueOf(componentContext.getProperties().get(SOLR_CORE_NAME));

        solrHttpPort = Integer.valueOf(String.valueOf(componentContext.getProperties().get(SOLR_HTTP_PORT)));
        solrContext = String.valueOf(componentContext.getProperties().get(SOLR_CONTEXT));

        solrServerConfiguration = new SolrServerConfiguration(solrHome, solrConfigFile, solrCoreName).
                withHttpConfiguration(solrContext, solrHttpPort);
    }

    @Deactivate
    protected void deactivate() throws Exception {
        solrHome = null;
        solrHttpPort = null;
        solrConfigFile = null;
        solrCoreName = null;
        solrContext = null;
        if (solrServer != null) {
            solrServer.shutdown();
            solrServer = null;
        }
    }

    @Override
    public SolrServerConfiguration getSolrServerConfiguration() {
        return solrServerConfiguration;
    }
}
