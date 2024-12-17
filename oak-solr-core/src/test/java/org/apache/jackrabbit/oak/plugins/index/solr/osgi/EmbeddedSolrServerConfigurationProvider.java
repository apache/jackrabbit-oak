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

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.osgi.service.metatype.annotations.AttributeDefinition;

import org.apache.jackrabbit.oak.plugins.index.solr.configuration.EmbeddedSolrServerConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfigurationDefaults;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.server.EmbeddedSolrServerProvider;

import org.jetbrains.annotations.NotNull;

/**
 * An OSGi service {@link org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfigurationProvider}
 */
@Component(
        immediate = true,
        service = { SolrServerConfigurationProvider.class }
)
@Designate( ocd = EmbeddedSolrServerConfigurationProvider.Configuration.class )
public class EmbeddedSolrServerConfigurationProvider implements SolrServerConfigurationProvider<EmbeddedSolrServerProvider> {

    @ObjectClassDefinition(
            id = "org.apache.jackrabbit.oak.plugins.index.solr.osgi.EmbeddedSolrServerConfigurationProvider",
            name = "Apache Jackrabbit Oak Solr embedded server configuration"
    )
    @interface Configuration {
        @AttributeDefinition(
                name = "Solr home directory"
        )
        String solr_home_path() default SolrServerConfigurationDefaults.SOLR_HOME_PATH;

        @AttributeDefinition(
                name = "Solr Core name"
        )
        String solr_core_name() default SolrServerConfigurationDefaults.CORE_NAME;

        String name() default "embedded";
    }

    private String solrHome;
    private String solrCoreName;

    private SolrServerConfiguration<EmbeddedSolrServerProvider> solrServerConfiguration;


    @Activate
    protected void activate(Configuration configuration) throws Exception {
        solrHome = configuration.solr_home_path();
        File file = new File(solrHome);
        if (!file.exists()) {
            assert file.createNewFile();
        }
        solrCoreName = configuration.solr_core_name();

        solrServerConfiguration = new EmbeddedSolrServerConfiguration(solrHome, solrCoreName);
    }

    @Deactivate
    protected void deactivate() throws Exception {
        solrHome = null;
        solrCoreName = null;
    }

    @NotNull
    @Override
    public SolrServerConfiguration<EmbeddedSolrServerProvider> getSolrServerConfiguration() {
        return solrServerConfiguration;
    }
}
