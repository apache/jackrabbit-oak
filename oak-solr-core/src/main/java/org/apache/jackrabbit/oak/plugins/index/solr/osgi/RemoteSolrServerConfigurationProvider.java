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

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

import org.apache.jackrabbit.oak.plugins.index.solr.configuration.RemoteSolrServerConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfigurationDefaults;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.server.RemoteSolrServerProvider;

import org.jetbrains.annotations.NotNull;

/**
 * {@link org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider} for remote Solr installations.
 */
@Component(
        immediate = true,
        service = { SolrServerConfigurationProvider.class }
)
@Designate(
        ocd = RemoteSolrServerConfigurationProvider.Configuration.class
)
public class RemoteSolrServerConfigurationProvider implements SolrServerConfigurationProvider<RemoteSolrServerProvider> {

    @ObjectClassDefinition(
            id = "org.apache.jackrabbit.oak.plugins.index.solr.osgi.RemoteSolrServerConfigurationProvider",
            name = "Apache Jackrabbit Oak Solr remote server configuration"
    )
    @interface Configuration {
        @AttributeDefinition(
                name = "Solr HTTP URL"
        )
        String solr_http_url() default SolrServerConfigurationDefaults.HTTP_URL;

        @AttributeDefinition(
                name = "ZooKeeper host"
        )
        String solr_zk_host() default SolrServerConfigurationDefaults.ZK_HOST;

        @AttributeDefinition(
                name = "Solr collection"
        )
        String solr_collection() default SolrServerConfigurationDefaults.COLLECTION;

        @AttributeDefinition(
                name = "Socket timeout (ms)"
        )
        int solr_socket_timeout() default SolrServerConfigurationDefaults.SOCKET_TIMEOUT;

        @AttributeDefinition(
                name = "Connection timeout (ms)"
        )
        int solr_connection_timeout() default SolrServerConfigurationDefaults.CONNECTION_TIMEOUT;

        @AttributeDefinition(
                name = "No. of collection shards"
        )
        int solr_shards_no() default SolrServerConfigurationDefaults.SHARDS_NO;

        @AttributeDefinition(
                name = "Replication factor"
        )
        int solr_replication_factor() default SolrServerConfigurationDefaults.REPLICATION_FACTOR;

        @AttributeDefinition(
                name = "Solr configuration directory"
        )
        String solr_conf_dir() default SolrServerConfigurationDefaults.CONFIGURATION_DIRECTORY;

        String name() default "remote";
    }

    private String solrHttpUrl;
    private String solrZkHost;
    private String solrCollection;
    private Integer solrShardsNo;
    private Integer solrReplicationFactor;
    private String solrConfDir;
    private RemoteSolrServerConfiguration remoteSolrServerConfiguration;

    @Activate
    protected void activate(Configuration configuration) throws Exception {
        solrHttpUrl = configuration.solr_http_url();
        solrZkHost = configuration.solr_zk_host();
        solrCollection = configuration.solr_collection();
        solrShardsNo = configuration.solr_shards_no();
        solrReplicationFactor = configuration.solr_replication_factor();
        solrConfDir = configuration.solr_conf_dir();
        int socketTimeout = configuration.solr_socket_timeout();
        int connectionTimeout = configuration.solr_connection_timeout();
        remoteSolrServerConfiguration = new RemoteSolrServerConfiguration(solrZkHost, solrCollection, solrShardsNo,
                solrReplicationFactor, solrConfDir, socketTimeout, connectionTimeout, solrHttpUrl);
    }

    @Deactivate
    protected void deactivate() throws Exception {
        solrHttpUrl = null;
        solrZkHost = null;
        solrCollection = null;
        solrShardsNo = 0;
        solrReplicationFactor = 0;
        solrConfDir = null;
    }


    @NotNull
    @Override
    public SolrServerConfiguration<RemoteSolrServerProvider> getSolrServerConfiguration() {
        return remoteSolrServerConfiguration;
    }
}
