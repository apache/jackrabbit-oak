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

import javax.annotation.Nonnull;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.RemoteSolrServerConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfigurationDefaults;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.server.RemoteSolrServerProvider;
import org.osgi.service.component.ComponentContext;

/**
 * {@link org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider} for remote Solr installations.
 */
@Component(metatype = true, immediate = true, label = "Apache Jackrabbit Oak Solr remote server configuration")
@Service(SolrServerConfigurationProvider.class)
@Property(name = "name", value = "remote", propertyPrivate = true)
public class RemoteSolrServerConfigurationProvider implements SolrServerConfigurationProvider<RemoteSolrServerProvider> {

    @Property(value = SolrServerConfigurationDefaults.HTTP_URL, label = "Solr HTTP URL")
    private static final String SOLR_HTTP_URL = "solr.http.url";

    @Property(value = SolrServerConfigurationDefaults.ZK_HOST, label = "ZooKeeper host")
    private static final String SOLR_ZK_HOST = "solr.zk.host";

    @Property(value = SolrServerConfigurationDefaults.COLLECTION, label = "Solr collection")
    private static final String SOLR_COLLECTION = "solr.collection";

    @Property(intValue = SolrServerConfigurationDefaults.SOCKET_TIMEOUT, label = "Socket timeout (ms)")
    private static final String SOCKET_TIMEOUT = "solr.socket.timeout";

    @Property(intValue = SolrServerConfigurationDefaults.CONNECTION_TIMEOUT, label = "Connection timeout (ms)")
    private static final String CONNECTION_TIMEOUT = "solr.connection.timeout";

    @Property(intValue = SolrServerConfigurationDefaults.SHARDS_NO, label = "No. of collection shards")
    private static final String SOLR_SHARDS_NO = "solr.shards.no";

    @Property(intValue = SolrServerConfigurationDefaults.REPLICATION_FACTOR, label = "Replication factor")
    private static final String SOLR_REPLICATION_FACTOR = "solr.replication.factor";

    @Property(value = SolrServerConfigurationDefaults.CONFIGURATION_DIRECTORY, label = "Solr configuration directory")
    private static final String SOLR_CONF_DIR = "solr.conf.dir";

    private String solrHttpUrl;
    private String solrZkHost;
    private String solrCollection;
    private Integer solrShardsNo;
    private Integer solrReplicationFactor;
    private String solrConfDir;
    private RemoteSolrServerConfiguration remoteSolrServerConfiguration;

    @Activate
    protected void activate(ComponentContext componentContext) throws Exception {
        solrHttpUrl = String.valueOf(componentContext.getProperties().get(SOLR_HTTP_URL));
        solrZkHost = String.valueOf(componentContext.getProperties().get(SOLR_ZK_HOST));
        solrCollection = String.valueOf(componentContext.getProperties().get(SOLR_COLLECTION));
        solrShardsNo = Integer.valueOf(componentContext.getProperties().get(SOLR_SHARDS_NO).toString());
        solrReplicationFactor = Integer.valueOf(componentContext.getProperties().get(SOLR_REPLICATION_FACTOR).toString());
        solrConfDir = String.valueOf(componentContext.getProperties().get(SOLR_CONF_DIR));
        int socketTimeout = Integer.valueOf(componentContext.getProperties().get(SOCKET_TIMEOUT).toString());
        int connectionTimeout = Integer.valueOf(componentContext.getProperties().get(CONNECTION_TIMEOUT).toString());
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


    @Nonnull
    @Override
    public SolrServerConfiguration<RemoteSolrServerProvider> getSolrServerConfiguration() {
        return remoteSolrServerConfiguration;
    }
}
