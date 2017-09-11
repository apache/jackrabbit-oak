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
package org.apache.jackrabbit.oak.plugins.index.solr.configuration.nodestate;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.EmbeddedSolrServerConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.RemoteSolrServerConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfigurationDefaults;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public class NodeStateSolrServerConfigurationProvider implements SolrServerConfigurationProvider {

    private final NodeState nodeState;

    public NodeStateSolrServerConfigurationProvider(NodeState nodeState) {
        this.nodeState = nodeState;
        if (!nodeState.hasProperty(Properties.SERVER_TYPE)) {
            throw new IllegalArgumentException("missing property " + Properties.SERVER_TYPE + " in " + nodeState);
        }
    }

    private int getIntValueFor(String propertyName, int defaultValue) {
        long value = defaultValue;
        PropertyState property = nodeState.getProperty(propertyName);
        if (property != null) {
            value = property.getValue(Type.LONG);
        }
        return (int) value;
    }

    private String getStringValueFor(String propertyName, String defaultValue) {
        String value = defaultValue;
        PropertyState property = nodeState.getProperty(propertyName);
        if (property != null) {
            value = property.getValue(Type.STRING);
        }
        return value;
    }

    @Nonnull
    @Override
    public SolrServerConfiguration<SolrServerProvider> getSolrServerConfiguration() {
        String type = getStringValueFor(Properties.SERVER_TYPE, null);
        if ("embedded".equalsIgnoreCase(type)) {
            String solrHomePath = getStringValueFor(Properties.SOLRHOME_PATH, SolrServerConfigurationDefaults.SOLR_HOME_PATH);
            String coreName = getStringValueFor(Properties.CORE_NAME, SolrServerConfigurationDefaults.CORE_NAME);
            String context = getStringValueFor(Properties.CONTEXT, null);
            Integer httpPort = Integer.valueOf(getStringValueFor(Properties.HTTP_PORT, "0"));

            if (context != null && httpPort > 0) {
                return (SolrServerConfiguration) new EmbeddedSolrServerConfiguration(solrHomePath, coreName)
                        .withHttpConfiguration(context, httpPort);
            } else {
                return (SolrServerConfiguration) new EmbeddedSolrServerConfiguration(solrHomePath, coreName);
            }
        } else if ("remote".equalsIgnoreCase(type)) {
            String solrZkHost = getStringValueFor(Properties.ZK_HOST, null);
            String solrCollection = getStringValueFor(Properties.COLLECTION, SolrServerConfigurationDefaults.COLLECTION);
            int solrReplicationFactor = getIntValueFor(Properties.REPLICATION_FACTOR, SolrServerConfigurationDefaults.REPLICATION_FACTOR);
            String solrConfDir = getStringValueFor(Properties.CONFIGURATION_DIRECTORY, SolrServerConfigurationDefaults.CONFIGURATION_DIRECTORY);
            String solrHttpUrls = getStringValueFor(Properties.HTTP_URL, SolrServerConfigurationDefaults.HTTP_URL);
            int solrShardsNo = getIntValueFor(Properties.SHARDS_NO, SolrServerConfigurationDefaults.SHARDS_NO);

            int socketTimeout = getIntValueFor(Properties.SOCKET_TIMEOUT, SolrServerConfigurationDefaults.SOCKET_TIMEOUT);
            int connectionTimeout = getIntValueFor(Properties.CONNECTION_TIMEOUT, SolrServerConfigurationDefaults.CONNECTION_TIMEOUT);

            return (SolrServerConfiguration) new RemoteSolrServerConfiguration(solrZkHost, solrCollection, solrShardsNo,
                    solrReplicationFactor, solrConfDir, socketTimeout, connectionTimeout, solrHttpUrls);
        } else {
            throw new RuntimeException("unexpected Solr server type: " + type);
        }
    }

    /**
     * Properties that may be retrieved from the configuration {@link org.apache.jackrabbit.oak.spi.state.NodeState}.
     */
    public final class Properties {
        public static final String SERVER_TYPE = "solrServerType";

        // --> embedded solr server properties <--
        public static final String SOLRHOME_PATH = "solrHomePath";
        public static final String CONTEXT = "solrContext";
        public static final String HTTP_PORT = "httpPort";
        public static final String CORE_NAME = "coreName";

        // --> remote solr server properties <--
        public static final String ZK_HOST = "zkHost";
        public static final String COLLECTION = "collection";
        public static final String REPLICATION_FACTOR = "replicationFactor";
        public static final String CONFIGURATION_DIRECTORY = "configurationDirectory";
        public static final String HTTP_URL = "httpUrl";
        public static final String SHARDS_NO = "shardsNo";
        public static final String CONNECTION_TIMEOUT = "connectionTimeout";
        public static final String SOCKET_TIMEOUT = "socketTimeout";
    }

}
