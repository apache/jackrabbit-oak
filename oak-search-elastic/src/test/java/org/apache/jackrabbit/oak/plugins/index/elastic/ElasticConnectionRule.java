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
package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

/*
To be used as a @ClassRule
 */
public class ElasticConnectionRule extends ExternalResource {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticConnectionRule.class);

    // Set this connection string as
    // <scheme>://<hostname>:<port>?key_id=<>,key_secret=<>
    // key_id and key_secret are optional in case the ES server
    // needs authentication
    // Do not set this if docker is running and you want to run the tests on docker instead.
    private static final String ELASTIC_CONNECTION_STRING = System.getProperty("elasticConnectionString");

    private final String indexPrefix;
    private static boolean useDocker = false;

    private final String elasticConnectionString;

    private ElasticConnectionModel elasticConnectionModel;

    public ElasticConnectionRule() {
        this(ELASTIC_CONNECTION_STRING);
    }

    public ElasticConnectionRule(String elasticConnectionString) {
        this(elasticConnectionString,
                "elastic_test_" +
                        RandomStringUtils.random(5, true, false).toLowerCase() +
                        System.currentTimeMillis()
        );
    }

    public ElasticConnectionRule(String elasticConnectionString, String indexPrefix) {
        this.elasticConnectionString = elasticConnectionString;
        this.indexPrefix = indexPrefix;
    }

    public ElasticsearchContainer elastic;

    /*
    This is the first method to be executed. It gets executed exactly once at the beginning of the test class execution.
     */
    @Override
    public Statement apply(Statement base, Description description) {
        Statement s = super.apply(base, description);
        if (!isValidUri(elasticConnectionString)) {
            elastic = ElasticTestServer.getESTestServer();
            setUseDocker(true);
            initializeElasticConnectionModel(elastic);
        } else {
            initializeElasticConnectionModel(elasticConnectionString);
        }
        return s;
    }

    @Override
    protected void after() {
        ElasticConnection esConnection = getElasticConnection();
        if (esConnection != null) {
            try {
                esConnection.getClient().indices().delete(d -> d.index(this.indexPrefix + "*"));
            } catch (IOException e) {
                LOG.error("Unable to delete indexes with prefix {}", this.indexPrefix);
            } finally {
                IOUtils.closeQuietly(esConnection, e -> LOG.debug("Error closing Elasticsearch connection", e));
            }
        }
    }

    public ElasticConnectionModel getElasticConnectionModel() {
        return elasticConnectionModel;
    }

    private void initializeElasticConnectionModel(String elasticConnectionString) {
        try {
            URI uri = new URI(elasticConnectionString);
            String host = uri.getHost();
            String scheme = uri.getScheme();
            int port = uri.getPort();

            Map<String, String> queryParams = getUriQueryParams(uri);
            String apiKey = queryParams.get("key_id");
            String apiSecret = queryParams.get("key_secret");

            this.elasticConnectionModel = new ElasticConnectionModel();
            elasticConnectionModel.scheme = scheme;
            elasticConnectionModel.elasticHost = host;
            elasticConnectionModel.elasticPort = port;
            elasticConnectionModel.elasticApiKey = apiKey;
            elasticConnectionModel.elasticApiSecret = apiSecret;
            elasticConnectionModel.indexPrefix = indexPrefix;
        } catch (URISyntaxException e) {
            LOG.error("Provided elastic connection string is not valid ", e);
        }
    }

    private void initializeElasticConnectionModel(ElasticsearchContainer elastic) {
        this.elasticConnectionModel = new ElasticConnectionModel();
        elasticConnectionModel.scheme = ElasticConnection.DEFAULT_SCHEME;
        elasticConnectionModel.elasticHost = elastic.getHost();
        elasticConnectionModel.elasticPort = elastic.getMappedPort(ElasticConnection.DEFAULT_PORT);
        elasticConnectionModel.elasticApiKey = null;
        elasticConnectionModel.elasticApiSecret = null;
        elasticConnectionModel.indexPrefix = indexPrefix;
    }

    private Map<String, String> getUriQueryParams(URI uri) {
        String query = uri.getQuery();
        if (query != null) {
            return Arrays.stream(query.split(","))
                    .map(s -> s.split("="))
                    .collect(Collectors.toMap(
                            a -> a[0],  //key
                            a -> a[1]   //value
                    ));
        }
        return Collections.emptyMap();
    }

    public ElasticConnection getElasticConnectionFromString() {
        try {
            URI uri = new URI(elasticConnectionString);
            String host = uri.getHost();
            String scheme = uri.getScheme();
            int port = uri.getPort();
            Map<String, String> queryParams = getUriQueryParams(uri);
            String apiKey = queryParams.get("key_id");
            String apiSecret = queryParams.get("key_secret");

            return ElasticConnection.newBuilder()
                    .withIndexPrefix(indexPrefix)
                    .withConnectionParameters(scheme, host, port)
                    .withApiKeys(apiKey, apiSecret)
                    .build();
        } catch (URISyntaxException e) {
            LOG.error("Provided elastic connection string is not valid ", e);
            return null;
        }
    }

    private boolean isValidUri(String connectionString) {
        if (connectionString == null) {
            return false;
        }
        try {
            new URI(connectionString);
            return true;
        } catch (URISyntaxException e) {
            LOG.debug("Provided elastic connection string is not valid ", e);
            return false;
        }
    }

    /**
     * We initialise elasticConnectionModel in apply method. So use this method only
     * if apply had been called once.
     *
     * @return
     */
    public ElasticConnection getElasticConnection() {
        return ElasticConnection.newBuilder()
                .withIndexPrefix(elasticConnectionModel.indexPrefix)
                .withConnectionParameters(elasticConnectionModel.scheme,
                        elasticConnectionModel.elasticHost, elasticConnectionModel.elasticPort)
                .withApiKeys(elasticConnectionModel.elasticApiKey, elasticConnectionModel.elasticApiSecret)
                .build();
    }

    public ElasticConnection getElasticConnectionForDocker() {
        return getElasticConnectionForDocker(elasticConnectionModel.elasticHost,
                elasticConnectionModel.elasticPort);
    }

    public ElasticConnection getElasticConnectionForDocker(String containerIpAddress, int port) {
        return ElasticConnection.newBuilder()
                .withIndexPrefix(elasticConnectionModel.indexPrefix)
                .withConnectionParameters(elasticConnectionModel.scheme,
                        containerIpAddress, port)
                .withApiKeys(elasticConnectionModel.elasticApiKey, elasticConnectionModel.elasticApiSecret)
                .build();
    }

    private void setUseDocker(boolean useDocker) {
        ElasticConnectionRule.useDocker = useDocker;
    }

    public boolean useDocker() {
        return useDocker;
    }

    public static class ElasticConnectionModel {
        private String elasticApiSecret;
        private String elasticApiKey;
        private String scheme;
        private String elasticHost;
        private int elasticPort;
        private String indexPrefix;

        public String getElasticApiSecret() {
            return elasticApiSecret;
        }

        public String getElasticApiKey() {
            return elasticApiKey;
        }

        public String getScheme() {
            return scheme;
        }

        public String getElasticHost() {
            return elasticHost;
        }

        public int getElasticPort() {
            return elasticPort;
        }

        public String getIndexPrefix() {
            return indexPrefix;
        }
    }
}
