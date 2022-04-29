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

import com.github.dockerjava.api.DockerClient;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static org.junit.Assume.assumeNotNull;

/*
To be used as a @ClassRule
 */
public class ElasticConnectionRule extends ExternalResource {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticConnectionRule.class);

    private final String indexPrefix;
    private static boolean useDocker = false;

    private final String elasticConnectionString;

    public ElasticConnectionRule(String elasticConnectionString) {
        this.elasticConnectionString = elasticConnectionString;
        indexPrefix = "elastic_test_" + RandomStringUtils.random(5, true, false).toLowerCase();
    }

    public ElasticsearchContainer elastic;

    /*
    This is the first method to be executed. It gets executed exactly once at the beginning of the test class execution.
     */
    @Override
    public Statement apply(Statement base, Description description) {
        Statement s = super.apply(base, description);
        if (elasticConnectionString == null || getElasticConnectionFromString() == null) {
            elastic = ElasticTestServer.getESTestServer().container();
            setUseDocker(true);
        }
        return s;
    }

    @Override
    protected void after() {
        ElasticConnection esConnection = useDocker() ? getElasticConnectionForDocker() : getElasticConnectionFromString();
        if (esConnection != null) {
            try {
                esConnection.getClient().indices().delete(new DeleteIndexRequest(esConnection.getIndexPrefix() + "*"), RequestOptions.DEFAULT);
                esConnection.close();
            } catch (IOException e) {
                LOG.error("Unable to delete indexes with prefix {}", esConnection.getIndexPrefix());
            }
        }
    }

    public ElasticConnection getElasticConnectionFromString() {
        try {
            URI uri = new URI(elasticConnectionString);
            String host = uri.getHost();
            String scheme = uri.getScheme();
            int port = uri.getPort();
            String query = uri.getQuery();

            String api_key = null;
            String api_secret = null;
            if (query != null) {
                api_key = query.split(",")[0].split("=")[1];
                api_secret = query.split(",")[1].split("=")[1];
            }
            return ElasticConnection.newBuilder()
                    .withIndexPrefix(indexPrefix + System.currentTimeMillis())
                    .withConnectionParameters(scheme, host, port)
                    .withApiKeys(api_key, api_secret)
                    .build();
        } catch (URISyntaxException e) {
            return null;
        }
    }

    public ElasticConnection getElasticConnectionForDocker() {
        return getElasticConnectionForDocker(elastic.getContainerIpAddress(),
                elastic.getMappedPort(ElasticConnection.DEFAULT_PORT));
    }

    public ElasticConnection getElasticConnectionForDocker(String containerIpAddress, int port) {
        return ElasticConnection.newBuilder()
                .withIndexPrefix(indexPrefix + System.currentTimeMillis())
                .withConnectionParameters(ElasticConnection.DEFAULT_SCHEME,
                        containerIpAddress, port)
                .withApiKeys(null, null)
                .build();
    }

    private void setUseDocker(boolean useDocker) {
        ElasticConnectionRule.useDocker = useDocker;
    }

    public boolean useDocker() {
        return useDocker;
    }
}
