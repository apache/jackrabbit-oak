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

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.security.CreateApiKeyResponse;
import co.elastic.clients.elasticsearch.security.RoleDescriptor;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.Version;
import co.elastic.clients.transport.rest5_client.Rest5ClientTransport;
import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import com.github.dockerjava.api.DockerClient;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.core5.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.Assume.assumeNotNull;

public class ElasticTestServer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticTestServer.class);
    private static final String ELASTIC_DOCKER_IMAGE_VERSION = System.getProperty("elasticDockerImageVersion");

    private static volatile ElasticTestServer SERVER;

    private final ElasticsearchContainer container;
    private final Network network;
    private final String apiKeyId;
    private final String apiKeySecret;
    private volatile boolean closed = false;

    public static synchronized ElasticTestServer getESTestServer() {
        // Setup a new ES container if elasticsearchContainer is null, closed or not running anymore
        if (SERVER == null || SERVER.closed || !SERVER.container.isRunning()) {
            if (SERVER != null && !SERVER.closed) {
                LOG.warn("Existing ES test server is not running anymore, restarting it");
                SERVER.close();
            }
            LOG.info("Starting ES test server");
            SERVER = new ElasticTestServer();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOG.info("Stopping global ES test server.");
                SERVER.close();
            }));
        }
        return SERVER;
    }

    public ElasticsearchContainer getContainer() {
        if (closed) {
            throw new IllegalStateException("Elasticsearch test server is closed");
        }
        return container;
    }

    public String getApiKeyId() {
        return apiKeyId;
    }

    public String getApiKeySecret() {
        return apiKeySecret;
    }

    private ElasticTestServer() {
        String esDockerImageVersion = ELASTIC_DOCKER_IMAGE_VERSION != null ? ELASTIC_DOCKER_IMAGE_VERSION : Version.VERSION.toString();
        LOG.info("Elasticsearch test Docker image version: {}.", esDockerImageVersion);
        checkIfDockerClientAvailable();
        network = Network.newNetwork();
        container = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:" + esDockerImageVersion)
                .withEnv("ES_JAVA_OPTS", "-Xms1g -Xmx1g")
                .withEnv("network.host", "0.0.0.0")
                .withEnv("ingest.geoip.downloader.enabled", "false")
                .withEnv("xpack.security.enabled", "true")
                .withEnv("xpack.security.http.ssl.enabled", "false")
                .withEnv("action.destructive_requires_name", "false")
                .withNetwork(network)
                .withNetworkAliases("elasticsearch")
                .withPassword(ElasticsearchContainer.ELASTICSEARCH_DEFAULT_PASSWORD)
                .waitingFor(Wait.forHttp("/").forPort(9200).forStatusCode(200)
                        .withBasicCredentials("elastic", ElasticsearchContainer.ELASTICSEARCH_DEFAULT_PASSWORD))
                // Default is 30 seconds, which might not be enough on environments with limited resources or network latency
                .withStartupTimeout(Duration.ofMinutes(3))
                .withStartupAttempts(3);
        try {
            container.start();

            try (var es = createAdminEsClient()) {
                CreateApiKeyResponse response = es.security().createApiKey(k -> k
                        .name("test-api-key")
                        .roleDescriptors("oak", RoleDescriptor.roleDescriptorOf(r -> r
                                .indices(i -> i
                                        .names("*")
                                        .privileges("all")))));
                apiKeyId = response.id();
                apiKeySecret = response.apiKey();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            verifyConnectivity();
        } catch (RuntimeException e) {
            close();
            throw e;
        }

        Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(LOG).withSeparateOutputStreams();
        container.followOutput(logConsumer);

        // Check if the ES container started, if not then cleanup and throw an exception
        // No need to run the tests further since they will anyhow fail.
        if (!container.isRunning()) {
            close();
            throw new RuntimeException("Unable to start ES container after retries. Any further tests will fail");
        }
    }

    private ElasticsearchClient createAdminEsClient() {
        BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(new AuthScope(null, -1),
                new UsernamePasswordCredentials("elastic", ElasticsearchContainer.ELASTICSEARCH_DEFAULT_PASSWORD.toCharArray()));
        Rest5Client restClient = Rest5Client.builder(
                        new HttpHost(ElasticConnection.DEFAULT_SCHEME, container.getHost(), container.getMappedPort(ElasticConnection.DEFAULT_PORT)))
                .setHttpClientConfigCallback(httpClientBuilder ->
                        httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider))
                .build();
        ElasticsearchTransport transport = new Rest5ClientTransport(restClient, new JacksonJsonpMapper());
        return new ElasticsearchClient(transport);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        try {
            container.stop();
        } finally {
            try {
                network.close();
            } finally {
                closed = true;
            }
        }
    }

    private void checkIfDockerClientAvailable() {
        DockerClient client = null;
        try {
            client = DockerClientFactory.instance().client();
        } catch (Exception e) {
            LOG.warn("Docker is not available and elasticConnectionDetails sys prop not specified or incorrect" +
                    ", Elastic tests will be skipped");
        }
        assumeNotNull(client);
    }

    private void verifyConnectivity() {
        // Ensure the container is actually reachable before tests proceed.
        try (ElasticConnection connection = ElasticConnection.newBuilder()
                .withIndexPrefix("elastic_test_bootstrap")
                .withConnectionParameters(ElasticConnection.DEFAULT_SCHEME, container.getHost(),
                        container.getMappedPort(ElasticConnection.DEFAULT_PORT))
                .withApiKeys(apiKeyId, apiKeySecret)
                .build()) {
            long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(1);
            while (System.nanoTime() < deadline) {
                if (connection.isAvailable()) {
                    return;
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while waiting for Elasticsearch readiness", e);
                }
            }
            throw new IllegalStateException("Elasticsearch test container started but did not become reachable via HTTP");
        } catch (IOException e) {
            LOG.debug("Error closing bootstrap Elastic connection", e);
        }
    }

    /**
     * Launches an Elasticsearch Test Server to re-use among several test executions.
     */
    public static void main(String[] args) throws IOException {
        ElasticsearchContainer esContainer = ElasticTestServer.getESTestServer().getContainer();
        System.out.println("Docker container with Elasticsearch launched at \"" + esContainer.getHttpHostAddress() +
                "\". Please PRESS ENTER to stop it...");
        System.in.read();
        esContainer.stop();
    }
}
