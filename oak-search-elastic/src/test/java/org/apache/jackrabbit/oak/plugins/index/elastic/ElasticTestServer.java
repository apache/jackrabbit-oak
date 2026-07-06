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

import static org.junit.Assume.assumeNotNull;

import com.github.dockerjava.api.DockerClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.MountableFile;

import co.elastic.clients.transport.Version;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class ElasticTestServer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticTestServer.class);
    private static final String ELASTIC_DOCKER_IMAGE_VERSION = System.getProperty("elasticDockerImageVersion");

    private static final ElasticTestServer SERVER = new ElasticTestServer();
    private static volatile ElasticsearchContainer CONTAINER;
    private static volatile Network network;

    private ElasticTestServer() {
    }

    public static synchronized ElasticsearchContainer getESTestServer() {
        // Setup a new ES container if elasticsearchContainer is null or not running
        if (CONTAINER == null || !CONTAINER.isRunning()) {
            LOG.info("Starting ES test server");
            SERVER.setup();
            // Check if the ES container started, if not then cleanup and throw an exception
            // No need to run the tests further since they will anyhow fail.
            if (CONTAINER == null || !CONTAINER.isRunning()) {
                SERVER.close();
                throw new RuntimeException("Unable to start ES container after retries. Any further tests will fail");
            }
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOG.info("Stopping global ES test server.");
                SERVER.close();
            }));
        }
        return CONTAINER;
    }

    private synchronized void setup() {
        String esDockerImageVersion = ELASTIC_DOCKER_IMAGE_VERSION != null ? ELASTIC_DOCKER_IMAGE_VERSION : Version.VERSION.toString();
        LOG.info("Elasticsearch test Docker image version: {}.", esDockerImageVersion);
        checkIfDockerClientAvailable();
        network = Network.newNetwork();
        CONTAINER = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:" + esDockerImageVersion)
                .withEnv("ES_JAVA_OPTS", "-Xms1g -Xmx1g")
                // Keep test transport deterministic across Testcontainers/ES versions.
                .withEnv("xpack.security.enabled", "false")
                .withEnv("xpack.security.http.ssl.enabled", "false")
                .withCopyFileToContainer(
                        MountableFile.forClasspathResource("elasticsearch.yml"),
                        "/usr/share/elasticsearch/config/elasticsearch.yml")
                .withNetwork(network)
                .withNetworkAliases("elasticsearch")
                .waitingFor(Wait.forHttp("/").forPort(9200).forStatusCode(200))
                // Default is 30 seconds, which might not be enough on environments with limited resources or network latency
                .withStartupTimeout(Duration.ofMinutes(3))
                .withStartupAttempts(3);
        CONTAINER.start();
        verifyConnectivity();

        Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(LOG).withSeparateOutputStreams();
        CONTAINER.followOutput(logConsumer);
    }

    @Override
    public void close() {
        if (CONTAINER != null) {
            CONTAINER.stop();
        }
        CONTAINER = null;
        if (network != null) {
            network.close();
        }
        network = null;
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
                .withConnectionParameters(ElasticConnection.DEFAULT_SCHEME, CONTAINER.getHost(),
                        CONTAINER.getMappedPort(ElasticConnection.DEFAULT_PORT))
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
        ElasticsearchContainer esContainer = ElasticTestServer.getESTestServer();
        System.out.println("Docker container with Elasticsearch launched at \"" + esContainer.getHttpHostAddress() +
                "\". Please PRESS ENTER to stop it...");
        System.in.read();
        esContainer.stop();
    }
}
