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
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.MountableFile;

import co.elastic.clients.transport.Version;

import java.io.IOException;
import java.time.Duration;

public class ElasticTestServer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticTestServer.class);

    private static final ElasticTestServer SERVER = new ElasticTestServer();
    private static volatile ElasticsearchContainer CONTAINER;

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
        String esDockerImageVersion = ElasticTestUtils.ELASTIC_DOCKER_IMAGE_VERSION;
        if (esDockerImageVersion == null) {
            esDockerImageVersion = Version.VERSION.toString();
        }
        LOG.info("Elasticsearch test Docker image version: {}.", esDockerImageVersion);
        checkIfDockerClientAvailable();
        Network network = Network.newNetwork();
        CONTAINER = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:" + esDockerImageVersion)
                .withEnv("ES_JAVA_OPTS", "-Xms1g -Xmx1g")
                .withCopyFileToContainer(
                        MountableFile.forClasspathResource("elasticsearch.yml"),
                        "/usr/share/elasticsearch/config/elasticsearch.yml")
                .withNetwork(network)
                .withNetworkAliases("elasticsearch")
                // Default is 30 seconds, which might not be enough on environments with limited resources or network latency
                .withStartupTimeout(Duration.ofMinutes(3))
                .withStartupAttempts(3);
        CONTAINER.start();

        Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(LOG).withSeparateOutputStreams();
        CONTAINER.followOutput(logConsumer);
    }

    @Override
    public void close() {
        if (this == SERVER) {
            // Closed with a shutdown hook
            return;
        }

        if (CONTAINER != null) {
            CONTAINER.stop();
        }
        CONTAINER = null;
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
