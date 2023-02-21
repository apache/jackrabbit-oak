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

import co.elastic.clients.transport.Version;
import com.github.dockerjava.api.DockerClient;
import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import static org.junit.Assume.assumeNotNull;

public class ElasticTestServer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticTestServer.class);
    private static final Map<String, String> PLUGIN_OFFICIAL_RELEASES_DIGEST_MAP = ImmutableMap.<String, String>builder()
            .put("7.17.3.0", "5e3b40bb72b2813f927be9bf6ecdf88668d89d2ef20c7ebafaa51ab8407fd179")
            .put("7.17.6.0", "326893bb98ef1a0c569d9f4c4a9a073e53361924f990b17e87077985ce8a7478")
            .put("7.17.7.0", "4252eb55cc7775f1b889d624ac335abfa2e357931c40d0accb4d520144246b8b")
            .put("8.3.3.0", "14d3223456f4b9f00f86628ec8400cb46513935e618ae0f5d0d1088739ccc233")
            .put("8.4.1.0", "56797a1bac6ceeaa36d2358f818b14633124d79c5e04630fa3544603d82eaa01")
            .put("8.4.2.0", "5ce81ad043816900a496ad5b3cce7de1d99547ebf92aa1f9856343e48580c71c")
            .put("8.4.3.0", "5c00d43cdd56c5c5d8e9032ad507acea482fb5ca9445861c5cc12ad63af66425")
            .build();

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
        final String pluginVersion = esDockerImageVersion + ".0";
        final String pluginFileName = "elastiknn-" + pluginVersion + ".zip";
        final String localPluginPath = "target/" + pluginFileName;
        LOG.info("Elasticsearch test Docker image version: {}.", esDockerImageVersion);
        downloadSimilaritySearchPluginIfNotExists(localPluginPath, pluginVersion);
        checkIfDockerClientAvailable();
        Network network = Network.newNetwork();
        CONTAINER = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:" + esDockerImageVersion)
                .withEnv("ES_JAVA_OPTS", "-Xms1g -Xmx1g")
                .withCopyFileToContainer(
                        MountableFile.forClasspathResource("elasticsearch.yml"),
                        "/usr/share/elasticsearch/config/elasticsearch.yml")
                // https://www.elastic.co/guide/en/elasticsearch/plugins/8.4/manage-plugins-using-configuration-file.html
                .withCopyFileToContainer(
                        MountableFile.forClasspathResource("elasticsearch-plugins.yml"),
                        "/usr/share/elasticsearch/config/elasticsearch-plugins.yml")
                .withCopyFileToContainer(
                        MountableFile.forHostPath(localPluginPath),
                        "/tmp/plugins/elastiknn.zip")
                .withNetwork(network)
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

    private void downloadSimilaritySearchPluginIfNotExists(String localPluginPath, String pluginVersion) {
        File pluginFile = new File(localPluginPath);
        if (!pluginFile.exists()) {
            LOG.info("Plugin file {} doesn't exist. Trying to download.", localPluginPath);
            String pluginUri;
            String pluginDigest;
            if (PLUGIN_OFFICIAL_RELEASES_DIGEST_MAP.containsKey(pluginVersion)) {
                pluginDigest = PLUGIN_OFFICIAL_RELEASES_DIGEST_MAP.get(pluginVersion);
                pluginUri = "https://github.com/alexklibisz/elastiknn/releases/download/" + pluginVersion
                        + "/elastiknn-" + pluginVersion + ".zip";
            } else {
                pluginDigest = null; // Skip validation
                pluginUri = ElasticTestUtils.ELASTIC_KNN_PLUGIN_URI;
                if (pluginUri == null) {
                    throw new RuntimeException("Elastiknn " + pluginVersion + " is not a known official release, so it cannot be downloaded from the official GitHub repo. Please provide the download URI in system property \"" + ElasticTestUtils.ELASTIC_KNN_PLUGIN_URI_KEY + "\".");
                }
            }
            LOG.info("Downloading Elastiknn plugin from {}.", pluginUri);
            try {
                try (InputStream inputStream = new URL(pluginUri).openStream();
                     FileOutputStream outputStream = new FileOutputStream(pluginFile)
                ) {
                    if (pluginDigest != null) {
                        MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
                        DigestInputStream dis = new DigestInputStream(inputStream, messageDigest);
                        IOUtils.copy(dis, outputStream);
                        messageDigest = dis.getMessageDigest();
                        // bytes to hex
                        StringBuilder result = new StringBuilder();
                        for (byte b : messageDigest.digest()) {
                            result.append(String.format("%02x", b));
                        }
                        if (!pluginDigest.equals(result.toString())) {
                            String deleteString = "Downloaded plugin file deleted.";
                            if (!pluginFile.delete()) {
                                deleteString = "Could not delete downloaded plugin file.";
                            }
                            throw new RuntimeException("Plugin digest unequal. Found " + result + ". Expected " + pluginDigest + ". " + deleteString);
                        }
                    } else {
                        IOUtils.copy(inputStream, outputStream);
                    }
                }
            } catch (IOException | NoSuchAlgorithmException e) {
                throw new RuntimeException("Could not download similarity search plugin", e);
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
