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
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.elasticsearch.Version;
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
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static org.junit.Assume.assumeNotNull;

public class ElasticTestServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticTestServer.class);
    private static final String PLUGIN_DIGEST = "db479aeee452b2a0f6e3c619ecdf27ca5853e54e7bc787e5c56a49899c249240";
    private static ElasticTestServer esTestServer;
    private volatile ElasticsearchContainer elasticsearchContainer;

    public static synchronized ElasticTestServer getESTestServer() {
            if (esTestServer == null) {
                LOG.info("Starting ES test server");
                esTestServer = new ElasticTestServer();
                esTestServer.setup();
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    LOG.info("Stopping global ES test server.");
                    esTestServer.close();
                }));
            }
            return esTestServer;
    }

    private synchronized  void setup() {
        final String pluginVersion = "7.16.3.0";
        final String pluginFileName = "elastiknn-" + pluginVersion + ".zip";
        final String localPluginPath = "target/" + pluginFileName;
        downloadSimilaritySearchPluginIfNotExists(localPluginPath, pluginVersion);
        checkIfDockerClientAvailable();
        Network network = Network.newNetwork();
        elasticsearchContainer = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:" + Version.CURRENT)
                .withCopyFileToContainer(MountableFile.forHostPath(localPluginPath), "/tmp/plugins/" + pluginFileName)
                .withCopyFileToContainer(MountableFile.forClasspathResource("elasticstartscript.sh"), "/tmp/elasticstartscript.sh")
                .withCommand("bash /tmp/elasticstartscript.sh")
                .withNetwork(network);
        elasticsearchContainer.start();
    }

    @Override
    public void close() {
        if (this == esTestServer) {
            // Closed with a shutdown hook
            return;
        }

        if (elasticsearchContainer != null) {
            elasticsearchContainer.stop();
        }
        elasticsearchContainer = null;
    }

    private void downloadSimilaritySearchPluginIfNotExists(String localPluginPath, String pluginVersion) {
        File pluginFile = new File(localPluginPath);
        if (!pluginFile.exists()) {
            LOG.info("Plugin file {} doesn't exist. Trying to download.", localPluginPath);
            try (CloseableHttpClient client = HttpClients.createDefault()) {
                HttpGet get = new HttpGet("https://github.com/alexklibisz/elastiknn/releases/download/" + pluginVersion
                        + "/elastiknn-" + pluginVersion + ".zip");
                CloseableHttpResponse response = client.execute(get);
                InputStream inputStream = response.getEntity().getContent();
                MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
                DigestInputStream dis = new DigestInputStream(inputStream, messageDigest);
                FileOutputStream outputStream = new FileOutputStream(pluginFile);
                IOUtils.copy(dis, outputStream);
                messageDigest = dis.getMessageDigest();
                // bytes to hex
                StringBuilder result = new StringBuilder();
                for (byte b : messageDigest.digest()) {
                    result.append(String.format("%02x", b));
                }
                if (!PLUGIN_DIGEST.equals(result.toString())) {
                    String deleteString = "Downloaded plugin file deleted.";
                    if (!pluginFile.delete()) {
                        deleteString = "Could not delete downloaded plugin file.";
                    }
                    throw new RuntimeException("Plugin digest unequal. Found " + result + ". Expected " + PLUGIN_DIGEST + ". " + deleteString);
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

    public ElasticsearchContainer container() {
        return elasticsearchContainer;
    }

}
