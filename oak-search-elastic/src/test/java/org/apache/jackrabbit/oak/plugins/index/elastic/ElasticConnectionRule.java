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
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.jackrabbit.oak.commons.IOUtils;
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

    private static final String INDEX_PREFIX = "elastic_test";
    private static final String PLUGIN_DIGEST = "e1f81417d29afde76fea916df178d619f9245d661d692291f6c2cb48878bd209";
    private static boolean useDocker = false;

    private final String elasticConnectionString;

    public ElasticConnectionRule(String elasticConnectionString) {
        this.elasticConnectionString = elasticConnectionString;
    }

    public ElasticsearchContainer elastic;

    /*
    This is the first method to be executed. It gets executed exactly once at the beginning of the test class execution.
     */
    @Override
    public Statement apply(Statement base, Description description) {
        Statement s = super.apply(base, description);
        // see if docker is to be used or not... initialize docker rule only if that's the case.
        final String pluginVersion = "7.17.2.0";
        final String pluginFileName = "elastiknn-" + pluginVersion + ".zip";
        final String localPluginPath = "target/" + pluginFileName;
        downloadSimilaritySearchPluginIfNotExists(localPluginPath, pluginVersion);
        if (elasticConnectionString == null || getElasticConnectionFromString() == null) {
            checkIfDockerClientAvailable();
            Network network = Network.newNetwork();

            elastic = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:" + Version.VERSION)
                    .withCopyFileToContainer(MountableFile.forHostPath(localPluginPath), "/tmp/plugins/" + pluginFileName)
                    .withCopyFileToContainer(MountableFile.forClasspathResource("elasticstartscript.sh"), "/tmp/elasticstartscript.sh")
                    .withCommand("bash /tmp/elasticstartscript.sh")
                    .withNetwork(network);
            elastic.start();

            setUseDocker(true);
        }
        return s;
    }

    @Override
    protected void after() {
        if (elastic != null && elastic.isRunning()) {
            elastic.stop();
        }
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
                    .withIndexPrefix(INDEX_PREFIX + System.currentTimeMillis())
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
                .withIndexPrefix(INDEX_PREFIX + System.currentTimeMillis())
                .withConnectionParameters(ElasticConnection.DEFAULT_SCHEME,
                        containerIpAddress, port)
                .withApiKeys(null, null)
                .build();
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

    private void setUseDocker(boolean useDocker) {
        ElasticConnectionRule.useDocker = useDocker;
    }

    public boolean useDocker() {
        return useDocker;
    }
}
