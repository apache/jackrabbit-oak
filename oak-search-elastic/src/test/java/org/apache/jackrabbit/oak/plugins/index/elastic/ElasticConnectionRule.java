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
    private static final String PLUGIN_DIGEST = "db479aeee452b2a0f6e3c619ecdf27ca5853e54e7bc787e5c56a49899c249240";
    private static boolean useDocker = false;

    private final String elasticConnectionString;

    private ElasticConnectionConnectionModel elasticConnectionConnectionModel;

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
        final String pluginVersion = "7.16.3.0";
        final String pluginFileName = "elastiknn-" + pluginVersion + ".zip";
        final String localPluginPath = "target/" + pluginFileName;
        downloadSimilaritySearchPluginIfNotExists(localPluginPath, pluginVersion);
        if (!isValidUri(elasticConnectionString)) {
            checkIfDockerClientAvailable();
            Network network = Network.newNetwork();

            elastic = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:" + Version.CURRENT)
                    .withCopyFileToContainer(MountableFile.forHostPath(localPluginPath), "/tmp/plugins/" + pluginFileName)
                    .withCopyFileToContainer(MountableFile.forClasspathResource("elasticstartscript.sh"), "/tmp/elasticstartscript.sh")
                    .withCommand("bash /tmp/elasticstartscript.sh")
                    .withNetwork(network).withStartupAttempts(3);
            elastic.start();
            setUseDocker(true);
            initializeElasticConnectionModel(elastic);
        }
        else {
            initializeElasticConnectionModel(elasticConnectionString);
        }
        return s;
    }

    @Override
    protected void after() {
        if (elastic != null && elastic.isRunning()) {
            elastic.stop();
        }
    }

    public ElasticConnectionConnectionModel getElasticConnectionConnectionModel() {
        return elasticConnectionConnectionModel;
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



    private ElasticConnectionConnectionModel initializeElasticConnectionModel (String elasticConnectionString) {
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
            this.elasticConnectionConnectionModel = new ElasticConnectionConnectionModel();
            elasticConnectionConnectionModel.scheme = scheme;
            elasticConnectionConnectionModel.elasticHost = host;
            elasticConnectionConnectionModel.elasticPort = port;
            elasticConnectionConnectionModel.elasticApiKey = api_key;
            elasticConnectionConnectionModel.elasticApiSecret = api_secret;
            elasticConnectionConnectionModel.indexPrefix = INDEX_PREFIX + System.currentTimeMillis();
            return elasticConnectionConnectionModel;
        } catch (URISyntaxException e) {
            return null;
        }
    }

    private ElasticConnectionConnectionModel initializeElasticConnectionModel(ElasticsearchContainer elastic) {
        this.elasticConnectionConnectionModel = new ElasticConnectionConnectionModel();
        elasticConnectionConnectionModel.scheme = ElasticConnection.DEFAULT_SCHEME;
        elasticConnectionConnectionModel.elasticHost = elastic.getContainerIpAddress();
        elasticConnectionConnectionModel.elasticPort = elastic.getMappedPort(ElasticConnection.DEFAULT_PORT);
        elasticConnectionConnectionModel.elasticApiKey = null;
        elasticConnectionConnectionModel.elasticApiSecret = null;
        elasticConnectionConnectionModel.indexPrefix = INDEX_PREFIX + System.currentTimeMillis();
        return elasticConnectionConnectionModel;
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

    private boolean isValidUri(String connectionString) {
        if (connectionString == null) {
            return false;
        }
        try {
            new URI(connectionString);
            return true;
        } catch (URISyntaxException e) {
            return false;
        }
    }

    /**
     *  We initialise elasticConnectionConnectionModel in apply method. So use this method only
     *  if apply had been called once.
     * @return
     */
    public ElasticConnection getElasticConnection() {
            return ElasticConnection.newBuilder()
                    .withIndexPrefix(elasticConnectionConnectionModel.indexPrefix)
                    .withConnectionParameters(elasticConnectionConnectionModel.scheme,
                            elasticConnectionConnectionModel.elasticHost, elasticConnectionConnectionModel.elasticPort)
                    .withApiKeys(elasticConnectionConnectionModel.elasticApiKey, elasticConnectionConnectionModel.elasticApiSecret)
                    .build();
    }

    public ElasticConnection getElasticConnectionForDocker() {
        return getElasticConnectionForDocker(elasticConnectionConnectionModel.elasticHost,
                elasticConnectionConnectionModel.elasticPort);
    }

    public ElasticConnection getElasticConnectionForDocker(String containerIpAddress, int port) {
        elasticConnectionConnectionModel.elasticHost = containerIpAddress;
        elasticConnectionConnectionModel.elasticPort = port;
        return ElasticConnection.newBuilder()
                .withIndexPrefix(elasticConnectionConnectionModel.indexPrefix)
                .withConnectionParameters(elasticConnectionConnectionModel.scheme,
                        elasticConnectionConnectionModel.elasticHost, elasticConnectionConnectionModel.elasticPort)
                .withApiKeys(elasticConnectionConnectionModel.elasticApiKey, elasticConnectionConnectionModel.elasticApiSecret)
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

    public class ElasticConnectionConnectionModel {
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
