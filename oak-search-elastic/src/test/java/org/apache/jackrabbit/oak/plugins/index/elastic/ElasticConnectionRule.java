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
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
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
    private ElasticConnection elasticConnection;
    private final String elasticConnectionString;
    private static final String INDEX_PREFIX = "ElasticTest_";
    private static final String PLUGIN_DIGEST = "060117b4150c87274d9cff0925ec16e714f28a40906a53a2cd2a23322bbb3189";
    private static boolean useDocker = false;

    public ElasticConnectionRule(String elasticConnectionString) {
        this.elasticConnectionString = elasticConnectionString;
    }

    public ElasticsearchContainer elastic;

    /*
    Executed once in the test class' execution lifecycle, after the execution of apply()
     */
    @Override
    protected void before() {
        if (useDocker()) {
            elasticConnection = getElasticConnectionForDocker();
        }
    }

    /*
    This is the first method to be executed. It gets executed exactly once at the beginning of the test class execution.
     */
    @Override
    public Statement apply(Statement base, Description description) {
        Statement s = super.apply(base, description);
        // see if docker is to be used or not... initialize docker rule only if that's the case.
        final String pluginVersion = "7.10.2.3";
        final String pluginFileName = "elastiknn-" + pluginVersion + ".zip";
        final String localPluginPath = "target/" + pluginFileName;
        downloadSimilaritySearchPluginIfNotExists(localPluginPath, pluginVersion);
        if (elasticConnectionString == null || getElasticConnectionFromString() == null) {
            checkIfDockerClientAvailable();
            elastic = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:" + Version.CURRENT)
                    .withCopyFileToContainer(MountableFile.forHostPath(localPluginPath), "/tmp/plugins/" + pluginFileName)
                    .withCopyFileToContainer(MountableFile.forClasspathResource("elasticstartscript.sh"), "/tmp/elasticstartscript.sh")
                    .withCommand("bash /tmp/elasticstartscript.sh");
            s = elastic.apply(s, description);
            setUseDocker(true);
        }
        return s;
    }

    @Override
    protected void after() {
        //TODO: See if something needs to be cleaned up at test class level ??
    }

    private void downloadSimilaritySearchPluginIfNotExists(String localPluginPath, String pluginVersion) {
        File pluginFile = new File(localPluginPath);
        if (!pluginFile.exists()) {
            LOG.info("Plugin file {} doesn't exist. Trying to download.", localPluginPath);
            try (CloseableHttpClient client = HttpClients.createDefault()) {
                HttpGet get = new HttpGet("https://github.com/alexklibisz/elastiknn/releases/download/" + pluginVersion
                        +"/elastiknn-" + pluginVersion +".zip");
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
                    throw new RuntimeException("Plugin digest unequal. Found " + result.toString() + ". Expected " + PLUGIN_DIGEST + ". " + deleteString);
                }
            } catch (IOException|NoSuchAlgorithmException e) {
                throw new RuntimeException("Could not download similarity search plugin", e);
            }
        }
    }

    public ElasticConnection getElasticConnectionFromString() {
        if (elasticConnection == null) {
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
                elasticConnection = ElasticConnection.newBuilder()
                        .withIndexPrefix(INDEX_PREFIX + System.currentTimeMillis())
                        .withConnectionParameters(scheme, host, port)
                        .withApiKeys(api_key, api_secret)
                        .build();
            } catch (URISyntaxException e) {
                return null;
            }
        }
        return elasticConnection;
    }

    public ElasticConnection getElasticConnectionForDocker() {
        if (elasticConnection == null) {
            elasticConnection = ElasticConnection.newBuilder()
                    .withIndexPrefix(INDEX_PREFIX + System.currentTimeMillis())
                    .withConnectionParameters(ElasticConnection.DEFAULT_SCHEME,
                            elastic.getContainerIpAddress(),
                            elastic.getMappedPort(ElasticConnection.DEFAULT_PORT))
                    .withApiKeys(null, null)
                    .build();
        }
        return elasticConnection;
    }

    public void closeElasticConnection() throws IOException {
        if (elasticConnection != null) {
            elasticConnection.getClient().indices().delete(new DeleteIndexRequest(elasticConnection.getIndexPrefix() + "*"), RequestOptions.DEFAULT);
            elasticConnection.close();
            // Make this object null otherwise tests after the first test would
            // receive an client that is closed.
            elasticConnection = null;
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

    private void setUseDocker(boolean useDocker) {
        ElasticConnectionRule.useDocker = useDocker;
    }

    public boolean useDocker() {
        return useDocker;
    }
}
