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

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.transport.ElasticsearchTransport;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RestHighLevelClientBuilder;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class represents an Elasticsearch Connection with the related <code>RestHighLevelClient</code>.
 * As per Elasticsearch documentation: the client is thread-safe, there should be one instance per application and it
 * must be closed when it is not needed anymore.
 *
 * <p>
 * The getClient() initializes the rest client on the first call.
 * Once close() is invoked this instance cannot be used anymore.
 */
public class ElasticConnection implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticConnection.class);

    protected static final String DEFAULT_SCHEME = "http";
    protected static final String DEFAULT_HOST = "127.0.0.1";
    protected static final int DEFAULT_PORT = 9200;
    protected static final String DEFAULT_API_KEY_ID = "";
    protected static final String DEFAULT_API_KEY_SECRET = "";
    protected static final int ES_SOCKET_TIMEOUT = 120000;

    private final String indexPrefix;
    private final String scheme;
    private final String host;
    private final int port;

    // API key credentials
    private final String apiKeyId;
    private final String apiKeySecret;

    private volatile Clients clients;

    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    /**
     * Creates an {@code ElasticsearchConnection} instance with the given scheme, host address and port that support API
     * key-based authentication.
     *
     * @param indexPrefix  the prefix to be used for index creation
     * @param scheme       the name {@code HttpHost.scheme} name
     * @param host         the hostname (IP or DNS name)
     * @param port         the Elasticsearch port for incoming HTTP requests (transport client not supported)
     * @param apiKeyId     the unique id of the API key
     * @param apiKeySecret the generated API secret
     * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api.html#security-api-keys">Elasticsearch Security API Keys</a>
     */
    private ElasticConnection(@NotNull String indexPrefix, @NotNull String scheme, @NotNull String host,
                              @NotNull Integer port, String apiKeyId, String apiKeySecret) {
        this.indexPrefix = indexPrefix;
        this.scheme = scheme;
        this.host = host;
        this.port = port;
        this.apiKeyId = apiKeyId;
        this.apiKeySecret = apiKeySecret;
    }

    /**
     * Instantiates both RHL client (old) and the Java client by sharing the REST High Level Client transport layer
     * to follow the proposed migration strategy:
     * <a href="https://www.elastic.co/guide/en/elasticsearch/client/java-api-client/7.16/migrate-hlrc.html">Migrate HLRC</a>
     * It double-checks locking to get good performance and avoid double initialization
     */
    private Clients getClients() {
        if (isClosed.get()) {
            throw new IllegalStateException("Already closed");
        }

        // double-checked locking to get good performance and avoid double initialization
        if (clients == null) {
            synchronized (this) {
                if (clients == null) {
                    RestClientBuilder builder = RestClient.builder(new HttpHost(host, port, scheme));
                    if (apiKeyId != null && !apiKeyId.isEmpty() &&
                            apiKeySecret != null && !apiKeySecret.isEmpty()) {
                        String apiKeyAuth = Base64.getEncoder().encodeToString(
                                (apiKeyId + ":" + apiKeySecret).getBytes(StandardCharsets.UTF_8)
                        );
                        Header[] headers = new Header[]{new BasicHeader("Authorization", "ApiKey " + apiKeyAuth)};
                        builder.setDefaultHeaders(headers);
                    }
                    builder.setRequestConfigCallback(
                            requestConfigBuilder -> requestConfigBuilder.setSocketTimeout(ES_SOCKET_TIMEOUT));

                    RestClient httpClient = builder.build();
                    RestHighLevelClient hlClient = new RestHighLevelClientBuilder(httpClient)
                            .setApiCompatibilityMode(true).build();

                    ElasticsearchTransport transport = new RestClientTransport(
                            httpClient, new JacksonJsonpMapper());
                    ElasticsearchClient esClient = new ElasticsearchClient(transport);
                    ElasticsearchAsyncClient esAsyncClient = new ElasticsearchAsyncClient(transport);
                    clients = new Clients(esClient, esAsyncClient, hlClient);
                }
            }
        }
        return clients;
    }

    /**
     * Gets the Elasticsearch Client
     * @return the Elasticsearch client
     */
    public ElasticsearchClient getClient() {
        return getClients().client;
    }

    /**
     * Gets the Elasticsearch Asynchronous Client
     * @return the Elasticsearch client
     */
    public ElasticsearchAsyncClient getAsyncClient() {
        return getClients().asyncClient;
    }

    /**
     * @deprecated
     * @return the old Elasticsearch client
     */
    public RestHighLevelClient getOldClient() {
        return getClients().rhlClient;
    }

    public String getIndexPrefix() {
        return indexPrefix;
    }

    /**
     * Checks if elastic server is available for connection.
     * @return true if available, false otherwise.
     */
    public boolean isAvailable() {
        try {
            return this.getClient().ping().value();
        } catch (Exception e) {
            LOG.warn("Error checking connection for {}, message: {}", this, e.getMessage());
            LOG.debug("", e);
            return false;
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (clients != null) {
            if (clients.client != null) {
                // standard and async clients are based on the same transport layer. We can just close the one from the
                // standard client
                clients.client._transport().close();
            }
            if (clients.rhlClient != null) {
                clients.rhlClient.close();
            }
        }
        isClosed.set(true);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ElasticConnection that = (ElasticConnection) o;
        return port == that.port &&
                Objects.equals(scheme, that.scheme) &&
                Objects.equals(host, that.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexPrefix, scheme, host, port);
    }

    @Override
    public String toString() {
        return scheme + "://" + host + ":" + port + "/" + indexPrefix;
    }

    private static class Clients {
        public final ElasticsearchClient client;
        public final ElasticsearchAsyncClient asyncClient;
        public final RestHighLevelClient rhlClient;

        Clients(ElasticsearchClient client, ElasticsearchAsyncClient asyncClient, RestHighLevelClient rhlClient) {
            this.client = client;
            this.asyncClient = asyncClient;
            this.rhlClient = rhlClient;
        }
    }

    /**
     * Returns a new {@code Builder.IndexPrefixStep} instance to allow a step by step construction of a
     * {@link ElasticConnection} object.
     */
    public static Builder.IndexPrefixStep newBuilder() {
        return new Builder.Steps();
    }

    // Step Builder pattern
    // https://github.com/iluwatar/java-design-patterns/tree/master/step-builder
    public static final class Builder {

        private Builder() {
        }

        /**
         * First Builder Step in charge of the mandatory indexPrefix. Next Step: {@link BasicConnectionStep}.
         */
        public interface IndexPrefixStep {
            BasicConnectionStep withIndexPrefix(String indexPrefix);
        }

        /**
         * Step in charge of handling connection parameters (with default option). Next step: {@link BuildStep}.
         */
        public interface BasicConnectionStep {
            BuildStep withConnectionParameters(
                    @NotNull String scheme,
                    @NotNull String host,
                    @NotNull Integer port
            );

            BuildStep withDefaultConnectionParameters();
        }

        /**
         * Step in charge of optional steps. Next step: {@link BuildStep}.
         */
        public interface OptionalSteps {
            BuildStep withApiKeys(String id, String secret);
        }

        /**
         * This is the final step in charge of building the {@link ElasticConnection}.
         * Validation should be here.
         *
         * It adds support for {@link OptionalSteps}.
         */
        public interface BuildStep extends OptionalSteps {
            ElasticConnection build();
        }

        private static class Steps implements IndexPrefixStep, BasicConnectionStep, OptionalSteps, BuildStep {

            private String indexPrefix;

            private String scheme;
            private String host;
            private Integer port;

            private String apiKeyId;
            private String apiKeySecret;

            @Override
            public BasicConnectionStep withIndexPrefix(@NotNull String indexPrefix) {
                this.indexPrefix = indexPrefix;
                return this;
            }

            @Override
            public BuildStep withConnectionParameters(@NotNull String scheme, @NotNull String host, @NotNull Integer port) {
                this.scheme = scheme;
                this.host = host;
                this.port = port;
                return this;
            }

            @Override
            public BuildStep withDefaultConnectionParameters() {
                return withConnectionParameters(ElasticConnection.DEFAULT_SCHEME, ElasticConnection.DEFAULT_HOST, ElasticConnection.DEFAULT_PORT);
            }

            @Override
            public BuildStep withApiKeys(String id, String secret) {
                this.apiKeyId = id;
                this.apiKeySecret = secret;
                return this;
            }

            @Override
            public ElasticConnection build() {
                if (!ElasticIndexNameHelper.isValidPrefix(indexPrefix)) {
                    throw new IllegalArgumentException("The indexPrefix does not follow the elasticsearch naming convention: " + indexPrefix);
                }
                return new ElasticConnection(
                        Objects.requireNonNull(indexPrefix, "indexPrefix must be not null"),
                        Objects.requireNonNull(scheme, "scheme must be not null"),
                        Objects.requireNonNull(host, "host must be not null"),
                        Objects.requireNonNull(port, "port must be not null"),
                        apiKeyId, apiKeySecret);
            }
        }
    }
}
