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
package org.apache.jackrabbit.oak.plugins.index.elasticsearch;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.jetbrains.annotations.NotNull;

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
 * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/_changing_the_client_8217_s_initialization_code.html
 * <p>
 * The getClient() initializes the rest client on the first call.
 * Once close() is invoked this instance cannot be used anymore.
 */
public class ElasticsearchConnection implements Closeable {

    protected static final String SCHEME_PROP = "elasticsearch.scheme";
    protected static final String DEFAULT_SCHEME = "http";
    protected static final String HOST_PROP = "elasticsearch.host";
    protected static final String DEFAULT_HOST = "127.0.0.1";
    protected static final String PORT_PROP = "elasticsearch.port";
    protected static final int DEFAULT_PORT = 9200;
    protected static final String API_KEY_ID_PROP = "elasticsearch.apiKeyId";
    protected static final String DEFAULT_API_KEY_ID = "";
    protected static final String API_KEY_SECRET_PROP = "elasticsearch.apiKeySecret";
    protected static final String DEFAULT_API_KEY_SECRET = "";

    private final String indexPrefix;
    private final String scheme;
    private final String host;
    private final int port;

    // API key credentials
    private final String apiKeyId;
    private final String apiKeySecret;

    private volatile RestHighLevelClient client;

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
     * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api.html#security-api-keys">
     * Elasticsearch Security API Keys
     * </a>
     */
    private ElasticsearchConnection(@NotNull String indexPrefix, @NotNull String scheme, @NotNull String host,
                                    @NotNull Integer port, String apiKeyId, String apiKeySecret) {
        this.indexPrefix = indexPrefix;
        this.scheme = scheme;
        this.host = host;
        this.port = port;
        this.apiKeyId = apiKeyId;
        this.apiKeySecret = apiKeySecret;
    }

    public RestHighLevelClient getClient() {
        if (isClosed.get()) {
            throw new IllegalStateException("Already closed");
        }

        // double checked locking to get good performance and avoid double initialization
        if (client == null) {
            synchronized (this) {
                if (client == null) {
                    RestClientBuilder builder = RestClient.builder(new HttpHost(host, port, scheme));
                    if (apiKeyId != null && !apiKeyId.isEmpty() &&
                            apiKeySecret != null && !apiKeySecret.isEmpty()) {
                        String apiKeyAuth = Base64.getEncoder().encodeToString(
                                (apiKeyId + ":" + apiKeySecret).getBytes(StandardCharsets.UTF_8)
                        );
                        Header[] headers = new Header[]{new BasicHeader("Authorization", "ApiKey " + apiKeyAuth)};
                        builder.setDefaultHeaders(headers);
                    }
                    client = new RestHighLevelClient(builder);
                }
            }
        }
        return client;
    }

    public String getIndexPrefix() {
        return indexPrefix;
    }

    @Override
    public synchronized void close() throws IOException {
        if (client != null) {
            client.close();
        }
        isClosed.set(true);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ElasticsearchConnection that = (ElasticsearchConnection) o;
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

    /**
     * Returns a new {@code Builder.IndexPrefixStep} instance to allow a step by step construction of a
     * {@link ElasticsearchConnection} object.
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
         * This is the final step in charge of building the {@link ElasticsearchConnection}.
         * Validation should be here.
         *
         * It adds support for {@link OptionalSteps}.
         */
        public interface BuildStep extends OptionalSteps {
            ElasticsearchConnection build();
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
                return withConnectionParameters(ElasticsearchConnection.DEFAULT_SCHEME, ElasticsearchConnection.DEFAULT_HOST, ElasticsearchConnection.DEFAULT_PORT);
            }

            @Override
            public BuildStep withApiKeys(String id, String secret) {
                this.apiKeyId = id;
                this.apiKeySecret = secret;
                return this;
            }

            @Override
            public ElasticsearchConnection build() {
                return new ElasticsearchConnection(
                        Objects.requireNonNull(indexPrefix, "indexPrefix must be not null"),
                        Objects.requireNonNull(scheme, "scheme must be not null"),
                        Objects.requireNonNull(host, "host must be not null"),
                        Objects.requireNonNull(port, "port must be not null"),
                        apiKeyId, apiKeySecret);
            }
        }
    }
}
