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

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * This class represents an Elasticsearch Connection with the related <code>RestHighLevelClient</code>.
 * As per Elasticsearch documentation: the client is thread-safe, there should be one instance per application and it
 * must be closed when it is not needed anymore.
 * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/_changing_the_client_8217_s_initialization_code.html
 *
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

    protected static final Supplier<ElasticsearchConnection> defaultConnection = () ->
            new ElasticsearchConnection(DEFAULT_SCHEME, DEFAULT_HOST, DEFAULT_PORT, "elastic");

    private String scheme;
    private String host;
    private int port;
    private final String indexPrefix;

    private volatile RestHighLevelClient client;

    private AtomicBoolean isClosed = new AtomicBoolean(false);

    public ElasticsearchConnection(String scheme, String host, Integer port, String indexPrefix) {
        if (scheme == null || host == null || port == null || indexPrefix == null) {
            throw new IllegalArgumentException();
        }
        this.scheme = scheme;
        this.host = host;
        this.port = port;
        this.indexPrefix = indexPrefix;
    }

    public RestHighLevelClient getClient() {
        if (isClosed.get()) {
            throw new IllegalStateException("Already closed");
        }

        // double checked locking to get good performance and avoid double initialization
        if (client == null) {
            synchronized (this) {
                if (client == null) {
                    client = new RestHighLevelClient(RestClient.builder(new HttpHost(host, port, scheme)));
                }
            }
        }
        return client;
    }

    public String getScheme() {
        return scheme;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
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
        return Objects.hash(getScheme(), getHost(), getPort());
    }

    @Override
    public String toString() {
        return getScheme() + "://" + getHost() + ":" + getPort();
    }

}
