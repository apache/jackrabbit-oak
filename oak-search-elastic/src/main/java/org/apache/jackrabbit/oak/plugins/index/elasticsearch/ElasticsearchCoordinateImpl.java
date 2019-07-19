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

import com.google.common.base.Objects;
import org.apache.jackrabbit.oak.plugins.index.search.util.ConfigUtil;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.Map;

public class ElasticsearchCoordinateImpl implements ElasticsearchCoordinate {
    private final ElasticsearchConnectionFactory connectionFactory;
    private final String scheme;
    private final String host;
    private final int port;

    ElasticsearchCoordinateImpl(ElasticsearchConnectionFactory connectionFactory,
                                String scheme, String host, int port) {
        this.connectionFactory = connectionFactory;
        this.scheme = scheme;
        this.host = host;
        this.port = port;
    }

    static ElasticsearchCoordinate construct(ElasticsearchConnectionFactory connectionFactory,
                                             NodeState indexDefn, Map<String, String> configMap) {
        ElasticsearchCoordinate esCoord;

        // index defn is at highest prio
        esCoord = readFrom(connectionFactory, indexDefn);
        if (esCoord != null) {
            return esCoord;
        }

        // command line comes next
        esCoord = construct(connectionFactory,
                System.getProperty(SCHEME_PROP), System.getProperty(HOST_PROP), Integer.getInteger(PORT_PROP));
        if (esCoord != null) {
            return esCoord;
        }

        // config map
        if (configMap != null) {
            Integer port = null;
            try {
                port = Integer.parseInt(configMap.get(PORT_PROP));
            } catch (NumberFormatException nfe) {
                // ignore
            }
            esCoord = construct(connectionFactory, configMap.get(SCHEME_PROP), configMap.get(HOST_PROP), port);
            if (esCoord != null) {
                return esCoord;
            }
        }

        return new ElasticsearchCoordinateImpl(connectionFactory, DEFAULT_SCHEME, DEFAULT_HOST, DEFAULT_PORT);
    }

    @Override
    public RestHighLevelClient getClient() {
        return connectionFactory.getConnection(this);
    }

    @Override
    public String getScheme() {
        return scheme;
    }

    @Override
    public String getHost() {
        return host;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ElasticsearchCoordinate)) {
            return false;
        }

        ElasticsearchCoordinate other = (ElasticsearchCoordinate)o;
        return hashCode() == other.hashCode() // just to have a quicker comparison
                && getScheme().equals(other.getScheme())
                && getHost().equals(other.getHost())
                && getPort() == other.getPort();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getScheme(), getHost(), getPort());
    }

    @Override
    public String toString() {
        return getScheme() + "://" + getHost() + ":" + getPort();
    }

    private static ElasticsearchCoordinate readFrom(ElasticsearchConnectionFactory factory, NodeState definition) {
        if (definition == null
                || !definition.hasProperty(SCHEME_PROP)
                || !definition.hasProperty(HOST_PROP)
                || !definition.hasProperty(PORT_PROP)) {
            return null;
        }

        String scheme = ConfigUtil.getOptionalValue(definition, SCHEME_PROP, DEFAULT_SCHEME);
        String host = ConfigUtil.getOptionalValue(definition, HOST_PROP, DEFAULT_HOST);
        int port = ConfigUtil.getOptionalValue(definition, PORT_PROP, DEFAULT_PORT);

        return new ElasticsearchCoordinateImpl(factory, scheme, host, port);
    }

    private static ElasticsearchCoordinate construct(ElasticsearchConnectionFactory factory,
                                                     String scheme, String host, Integer port) {
        if (scheme == null || host == null || port == null) {
            return null;
        }

        return new ElasticsearchCoordinateImpl(factory, scheme, host, port);
    }
}
