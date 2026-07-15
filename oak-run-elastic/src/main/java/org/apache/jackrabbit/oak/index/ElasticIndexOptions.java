/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.index;

import joptsimple.OptionParser;
import joptsimple.OptionSpec;
import org.apache.jackrabbit.oak.run.cli.OptionsBeanFactory;

/*
Oak Run options for Elasticsearch indexing. These are primarily used to create the connection to elasticsearch.
 */
public class ElasticIndexOptions extends IndexOptions {

    public static final OptionsBeanFactory FACTORY = ElasticIndexOptions::new;

    private final OptionSpec<String> scheme;
    private final OptionSpec<String> host;
    private final OptionSpec<String> apiKeyId;
    private final OptionSpec<String> apiKeySecret;
    private final OptionSpec<Integer> port;
    private final OptionSpec<String> indexPrefix;


    public ElasticIndexOptions(OptionParser parser) {
        super(parser);
        scheme = parser.accepts("scheme", "Elastic scheme")
                .withRequiredArg().ofType(String.class);
        host = parser.accepts("host", "Elastic host")
                .withRequiredArg().ofType(String.class);
        port = parser.accepts("port", "Elastic port")
                .withRequiredArg().ofType(Integer.class);
        apiKeyId = parser.accepts("apiKeyId", "Elastic api key id")
                .withRequiredArg().ofType(String.class);
        apiKeySecret = parser.accepts("apiKeySecret", "Elastic api key host")
                .withRequiredArg().ofType(String.class);
        indexPrefix = parser.accepts("indexPrefix", "Elastic indexPrefix")
                .withRequiredArg().ofType(String.class);
    }

    public String getElasticScheme() {
        return scheme.value(options);
    }

    public int getElasticPort() {
        return port.value(options);
    }

    public String getElasticHost() {
        return host.value(options);
    }

    public String getApiKeyId() {
        return apiKeyId.value(options);
    }

    public String getApiKeySecret() {
        return apiKeySecret.value(options);
    }

    public String getIndexPrefix() {
        return indexPrefix.value(options);
    }

}
