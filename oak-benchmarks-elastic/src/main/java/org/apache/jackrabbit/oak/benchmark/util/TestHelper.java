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
package org.apache.jackrabbit.oak.benchmark.util;

import co.elastic.clients.elasticsearch.indices.DeleteIndexResponse;
import co.elastic.clients.elasticsearch.indices.GetAliasResponse;
import co.elastic.clients.elasticsearch.indices.get_alias.IndexAliases;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexNameHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class TestHelper {

    private static final Logger LOG = LoggerFactory.getLogger(TestHelper.class);
    /**
     * Generates a unique index name from the given suggestion.
     * @param name name suggestion
     * @return unique index name
     */
    public static String getUniqueIndexName(String name) {
        return name + System.currentTimeMillis();
    }

    /*
    Deletes the remote elastic index from the elastic server.
     */
    public static void cleanupRemoteElastic(ElasticConnection connection, String indexName) throws IOException {
        String alias =  ElasticIndexNameHelper.getElasticSafeIndexName(connection.getIndexPrefix(), "/oak:index/" + indexName);
        // get and delete the indexes which this alias is pointing to
        GetAliasResponse aliasResponse = connection.getClient().indices().getAlias(fn -> fn.name(alias));
        Map<String, IndexAliases> aliases = aliasResponse.result();
        for (String remoteIndexName : aliases.keySet()) {
            DeleteIndexResponse deleteIndexResponse = connection.getClient().indices().
                    delete(fn -> fn.index(remoteIndexName));
            if (!deleteIndexResponse.acknowledged()) {
                LOG.warn("Delete index call not acknowledged for index " + remoteIndexName + " .Please check if remote index deleted or not.");
            }
        }
    }

}
