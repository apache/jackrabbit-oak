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
package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.importer.IndexImporterProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexPathServiceImpl;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Set;

public class ElasticIndexImporter implements IndexImporterProvider {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticIndexImporter.class);
    private final ElasticConnection connection;

    public ElasticIndexImporter(){
        this(null);
    }

    public ElasticIndexImporter(ElasticConnection connection) {
        this.connection = connection;
    }

    @Override
    public void importIndex(NodeState root, NodeBuilder definitionBuilder, File indexDir) throws IOException, CommitFailedException {
        // NOOP for elastic
        LOG.info("No need to import data in case of elastic since this is a remote index. Exiting with NOOP");
    }

    @Override
    public String getType() {
        return ElasticIndexDefinition.TYPE_ELASTICSEARCH;
    }

    @Override
    public void cleanupFailedImport(NodeStore store, String path, NodeState definition) throws IOException {
        if (connection == null) {
            throw new IOException("No Elasticsearch connection available for failed-import cleanup of " + path);
        }
        if (!ElasticIndexDefinition.TYPE_ELASTICSEARCH.equals(definition.getString("type"))
                || !definition.hasProperty(ElasticIndexDefinition.PROP_INDEX_NAME_SEED)) {
            throw new IOException("Cannot establish generated Elasticsearch index identity for " + path);
        }
        String prefix = connection.getIndexPrefix();
        String remote = ElasticIndexNameHelper.getRemoteIndexName(prefix, path, definition.builder());
        String alias = ElasticIndexNameHelper.getElasticSafeIndexName(prefix, path);
        if (remote.contains("*") || remote.contains("?") || remote.contains(",")
                || alias.contains("*") || alias.contains("?") || alias.contains(",")) {
            throw new IOException("Refusing non-exact Elasticsearch cleanup of " + remote);
        }
        NodeState root = store.getRoot();
        if (NodeStateUtils.getNode(root, path).exists()) {
            throw new IOException("Definition still exists; refusing Elasticsearch cleanup of " + path);
        }
        NodeState indexes = root.getChildNode("oak:index");
        for (String name : indexes.getChildNodeNames()) {
            if (references(indexes.getChildNode(name), "/oak:index/" + name, remote, alias)) {
                LOG.warn("Retaining generated ES index {}: another definition references its backing index or alias", remote);
                return;
            }
        }
        boolean allPathsIndexed = false;
        for (String type : indexes.getChildNode("nodetype").getNames("declaringNodeTypes")) {
            if ("oak:QueryIndexDefinition".equals(type)) {
                allPathsIndexed = true;
                break;
            }
        }
        if (!allPathsIndexed) {
            LOG.warn("Retaining generated ES index {}: non-root references cannot be ruled out", remote);
            return;
        }
        for (String indexPath : new IndexPathServiceImpl(store).getIndexPaths()) {
            if (references(NodeStateUtils.getNode(root, indexPath), indexPath, remote, alias)) {
                LOG.warn("Retaining generated ES index {}: another definition references its backing index or alias", remote);
                return;
            }
        }
        var indices = connection.getClient().indices();
        if (!indices.exists(r -> r.index(remote)).value()) {
            return;
        }
        var aliases = indices.getAlias(r -> r.index(remote)).aliases();
        if (!aliases.keySet().equals(Set.of(remote))
                || !Set.of(alias).containsAll(aliases.get(remote).aliases().keySet())) {
            LOG.warn("Retaining generated ES index {}: alias ownership is unexpected or ambiguous", remote);
            return;
        }
        if (!aliases.get(remote).aliases().isEmpty()) {
            var targets = indices.getAlias(r -> r.name(alias)).aliases();
            if (!targets.keySet().equals(Set.of(remote))
                    || !targets.get(remote).aliases().keySet().equals(Set.of(alias))) {
                LOG.warn("Retaining generated ES index {}: alias {} is shared or changed", remote, alias);
                return;
            }
        }
        if (!indices.delete(r -> r.index(remote)).acknowledged()
                || indices.exists(r -> r.index(remote)).value()) {
            throw new IOException("Generated Elasticsearch index deletion was not completed: " + remote);
        }
        LOG.info("Removed abandoned generated ES index {} and its exclusively owned alias", remote);
    }

    private boolean references(NodeState definition, String path, String remote, String alias) {
        String prefix = connection.getIndexPrefix();
        return ElasticIndexDefinition.TYPE_ELASTICSEARCH.equals(definition.getString("type"))
                && (alias.equals(ElasticIndexNameHelper.getElasticSafeIndexName(prefix, path))
                    || (definition.hasProperty(ElasticIndexDefinition.PROP_INDEX_NAME_SEED)
                        && remote.equals(ElasticIndexNameHelper.getRemoteIndexName(prefix, path, definition.builder()))));
    }
}
