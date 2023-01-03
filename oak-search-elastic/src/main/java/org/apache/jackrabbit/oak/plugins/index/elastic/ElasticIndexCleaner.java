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

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.elastic.clients.elasticsearch._types.ExpandWildcard;
import co.elastic.clients.elasticsearch.cat.IndicesResponse;
import co.elastic.clients.elasticsearch.cat.indices.IndicesRecord;
import co.elastic.clients.elasticsearch.indices.DeleteIndexResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;

/**
 * Deletes those remote elastic indexes for which no index definitions exist in repository. The remote indexes are not deleted
 * the first time they are discovered. A dangling remote index is deleted in subsequent runs of this cleaner only after a
 * given threshold of time has passed since the dangling index was first discovered.
 */
public class ElasticIndexCleaner implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticIndexCleaner.class);

    private final ElasticConnection elasticConnection;
    private final NodeStore nodeStore;
    private final String indexPrefix;
    private final Map<String, Long> danglingRemoteIndicesMap;
    private final int threshold;

    /**
     * Constructs a new instance of index cleaner with the given parameters.
     * @param elasticConnection elastic connection to use
     * @param nodeStore node store where index definitions exist
     * @param thresholdInSeconds time in seconds before which a dangling remote index won't be deleted.
     */
    public ElasticIndexCleaner(ElasticConnection elasticConnection, NodeStore nodeStore, int thresholdInSeconds) {
        this.elasticConnection = elasticConnection;
        this.nodeStore = nodeStore;
        this.indexPrefix = elasticConnection.getIndexPrefix();
        danglingRemoteIndicesMap = new HashMap<>();
        this.threshold = thresholdInSeconds;
    }

    public void run() {
        try {
            NodeState root = nodeStore.getRoot();

            IndicesResponse indicesRes = elasticConnection.getClient()
                    .cat().indices(r -> r
                            .index(elasticConnection.getIndexPrefix() + "*")
                            .expandWildcards(ExpandWildcard.Open));
            String[] remoteIndices = indicesRes.valueBody()
                    .stream().map(IndicesRecord::index).toArray(String[]::new);
            if (remoteIndices.length == 0) {
                LOG.debug("No remote index found with prefix {}", indexPrefix);
                return;
            }
            // remove entry of remote index names which don't exist now
            List<String> externallyDeletedIndices = danglingRemoteIndicesMap.keySet().stream().
                    filter(index -> Arrays.stream(remoteIndices).noneMatch(remoteIndex -> remoteIndex.equals(index))).collect(Collectors.toList());
            externallyDeletedIndices.forEach(danglingRemoteIndicesMap::remove);
            Set<String> existingIndices = new HashSet<>();
            AtomicBoolean shouldReturnSilently = new AtomicBoolean(false);
            root.getChildNode(INDEX_DEFINITIONS_NAME).getChildNodeEntries().forEach(childNodeEntry -> {
                PropertyState typeProperty = childNodeEntry.getNodeState().getProperty(IndexConstants.TYPE_PROPERTY_NAME);
                String typeValue = typeProperty != null ? typeProperty.getValue(Type.STRING) : "";
                /*
                If index type is "elasticsearch" or "disabled", we try to find remote index name. In case of disabled lucene or
                property indices, the remote index name would be null. So only elasticsearch indices are affected here.
                 */
                if (ElasticIndexDefinition.TYPE_ELASTICSEARCH.equals(typeValue) || "disabled".equals(typeValue)) {
                    String indexPath = "/" + INDEX_DEFINITIONS_NAME + "/" + childNodeEntry.getName();
                    String remoteIndexName = getRemoteIndexName(indexPrefix, childNodeEntry.getNodeState(), indexPath);
                    if (remoteIndexName != null) {
                        existingIndices.add(remoteIndexName);
                    } else if (ElasticIndexDefinition.TYPE_ELASTICSEARCH.equals(typeValue)){
                        /*
                            Didn't check for disabled indexes in this "else if" condition because an index could be disabled at three stages
                            and the following should hold -
                            Stage 1 - index definition was created with type="disabled". This means indexing has not been done for index and
                            hence remote index name and the remote index itself shouldn't exist
                            Stage 2 - index was disabled after indexing was complete - in this case we would find remote index name and won't
                            enter this condition
                            Stage 3 - index was disabled after indexing started but before it was complete. Ideally this should only be done in
                            case of some error in indexing and after interrupting and pausing indexing lane. So in this case it should be safe to
                            delete the partially created remote index as it should be recreated by reindexing.
                         */
                        LOG.info("Could not obtain remote index name for {}. Won't delete any index.", childNodeEntry.getName());
                        shouldReturnSilently.set(true);
                    }
                }
            });
            if (shouldReturnSilently.get()) {
                return;
            }
            List<String> indicesToDelete = new ArrayList<>();
            for (String remoteIndexName : remoteIndices) {
                if (!existingIndices.contains(remoteIndexName)) {
                    Long curTime = System.currentTimeMillis();
                    Long oldTime = danglingRemoteIndicesMap.putIfAbsent(remoteIndexName, curTime);
                    if (threshold == 0 || (oldTime != null && curTime - oldTime >= TimeUnit.SECONDS.toMillis(threshold))) {
                        indicesToDelete.add(remoteIndexName);
                        danglingRemoteIndicesMap.remove(remoteIndexName);
                    }
                } else {
                    danglingRemoteIndicesMap.remove(remoteIndexName);
                }
            }
            if(!indicesToDelete.isEmpty()) {
                DeleteIndexResponse response = elasticConnection.getClient().indices().delete(i -> i.index(indicesToDelete));
                LOG.info("Deleting remote indices {}", indicesToDelete);
                if (!response.acknowledged()) {
                    LOG.error("Could not delete remote indices " + indicesToDelete);
                }
            }
        } catch (IOException e) {
            LOG.error("Could not delete remote indices", e);
        }
    }

    protected static @Nullable String getRemoteIndexName(String indexPrefix, NodeState indexNode, String indexPath) {
        PropertyState nodeTypeProp = indexNode.getProperty(JcrConstants.JCR_PRIMARYTYPE);
        if (nodeTypeProp == null || !IndexConstants.INDEX_DEFINITIONS_NODE_TYPE.equals(nodeTypeProp.getValue(Type.STRING))) {
            throw new IllegalArgumentException("Not an index definition node state");
        }
        PropertyState type = indexNode.getProperty(IndexConstants.TYPE_PROPERTY_NAME);
        String typeValue = type != null ? type.getValue(Type.STRING) : "";
        if (!ElasticIndexDefinition.TYPE_ELASTICSEARCH.equals(typeValue) && !"disabled".equals(typeValue)) {
            throw new IllegalArgumentException("Not an elastic index node");
        }
        try {
            return ElasticIndexNameHelper.getRemoteIndexName(indexPrefix, indexPath, indexNode.builder());
        } catch (IllegalStateException ise) { // this happens when there is no seed for the index
            return null;
        }
    }
}
