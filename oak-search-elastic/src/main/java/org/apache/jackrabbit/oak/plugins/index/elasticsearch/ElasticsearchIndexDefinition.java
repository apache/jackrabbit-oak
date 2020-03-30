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
package org.apache.jackrabbit.oak.plugins.index.elasticsearch;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.jackrabbit.oak.plugins.index.search.util.ConfigUtil.getOptionalValue;
import static org.elasticsearch.common.Strings.INVALID_FILENAME_CHARS;

public class ElasticsearchIndexDefinition extends IndexDefinition implements ElasticsearchIndexConstants {

    private static final int MAX_NAME_LENGTH = 255;

    private static final String INVALID_CHARS_REGEX = Pattern.quote(INVALID_FILENAME_CHARS
            .stream()
            .map(Object::toString)
            .collect(Collectors.joining("")));

    private final String elasticsearchIndexName;

    public final int bulkActions;
    public final long bulkSizeBytes;
    public final long bulkFlushIntervalMs;
    public final int bulkRetries;
    public final long bulkRetriesBackoff;

    public ElasticsearchIndexDefinition(NodeState root, NodeState defn, String indexPath) {
        super(root, getIndexDefinitionState(defn), determineIndexFormatVersion(defn), determineUniqueId(defn), indexPath);
        this.elasticsearchIndexName = getRemoteIndexName();

        this.bulkActions = getOptionalValue(defn, BULK_ACTIONS, BULK_ACTIONS_DEFAULT);
        this.bulkSizeBytes = getOptionalValue(defn, BULK_SIZE_BYTES, BULK_SIZE_BYTES_DEFAULT);
        this.bulkFlushIntervalMs = getOptionalValue(defn, BULK_FLUSH_INTERVAL_MS, BULK_FLUSH_INTERVAL_MS_DEFAULT);
        this.bulkRetries = getOptionalValue(defn, BULK_RETRIES, BULK_RETRIES_DEFAULT);
        this.bulkRetriesBackoff = getOptionalValue(defn, BULK_RETRIES_BACKOFF, BULK_RETRIES_BACKOFF_DEFAULT);
    }

    public String getElasticsearchIndexName() {
        return elasticsearchIndexName;
    }

    private String getRemoteIndexName() {
        // TODO: implement advanced remote index name strategy that takes into account multiple tenants and re-index process
        String suffix = String.valueOf(getReindexCount());
        return getESSafeIndexName(getIndexPath() + "-" + getReindexCount());
    }

    /**
     * <ul>
     *     <li>abc -> abc</li>
     *     <li>xy:abc -> xyabc</li>
     *     <li>/oak:index/abc -> abc</li>
     * </ul>
     * <p>
     * The resulting file name would be truncated to MAX_NAME_LENGTH
     */
    private static String getESSafeIndexName(String indexPath) {
        String name = StreamSupport
                .stream(PathUtils.elements(indexPath).spliterator(), false)
                .limit(3) //Max 3 nodeNames including oak:index which is the immediate parent for any indexPath
                .filter(p -> !"oak:index".equals(p))
                .map(ElasticsearchIndexDefinition::getESSafeName)
                .collect(Collectors.joining("_"));

        if (name.length() > MAX_NAME_LENGTH) {
            name = name.substring(0, MAX_NAME_LENGTH);
        }
        return name;
    }

    /**
     * Convert {@code e} to Elasticsearch safe index name.
     * Ref: https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html
     */
    private static String getESSafeName(String suggestedIndexName) {
        return suggestedIndexName.replaceAll(INVALID_CHARS_REGEX, "").toLowerCase();
    }

    public static class Builder extends IndexDefinition.Builder {
        @Override
        public ElasticsearchIndexDefinition build() {
            return (ElasticsearchIndexDefinition)super.build();
        }

        @Override
        public ElasticsearchIndexDefinition.Builder reindex() {
            super.reindex();
            return this;
        }

        @Override
        protected IndexDefinition createInstance(NodeState indexDefnStateToUse) {
            return new ElasticsearchIndexDefinition(root, indexDefnStateToUse, indexPath);
        }
    }
}
