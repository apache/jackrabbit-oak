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

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.common.Strings.INVALID_FILENAME_CHARS;

public class ElasticIndexNameHelper {

    private static final int MAX_NAME_LENGTH = 255;

    private static final String INVALID_CHARS_REGEX = Pattern.quote(INVALID_FILENAME_CHARS
            .stream()
            .map(Object::toString)
            .collect(Collectors.joining("")));

    public static String getIndexAlias(String indexPrefix, String indexPath) {
        // TODO: implement advanced remote index name strategy that takes into account multiple tenants and re-index process
        return getElasticSafeIndexName(indexPrefix + "." + indexPath);
    }

    public static @Nullable String getRemoteIndexName(String indexPrefix, NodeState indexNode, String indexPath) {
        PropertyState nodeTypeProp = indexNode.getProperty(JcrConstants.JCR_PRIMARYTYPE);
        if (nodeTypeProp == null || !IndexConstants.INDEX_DEFINITIONS_NODE_TYPE.equals(nodeTypeProp.getValue(Type.STRING))) {
            throw new IllegalArgumentException("Not an index definition node state");
        }
        PropertyState type = indexNode.getProperty(IndexConstants.TYPE_PROPERTY_NAME);
        String typeValue = type != null ? type.getValue(Type.STRING) : "";
        if (!ElasticIndexDefinition.TYPE_ELASTICSEARCH.equals(typeValue) && !"disabled".equals(typeValue)) {
            throw new IllegalArgumentException("Not an elastic index node");
        }
        PropertyState seedProp = indexNode.getProperty(ElasticIndexDefinition.PROP_INDEX_NAME_SEED);
        if (seedProp == null) {
            return null;
        }
        long seed = seedProp.getValue(Type.LONG);
        String indexAlias = getIndexAlias(indexPrefix, indexPath);
        return getRemoteIndexName(indexAlias, seed);
    }

    /**
     * Create a name for remote elastic index from given index definition and seed.
     * @param indexDefinition elastic index definition to use
     * @param seed seed to use
     * @return remote elastic index name
     */
    public static String getRemoteIndexName(ElasticIndexDefinition indexDefinition, long seed) {
        return getElasticSafeIndexName(
                indexDefinition.getRemoteIndexAlias() + "-" + Long.toHexString(seed));
    }

    /**
     * Create a name for remote elastic index from given index definition and a randomly generated seed.
     * @param indexDefinition elastic index definition to use
     * @return remote elastic index name
     */
    public static String getRemoteIndexName(ElasticIndexDefinition indexDefinition) {
        return getRemoteIndexName(indexDefinition, UUID.randomUUID().getMostSignificantBits());
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
    private static String getElasticSafeIndexName(String indexPath) {
        String name = StreamSupport
                .stream(PathUtils.elements(indexPath).spliterator(), false)
                .limit(3) //Max 3 nodeNames including oak:index which is the immediate parent for any indexPath
                .filter(p -> !"oak:index".equals(p))
                .map(ElasticIndexNameHelper::getElasticSafeName)
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
    static String getElasticSafeName(String suggestedIndexName) {
        return suggestedIndexName.replaceAll(INVALID_CHARS_REGEX, "").toLowerCase();
    }

    private static String getRemoteIndexName(String indexAlias, long seed) {
        return getElasticSafeIndexName(indexAlias + "-" + Long.toHexString(seed));
    }
}
