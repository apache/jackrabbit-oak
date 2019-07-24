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

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.elasticsearch.client.RestHighLevelClient;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.common.Strings.INVALID_FILENAME_CHARS;

public class ElasticsearchIndexCoordinateImpl implements ElasticsearchIndexCoordinate {

    private static final int MAX_NAME_LENGTH = 255;

    private final ElasticsearchCoordinate esCoord;
    private final String esIndexName;

    ElasticsearchIndexCoordinateImpl(@NotNull ElasticsearchCoordinate esCoord, IndexDefinition indexDefinition) {
        this.esCoord = esCoord;
        esIndexName = getRemoteIndexName(indexDefinition, indexDefinition.getIndexPath());
    }

    @Override
    public RestHighLevelClient getClient() {
        return esCoord.getClient();
    }

    @Override
    public String getEsIndexName() {
        return esIndexName;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(esCoord, esIndexName);
    }

    @Override
    public boolean equals(Object o) {
        if (! (o instanceof ElasticsearchIndexCoordinateImpl)) {
            return false;
        }
        ElasticsearchIndexCoordinateImpl other = (ElasticsearchIndexCoordinateImpl)o;
        return hashCode() == other.hashCode()
                && esCoord.equals(other.esCoord)
                && esIndexName.equals(other.esIndexName);
    }

    private String getRemoteIndexName(IndexDefinition definition, String indexPath) {
        String suffix = definition.getUniqueId();

        if (suffix == null) {
            suffix = String.valueOf(definition.getReindexCount());
        }

        return getESSafeIndexName(indexPath + "-" + suffix);
    }

    /**
     * <ul>
     *     <li>abc -> abc</li>
     *     <li>xy:abc -> xyabc</li>
     *     <li>/oak:index/abc -> abc</li>
     * </ul>
     *
     * The resulting file name would be truncated to MAX_NAME_LENGTH
     */
    private static String getESSafeIndexName(String indexPath) {
        List<String> elements = Lists.newArrayList(PathUtils.elements(indexPath));
        Collections.reverse(elements);
        List<String> result = Lists.newArrayListWithCapacity(2);

        //Max 3 nodeNames including oak:index which is the immediate parent for any indexPath
        for (String e : Iterables.limit(elements, 3)) {
            if ("oak:index".equals(e)) {
                continue;
            }

            result.add(getESSafeName(e));
        }

        Collections.reverse(result);
        String name = Joiner.on('_').join(result);
        if (name.length() > MAX_NAME_LENGTH){
            name = name.substring(0, MAX_NAME_LENGTH);
        }
        return name;
    }

    /**
     * Convert {@code e} to Elasticsearch safe index name.
     * Ref: https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html
     */
    private static String getESSafeName(String suggestedIndexName) {
        String invalidCharsRegex = Pattern.quote(String.join("", INVALID_FILENAME_CHARS
                .stream()
                .map(Object::toString)
                .collect(Collectors.toList())));
        return suggestedIndexName.replaceAll(invalidCharsRegex, "").toLowerCase();
    }

}
