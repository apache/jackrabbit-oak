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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ElasticIndexNameHelper {

    private static final int MAX_NAME_LENGTH = 255;

    // revised version of org.elasticsearch.common.Strings.INVALID_FILENAME_CHARS with additional chars:
    // ':' not supported in >= 7.0
    private static final Set<Character> INVALID_NAME_CHARS =
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList('\\', '/', '*', '?', '"', '<', '>', '|', ' ', ',', ':')));

    private static final Pattern INVALID_CHARS_REGEX = Pattern.compile(INVALID_NAME_CHARS
            .stream()
            .map(c -> "(?:(?:\\" + c + "))")
            .collect(Collectors.joining("|")));

    // these chars can be part of the name but are not allowed at the beginning
    private static final Set<Character> INVALID_NAME_START_CHARS =
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList('.', '-', '_', '+')));

    private static final Pattern INVALID_START_CHARS_REGEX = Pattern.compile(INVALID_NAME_START_CHARS
            .stream()
            .map(c -> "\\" + c)
            .collect(Collectors.joining("", "^[", "]+")));

    public static String getRemoteIndexName(String indexPrefix, String indexName, long seed) {
        return getElasticSafeIndexName(indexPrefix, indexName + "-" + Long.toHexString(seed));
    }

    public static String getRemoteIndexName(String indexPrefix, String indexName, NodeBuilder definitionBuilder) {
        PropertyState seedProp = definitionBuilder.getProperty(ElasticIndexDefinition.PROP_INDEX_NAME_SEED);
        if (seedProp == null) {
            throw new IllegalStateException("Index full name cannot be computed without name seed");
        }
        long seed = seedProp.getValue(Type.LONG);
        return getRemoteIndexName(indexPrefix, indexName, seed);
    }

    /**
     * Returns a name that can be safely used as index name in Elastic.
     *
     * Examples:
     * <ul>
     *     <li>prefix, abc -> prefix.abc</li>
     *     <li>prefix, xy:abc -> prefix.xyabc</li>
     *     <li>prefix, /oak:index/abc -> prefix.abc</li>
     * </ul>
     * <p>
     * The resulting file name would be truncated to {@link #MAX_NAME_LENGTH}
     *
     * @param indexPrefix the prefix of the index. This value does not get validated at this stage since it comes from
     *                    outside and the validation already happened in {@link ElasticConnection.Builder} when the
     *                    entire module gets initialized.
     * @param indexPath the path of the index that will be checked, eventually transformed and appended to the prefix
     */
    public static String getElasticSafeIndexName(String indexPrefix, String indexPath) {
        String name = indexPrefix + "." + StreamSupport
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
    private static String getElasticSafeName(String suggestedIndexName) {
        return INVALID_CHARS_REGEX.matcher(suggestedIndexName).replaceAll("").toLowerCase();
    }

    static boolean isValidPrefix(@NotNull String prefix) {
        if (!prefix.equals("")) {
            return prefix.equals(prefix.toLowerCase()) && // it has to be lowercase
                    !INVALID_START_CHARS_REGEX.matcher(prefix).find() && // not start with specific chars
                    !INVALID_CHARS_REGEX.matcher(prefix).find(); // not contain specific chars
        }
        return false;
    }
}
