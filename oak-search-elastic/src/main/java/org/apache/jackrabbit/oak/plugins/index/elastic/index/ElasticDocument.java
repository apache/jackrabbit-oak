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
package org.apache.jackrabbit.oak.plugins.index.elastic.index;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.codec.digest.MurmurHash3;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.spi.binary.BlobByteSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.LinkedHashSet;

import static org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexUtils.toDoubles;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class ElasticDocument {

    @JsonProperty(FieldNames.PATH)
    public final String path;
    @JsonProperty(ElasticIndexDefinition.PATH_RANDOM_VALUE)
    public final int pathRandomValue;
    @JsonProperty(FieldNames.FULLTEXT)
    public final Set<String> fulltext;
    @JsonProperty(FieldNames.SUGGEST)
    public final Set<Map<String, String>> suggest;
    @JsonProperty(FieldNames.SPELLCHECK)
    public final Set<String> spellcheck;
    @JsonProperty(ElasticIndexDefinition.DYNAMIC_BOOST_FULLTEXT)
    public final Set<String> dbFullText;
    @JsonProperty(ElasticIndexDefinition.SIMILARITY_TAGS)
    public final Set<String> similarityTags;
    // these are dynamic properties that need to be added to the document unwrapped. See the use of @JsonAnyGetter in the getter
    // TODO: to support strict mapping, these properties should be part of dynamicProperties
    private final Map<String, Object> properties;
    @JsonProperty(ElasticIndexDefinition.DYNAMIC_PROPERTIES)
    private final List<Map<String, Object>> dynamicProperties;

    ElasticDocument(String path) {
        this(path, 0);
    }

    ElasticDocument(String path, int seed) {
        this.path = path;
        byte[] pathBytes = path.getBytes(StandardCharsets.UTF_8);
        this.pathRandomValue = MurmurHash3.hash32x86(pathBytes, 0, pathBytes.length, seed);
        this.fulltext = new LinkedHashSet<>();
        this.suggest = new LinkedHashSet<>();
        this.spellcheck = new LinkedHashSet<>();
        this.properties = new HashMap<>();
        this.dynamicProperties = new ArrayList<>();
        this.dbFullText = new LinkedHashSet<>();
        this.similarityTags = new LinkedHashSet<>();
    }

    void addFulltext(String value) {
        fulltext.add(value);
    }

    void addFulltextRelative(String path, String value) {
        dynamicProperties.stream().filter(map -> map.get(ElasticIndexHelper.DYNAMIC_PROPERTY_NAME).equals(path))
                .findFirst()
                .ifPresentOrElse(
                        map -> {
                            Object existingValue = map.get(ElasticIndexHelper.DYNAMIC_PROPERTY_VALUE);
                            if (existingValue instanceof Set) {
                                Set<Object> existingSet = (Set<Object>) existingValue;
                                existingSet.add(value);
                            } else {
                                Set<Object> set = new LinkedHashSet<>();
                                set.add(existingValue);
                                set.add(value);
                                map.put(ElasticIndexHelper.DYNAMIC_PROPERTY_VALUE, set);
                            }
                        },
                        () -> {
                            Map<String, Object> newMap = new HashMap<>();
                            newMap.put(ElasticIndexHelper.DYNAMIC_PROPERTY_NAME, path);
                            newMap.put(ElasticIndexHelper.DYNAMIC_PROPERTY_VALUE, value);
                            dynamicProperties.add(newMap);
                        }
                );
    }

    void addSuggest(String value) {
        suggest.add(Map.of(ElasticIndexHelper.SUGGEST_NESTED_VALUE, value));
    }

    void addSpellcheck(String value) {
        spellcheck.add(value);
    }

    // ES for String values (that are not interpreted as date or numbers etc.) would analyze in the same
    // field and would index a sub-field "keyword" for non-analyzed value.
    // ref: https://www.elastic.co/blog/strings-are-dead-long-live-strings
    // (interpretation of date etc.: https://www.elastic.co/guide/en/elasticsearch/reference/current/dynamic-field-mapping.html)
    void addProperty(String fieldName, Object value) {
        Object existingValue = properties.get(fieldName);
        Object finalValue;

        if (existingValue == null) {
            finalValue = value;
        } else if (existingValue instanceof Set) {
            Set<Object> existingSet = (Set<Object>) existingValue;
            existingSet.add(value);
            finalValue = existingSet;
        } else {
            Set<Object> set = new LinkedHashSet<>();
            set.add(existingValue);
            set.add(value);
            finalValue = set.size() == 1 ? set.iterator().next() : set;
        }

        properties.put(fieldName, finalValue);
    }

    void addSimilarityField(String name, Blob value) throws IOException {
        byte[] bytes = new BlobByteSource(value).read();
        addProperty(FieldNames.createSimilarityFieldName(name), toDoubles(bytes));
    }

    void indexAncestors(String path) {
        String parPath = PathUtils.getParentPath(path);
        int depth = PathUtils.getDepth(path);

        addProperty(FieldNames.ANCESTORS, parPath);
        addProperty(FieldNames.PATH_DEPTH, depth);
    }

    void addDynamicBoostField(String propName, String value, double boost) {
        addProperty(propName,
                Map.of(
                        ElasticIndexHelper.DYNAMIC_BOOST_NESTED_VALUE, value,
                        ElasticIndexHelper.DYNAMIC_BOOST_NESTED_BOOST, boost
                )
        );

        // add value into the dynamic boost specific fulltext field. We cannot add this in the standard
        // field since dynamic boosted terms require lower weight compared to standard terms
        dbFullText.add(value);
    }

    void addSimilarityTag(String value) {
        similarityTags.add(value);
    }

    @JsonAnyGetter
    public Map<String, Object> getProperties() {
        return properties;
    }

}
