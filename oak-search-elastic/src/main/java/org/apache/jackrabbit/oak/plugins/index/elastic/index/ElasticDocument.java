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

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.spi.binary.BlobByteSource;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.ArrayList;
import java.util.LinkedHashSet;

import static org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexUtils.toDoubles;

public class ElasticDocument {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticDocument.class);

    private final String path;
    private final Set<String> fulltext;
    private final Set<String> suggest;
    private final Set<String> spellcheck;
    private final Map<String, List<Object>> properties;
    private final Map<String, Object> similarityFields;
    private final Map<String, Map<String, Double>> dynamicBoostFields;
    private final Set<String> similarityTags;

    ElasticDocument(String path) {
        this.path = path;
        this.fulltext = new LinkedHashSet<>();
        this.suggest = new LinkedHashSet<>();
        this.spellcheck = new LinkedHashSet<>();
        this.properties = new HashMap<>();
        this.similarityFields = new HashMap<>();
        this.dynamicBoostFields = new HashMap<>();
        this.similarityTags = new LinkedHashSet<>();
    }

    void addFulltext(String value) {
        fulltext.add(value);
    }

    void addFulltextRelative(String path, String value) {
        addProperty(FieldNames.createFulltextFieldName(path), value);
    }

    void addSuggest(String value) {
        suggest.add(value);
    }

    void addSpellcheck(String value) {
        spellcheck.add(value);
    }

    // ES for String values (that are not interpreted as date or numbers etc) would analyze in the same
    // field and would index a sub-field "keyword" for non-analyzed value.
    // ref: https://www.elastic.co/blog/strings-are-dead-long-live-strings
    // (interpretation of date etc: https://www.elastic.co/guide/en/elasticsearch/reference/current/dynamic-field-mapping.html)
    void addProperty(String fieldName, Object value) {
        properties.computeIfAbsent(fieldName, s -> new ArrayList<>()).add(value);
    }

    void addSimilarityField(String name, Blob value) throws IOException {
        byte[] bytes = new BlobByteSource(value).read();
        similarityFields.put(FieldNames.createSimilarityFieldName(name), toDoubles(bytes));
    }

    void indexAncestors(String path) {
        String parPath = PathUtils.getParentPath(path);
        int depth = PathUtils.getDepth(path);

        addProperty(FieldNames.ANCESTORS, parPath);
        addProperty(FieldNames.PATH_DEPTH, depth);
    }

    void addDynamicBoostField(String propName, String value, double boost) {
        dynamicBoostFields.computeIfAbsent(propName, s -> new HashMap<>())
                .putIfAbsent(value, boost);
    }

    void addSimilarityTag(String value) {
        similarityTags.add(value);
    }

    public String build() {
        String ret;
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.field(FieldNames.PATH, path);
                for (Map.Entry<String, Map<String, Double>> f : dynamicBoostFields.entrySet()) {
                    builder.startArray(f.getKey());
                    for (Map.Entry<String, Double> v : f.getValue().entrySet()) {
                        builder.startObject();
                        builder.field("value", v.getKey());
                        builder.field("boost", v.getValue());
                        builder.endObject();
                        // also add into fulltext field
                        addFulltext(v.getKey());
                    }
                    builder.endArray();
                }
                if (fulltext.size() > 0) {
                    builder.field(FieldNames.FULLTEXT, fulltext);
                }
                if (suggest.size() > 0) {
                    builder.startArray(FieldNames.SUGGEST);
                    for (String val : suggest) {
                        builder.startObject().field("value", val).endObject();
                    }
                    builder.endArray();
                }
                if (spellcheck.size() > 0) {
                    builder.field(FieldNames.SPELLCHECK, spellcheck);
                }
                for (Map.Entry<String, Object> simProp: similarityFields.entrySet()) {
                    builder.field(simProp.getKey(), simProp.getValue());
                }
                for (Map.Entry<String, List<Object>> prop : properties.entrySet()) {
                    builder.field(prop.getKey(), prop.getValue().size() == 1 ? prop.getValue().get(0) : prop.getValue());
                }
                if (!similarityTags.isEmpty()) {
                    builder.field(ElasticIndexDefinition.SIMILARITY_TAGS, similarityTags);
                }
            }
            builder.endObject();

            ret = Strings.toString(builder);
        } catch (IOException e) {
            LOG.error("Error serializing document - path: {}, properties: {}, fulltext: {}, suggest: {}",
                    path, properties, fulltext, suggest, e);
            ret = null;
        }

        return ret;
    }

    @Override
    public String toString() {
        return build();
    }

}
