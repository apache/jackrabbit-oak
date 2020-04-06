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
package org.apache.jackrabbit.oak.plugins.index.elasticsearch.index;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class ElasticsearchDocument {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchDocument.class);

    // id should only be useful for logging (at least as of now)
    private final String path;

    private final String id;
    private final List<String> fulltext;
    private final List<String> suggest;
    private final List<String> notNullProps;
    private final List<String> nullProps;
    private final Map<String, Object> properties;

    ElasticsearchDocument(String path) {
        this.path = path;
        String id = null;
        try {
            id = pathToId(path);
        } catch (UnsupportedEncodingException e) {
            LOG.warn("Couldn't encode {} as ES id", path);
        }
        this.id = id;
        this.fulltext = new ArrayList<>();
        this.suggest = new ArrayList<>();
        this.notNullProps = new ArrayList<>();
        this.nullProps = new ArrayList<>();
        this.properties = new HashMap<>();
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

    void notNullProp(String propName) {
        notNullProps.add(propName);
    }

    void nullProp(String propName) {
        nullProps.add(propName);
    }

    // ES for String values (that are not interpreted as date or numbers etc) would analyze in the same
    // field and would index a sub-field "keyword" for non-analyzed value.
    // ref: https://www.elastic.co/blog/strings-are-dead-long-live-strings
    // (interpretation of date etc: https://www.elastic.co/guide/en/elasticsearch/reference/current/dynamic-field-mapping.html)
    void addProperty(String fieldName, Object value) {
        properties.put(fieldName, value);
    }

    void indexAncestors(String path) {
        String parPath = PathUtils.getParentPath(path);
        int depth = PathUtils.getDepth(path);

        // TODO: remember that mapping must be configured with
        // https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-pathhierarchy-tokenizer.html
        addProperty(FieldNames.ANCESTORS, parPath);
        addProperty(FieldNames.PATH_DEPTH, depth);
    }

    String getId() {
        return id;
    }

    public String build() {
        String ret = null;
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            if (fulltext.size() > 0) {
                builder.field(FieldNames.FULLTEXT, fulltext);
            }
            if (suggest.size() > 0) {
                builder.startObject(FieldNames.SUGGEST).field("input", suggest).endObject();
            }
            if (notNullProps.size() > 0) {
                builder.field(FieldNames.NOT_NULL_PROPS, notNullProps);
            }
            if (nullProps.size() > 0) {
                builder.field(FieldNames.NULL_PROPS, nullProps);
            }
            for (Map.Entry<String, Object> prop : properties.entrySet()) {
                builder.field(prop.getKey(), prop.getValue());
            }
            builder.endObject();

            ret = Strings.toString(builder);
        } catch (IOException e) {
            LOG.error("Error serializing document - id: {}, properties: {}, fulltext: {}, suggest: {}, " +
                            "notNullProps: {}, nullProps: {}",
                    path, properties, fulltext, suggest, notNullProps, nullProps,
                    e);
        }

        return ret;
    }

    @Override
    public String toString() {
        return build();
    }

    public static String pathToId(String path) throws UnsupportedEncodingException {
        return URLEncoder.encode(path, "UTF-8");
    }
}
