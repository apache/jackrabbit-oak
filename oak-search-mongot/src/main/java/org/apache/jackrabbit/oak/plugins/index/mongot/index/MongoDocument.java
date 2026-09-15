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
package org.apache.jackrabbit.oak.plugins.index.mongot.index;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.bson.Document;

public final class MongoDocument {

    private final Document document;

    public MongoDocument(String path) {
        this.document = new Document(MongoFieldNames.ID, MongoDocumentId.fromPath(path))
                .append(MongoFieldNames.PATH, path)
                .append(MongoFieldNames.PARENT, PathUtils.getParentPath(path))
                .append(MongoFieldNames.DEPTH, PathUtils.getDepth(path))
                .append(MongoFieldNames.ANCESTORS, ancestors(path));
    }

    public void addTypedProperty(String propertyName, Object value) {
        addPropertyValue(MongoFieldNames.TYPED, propertyName, value, false);
    }

    public void addAnalyzedProperty(String propertyName, String value) {
        addPropertyValue(MongoFieldNames.ANALYZED, propertyName, value, true);
    }

    public void addOrderedProperty(String propertyName, Object value) {
        addPropertyValue(MongoFieldNames.ORDERED, propertyName, value, false);
    }

    public void addFacetProperty(String propertyName, Object value) {
        addPropertyValue(MongoFieldNames.FACET, propertyName, value, true);
    }

    public void addFulltext(String value) {
        addUniqueValue(MongoFieldNames.FULLTEXT, value);
    }

    public void addRelativeFulltext(String relativePath, String value) {
        addPropertyValue(MongoFieldNames.RELATIVE_FULLTEXT, relativePath, value, true);
    }

    public void addSuggest(String value) {
        addUniqueValue(MongoFieldNames.SUGGEST, value);
    }

    public void addSpellcheck(String value) {
        addUniqueValue(MongoFieldNames.SPELLCHECK, value);
    }

    public void addSimilarityVector(String propertyName, List<Float> vector) {
        document.put(FieldNames.createSimilarityFieldName(
                MongoFieldNames.encodeProperty(propertyName)), vector);
    }

    public void addSimilarityTag(String value) {
        addUniqueValue(FieldNames.SIMILARITY_TAGS, value);
    }

    public void addNullProperty(String propertyName) {
        addUniqueValue(MongoFieldNames.NULL_PROPERTIES, MongoFieldNames.encodeProperty(propertyName));
    }

    public void addNotNullProperty(String propertyName) {
        addUniqueValue(MongoFieldNames.NOT_NULL_PROPERTIES, MongoFieldNames.encodeProperty(propertyName));
    }

    public void setPrimaryType(String primaryType) {
        document.put(MongoFieldNames.PRIMARY_TYPE, primaryType);
    }

    public void setMixinTypes(Iterable<String> mixinTypes) {
        List<String> values = new ArrayList<>();
        mixinTypes.forEach(values::add);
        document.put(MongoFieldNames.MIXIN_TYPES, values);
    }

    public void addDynamicBoost(String propertyName, String token, double confidence) {
        Document section = document.get(MongoFieldNames.DYNAMIC_BOOST, Document.class);
        if (section == null) {
            section = new Document();
            document.append(MongoFieldNames.DYNAMIC_BOOST, section);
        }
        String fieldName = MongoFieldNames.encodeProperty(propertyName);
        List<Document> entries = section.getList(fieldName, Document.class);
        if (entries == null) {
            entries = new ArrayList<>();
            section.append(fieldName, entries);
        }
        entries.add(new Document("token", token).append("confidence", confidence));
        addUniqueValue(MongoFieldNames.DYNAMIC_BOOST_TOKENS, token);
        Document scores = document.get(MongoFieldNames.DYNAMIC_BOOST_SCORES, Document.class);
        if (scores == null) {
            scores = new Document();
            document.append(MongoFieldNames.DYNAMIC_BOOST_SCORES, scores);
        }
        String scoreField = MongoFieldNames.encodeProperty(token.toLowerCase(java.util.Locale.ROOT));
        Number current = scores.get(scoreField, Number.class);
        if (current == null || current.doubleValue() < confidence) {
            scores.put(scoreField, confidence);
        }
    }

    public Document toBson() {
        return document;
    }

    private void addPropertyValue(String sectionName, String propertyName, Object value, boolean alwaysArray) {
        Document section = document.get(sectionName, Document.class);
        if (section == null) {
            section = new Document();
            document.append(sectionName, section);
        }

        String fieldName = MongoFieldNames.encodeProperty(propertyName);
        Object current = section.get(fieldName);
        if (current == null) {
            section.append(fieldName, alwaysArray ? new ArrayList<>(List.of(value)) : value);
        } else if (current instanceof List<?>) {
            @SuppressWarnings("unchecked")
            List<Object> values = (List<Object>) current;
            if (!values.contains(value)) {
                values.add(value);
            }
        } else if (!current.equals(value)) {
            section.put(fieldName, new ArrayList<>(List.of(current, value)));
        }
    }

    private void addUniqueValue(String fieldName, Object value) {
        List<Object> values = document.getList(fieldName, Object.class);
        if (values == null) {
            values = new ArrayList<>();
            document.append(fieldName, values);
        }
        if (!values.contains(value)) {
            values.add(value);
        }
    }

    private static List<String> ancestors(String path) {
        LinkedHashSet<String> ancestors = new LinkedHashSet<>();
        String current = PathUtils.getParentPath(path);
        while (!current.isEmpty()) {
            ancestors.add(current);
            if (PathUtils.denotesRoot(current)) {
                break;
            }
            current = PathUtils.getParentPath(current);
        }
        List<String> result = new ArrayList<>(ancestors);
        java.util.Collections.reverse(result);
        return result;
    }
}
