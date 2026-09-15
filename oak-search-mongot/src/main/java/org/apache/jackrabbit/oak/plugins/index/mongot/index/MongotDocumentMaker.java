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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.Aggregate;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.binary.FulltextBinaryTextExtractor;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextDocumentMaker;
import org.apache.jackrabbit.oak.plugins.index.search.util.DataConversionUtil;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class MongotDocumentMaker extends FulltextDocumentMaker<MongoDocument> {

    public MongotDocumentMaker(@Nullable FulltextBinaryTextExtractor textExtractor,
                              @NotNull IndexDefinition definition,
                              IndexDefinition.IndexingRule indexingRule,
                              @NotNull String path) {
        super(textExtractor, definition, indexingRule, path);
    }

    @Override
    protected MongoDocument initDoc() {
        return new MongoDocument(path);
    }

    @Override
    protected MongoDocument finalizeDoc(MongoDocument doc, boolean dirty, boolean facet) {
        for (String element : PathUtils.elements(path)) {
            if (IndexConstants.INDEX_DEFINITIONS_NAME.equals(element)) {
                return null;
            }
        }
        return doc;
    }

    @Override
    protected boolean isFacetingEnabled() {
        return true;
    }

    @Override
    protected boolean indexTypeOrderedFields(MongoDocument doc, String propertyName, int tag,
                                             PropertyState property, PropertyDefinition definition) {
        Object value = value(property, tag, 0);
        if (value == null) {
            return false;
        }
        doc.addOrderedProperty(propertyName, value);
        return true;
    }

    @Override
    protected boolean addBinary(MongoDocument doc, String relativePath, List<String> binaryValues) {
        for (String value : binaryValues) {
            if (relativePath == null) {
                doc.addFulltext(value);
            } else {
                doc.addRelativeFulltext(relativePath, value);
            }
        }
        return !binaryValues.isEmpty();
    }

    @Override
    protected boolean indexFacetProperty(MongoDocument doc, int tag, PropertyState property, String propertyName) {
        boolean indexed = false;
        for (int i = 0; i < property.count(); i++) {
            Object value = value(property, tag, i);
            if (value != null) {
                doc.addFacetProperty(propertyName, value);
                indexed = true;
            }
        }
        return indexed;
    }

    @Override
    protected void indexAnalyzedProperty(MongoDocument doc, String propertyName, String value,
                                         PropertyDefinition definition) {
        addTrimmed(value, trimmed -> doc.addAnalyzedProperty(propertyName, trimmed));
    }

    @Override
    protected void indexSuggestValue(MongoDocument doc, String value) {
        addTrimmed(value, doc::addSuggest);
    }

    @Override
    protected void indexSpellcheckValue(MongoDocument doc, String value) {
        addTrimmed(value, doc::addSpellcheck);
    }

    @Override
    protected void indexFulltextValue(MongoDocument doc, String value) {
        addTrimmed(value, doc::addFulltext);
    }

    @Override
    protected void indexTypedProperty(MongoDocument doc, PropertyState property, String propertyName,
                                      PropertyDefinition definition, int index) {
        int tag = definition.isTypeDefined() ? definition.getType() : property.getType().tag();
        Object value = value(property, tag, index);
        if (value != null) {
            doc.addTypedProperty(propertyName, value);
        }
    }

    @Override
    protected boolean indexDynamicBoost(MongoDocument doc, String parent, String nodeName,
                                        String value, double confidence) {
        if (value.isEmpty()) {
            return false;
        }
        doc.addDynamicBoost(nodeName, value, confidence);
        return true;
    }

    @Override
    protected void indexAncestors(MongoDocument doc, String path) {
        // Structural metadata is always added by MongoDocument.
    }

    @Override
    protected void indexNotNullProperty(MongoDocument doc, PropertyDefinition definition) {
        doc.addNotNullProperty(definition.name);
    }

    @Override
    protected void indexNullProperty(MongoDocument doc, PropertyDefinition definition) {
        doc.addNullProperty(definition.name);
    }

    @Override
    protected void indexAggregateValue(MongoDocument doc, Aggregate.NodeIncludeResult result,
                                       String value, PropertyDefinition definition) {
        if (result.isRelativeNode()) {
            doc.addRelativeFulltext(result.rootIncludePath, value);
        } else {
            doc.addFulltext(value);
        }
    }

    @Override
    protected void indexNodeName(MongoDocument doc, String value) {
        doc.addTypedProperty(FieldNames.NODE_NAME, value);
        int namespace = value.indexOf(':');
        if (namespace >= 0) {
            doc.addTypedProperty(FieldNames.NODE_NAME, value.substring(namespace + 1));
        }
    }

    @Override
    protected boolean indexSimilarityTag(MongoDocument doc, String value) {
        if (value.isEmpty()) {
            return false;
        }
        doc.addSimilarityTag(value);
        return true;
    }

    @Override
    protected void indexSimilarityBinaries(MongoDocument doc, PropertyDefinition definition, Blob blob)
            throws IOException {
        int dimensions = definition.getSimilaritySearchDenseVectorSize();
        if (blob.length() != (long) dimensions * Float.BYTES) {
            return;
        }
        byte[] bytes;
        try (java.io.InputStream stream = blob.getNewStream()) {
            bytes = stream.readAllBytes();
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        List<Float> vector = new ArrayList<>(dimensions);
        while (buffer.hasRemaining()) {
            vector.add(buffer.getFloat());
        }
        doc.addSimilarityVector(definition.name, vector);
    }

    @Override
    protected void indexSimilarityStrings(MongoDocument doc, PropertyDefinition definition, String value)
            throws IOException {
        // Vector search is outside this POC task.
    }

    @Override
    protected boolean augmentCustomFields(String path, MongoDocument doc, NodeState document) {
        PropertyState primaryType = document.getProperty("jcr:primaryType");
        if (primaryType != null) {
            doc.setPrimaryType(primaryType.getValue(Type.NAME));
        }
        PropertyState mixinTypes = document.getProperty("jcr:mixinTypes");
        if (mixinTypes != null) {
            doc.setMixinTypes(mixinTypes.getValue(Type.NAMES));
        }
        // Node-type metadata must not make an otherwise unrelated property/function
        // node eligible. Aggregate roots are different: replacing their document even
        // after the final included child disappears removes stale aggregate text.
        return indexingRule.getAggregate() != null;
    }

    private static Object value(PropertyState property, int tag, int index) {
        try {
            if (tag == Type.LONG.tag()) {
                return property.getValue(Type.LONG, index);
            }
            if (tag == Type.DOUBLE.tag()) {
                return property.getValue(Type.DOUBLE, index);
            }
            if (tag == Type.BOOLEAN.tag()) {
                return property.getValue(Type.BOOLEAN, index);
            }
            if (tag == Type.DATE.tag()) {
                return new Date(DataConversionUtil.dateToLong(property.getValue(Type.DATE, index)));
            }
            if (tag == Type.BINARY.tag()) {
                return null;
            }
            return property.getValue(Type.STRING, index);
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static void addTrimmed(String value, java.util.function.Consumer<String> consumer) {
        if (value != null) {
            String trimmed = value.trim();
            if (!trimmed.isEmpty()) {
                consumer.accept(trimmed);
            }
        }
    }
}
