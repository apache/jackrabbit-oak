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
package org.apache.jackrabbit.oak.plugins.index.elastic.index;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.search.Aggregate;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.binary.FulltextBinaryTextExtractor;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextDocumentMaker;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class ElasticDocumentMaker extends FulltextDocumentMaker<ElasticDocument> {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticDocumentMaker.class);

    public ElasticDocumentMaker(@Nullable FulltextBinaryTextExtractor textExtractor,
                                @NotNull IndexDefinition definition,
                                IndexDefinition.IndexingRule indexingRule, @NotNull String path) {
        super(textExtractor, definition, indexingRule, path);
    }

    @Override
    protected ElasticDocument initDoc() {
        return new ElasticDocument(path);
    }

    @Override
    protected ElasticDocument finalizeDoc(ElasticDocument doc, boolean dirty, boolean facet) {
        // Evaluate path restrictions is enabled by default in elastic. Always index ancestors.
        // When specifically disabled, we will keep indexing it, but the field won't be used at query time
        doc.indexAncestors(path);
        return doc;
    }

    // TODO: needed only for oak-lucene. Should be removed from oak-search
    @Override
    protected boolean isFacetingEnabled() {
        return false;
    }

    @Override
    protected boolean indexTypeOrderedFields(ElasticDocument doc, String pname, int tag, PropertyState property, PropertyDefinition pd) {
        // TODO: check the conjecture below
        // ES doesn't seem to require special mapping to sort so we don't need to add anything
        return false;
    }

    @Override
    protected boolean addBinary(ElasticDocument doc, String path, List<String> binaryValues) {
        boolean added = false;
        for (String binaryValue : binaryValues) {
            if (path != null) {
                doc.addFulltextRelative(path, binaryValue);
            } else {
                doc.addFulltext(binaryValue);
            }

            added = true;
        }

        return added;
    }

    @Override
    protected boolean indexFacetProperty(ElasticDocument doc, int tag, PropertyState property, String pname) {
        // faceting on ES works automatically for keyword (propertyIndex) and subsequently query params.
        // We don't need to add anything.
        // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-terms-aggregation.html
        return false;
    }

    @Override
    protected void indexAnalyzedProperty(ElasticDocument doc, String pname, String value, PropertyDefinition pd) {
        // Analyzed properties are persisted in Elastic using multi fields, so it is enough to map the top level property
        // and ES will create the indexes for all the nested fields. If this property was already indexed by a call to
        // #indexTypedProperty, then we have nothing more to do here. But if the property is not marked as a propertyIndex,
        // then that method was not called and we must add the top level mapping here so that the full-text nested field
        // is populated.
        if (!pd.propertyIndex) {
            doc.addProperty(pname, value);
        }
    }

    @Override
    protected void indexSuggestValue(ElasticDocument doc, String value) {
        if (value != null) {
            String v = value.trim();
            if (v.length() > 0) {
                doc.addSuggest(v);
            }
        }
    }

    @Override
    protected void indexSpellcheckValue(ElasticDocument doc, String value) {
        if (value != null) {
            String v = value.trim();
            if (v.length() > 0) {
                doc.addSpellcheck(v);
            }
        }
    }

    @Override
    protected void indexFulltextValue(ElasticDocument doc, String value) {
        if (value != null) {
            String v = value.trim();
            if (v.length() > 0) {
                doc.addFulltext(v);
            }
        }
    }

    /**
     * We store the value in :fulltext only when the {@link PropertyDefinition} has a regular expression (that means we
     * were not able to create a ft property at mapping time) or the property is not analyzed.
     */
    @Override
    protected boolean isFulltextValuePersistedAtNode(PropertyDefinition pd) {
        return pd.isRegexp || !pd.analyzed;
    }

    @Override
    protected void indexTypedProperty(ElasticDocument doc, PropertyState property, String pname, PropertyDefinition pd, int i) {
        // Try to index the value as we receive it from the user. Elastic will try to coerce the value to the type defined
        // in the index. If this fails, the ES index is configured to ignore the malformed fields (see ElasticIndexHelper)
        // and continue indexing the document. The only exception are fields of type boolean, because ES does not support
        // ignoring malformed values for boolean types.

        // This is the type in the index definition. It may not be the same as the type of value of the property being
        // indexed.
        int indexType = pd.getType();
        try {
            Object f;
            if (indexType == Type.BOOLEAN.tag()) {
                // Try to convert the value to the index definition type, which is Boolean. This is a special case,
                // because ES does not support ignore_malformed in boolean fields so we cannot rely on ES for conversion
                f = property.getValue(Type.BOOLEAN, i);
            } else {
                // Send to ES the value as we got it from the user and let ES convert the property. If ES cannot convert
                // the value, then it ignores it without raising an error (ignore_malformed=true)
                // Get the type of the property, as was received from the client.
                int propertyType = property.getType().tag();
                // The code below does not perform any type conversion, it simply gets the value as the type of the property
                if (propertyType == Type.LONG.tag()) {
                    f = property.getValue(Type.LONG, i);
                } else if (propertyType == Type.DATE.tag()) {
                    f = property.getValue(Type.DATE, i);
                } else if (propertyType == Type.DOUBLE.tag()) {
                    f = property.getValue(Type.DOUBLE, i);
                } else if (propertyType == Type.BOOLEAN.tag()) {
                    f = property.getValue(Type.BOOLEAN, i);
                } else {
                    f = property.getValue(Type.STRING, i);
                }
            }
            doc.addProperty(pname, f);
        } catch (Exception e) {
            LOG.warn(
                    "[{}] Ignoring property. Could not convert property {} of type {} to type {} for path {}",
                    getIndexName(), pname,
                    Type.fromTag(property.getType().tag(), false),
                    Type.fromTag(indexType, false), path, e);
        }
    }

    /**
     * Empty method implementation. Ancestors are always indexed
     *
     * @see ElasticDocumentMaker#finalizeDoc
     */
    @Override
    protected void indexAncestors(ElasticDocument doc, String path) { /* empty */ }

    @Override
    protected void indexNotNullProperty(ElasticDocument doc, PropertyDefinition pd) {
        // Elastic support exist queries for specific fields
    }

    @Override
    protected void indexNullProperty(ElasticDocument doc, PropertyDefinition pd) {
        // Elastic support not exist queries for specific fields
    }

    @Override
    protected void indexAggregateValue(ElasticDocument doc, Aggregate.NodeIncludeResult result, String value, PropertyDefinition pd) {
        if (result.isRelativeNode()) {
            doc.addFulltextRelative(result.rootIncludePath, value);
        } else {
            doc.addFulltext(value);
        }
    }

    @Override
    protected void indexNodeName(ElasticDocument doc, String value) {
        doc.addProperty(FieldNames.NODE_NAME, value);
    }

    @Override
    protected boolean indexSimilarityTag(ElasticDocument doc, PropertyState property) {
        String val = property.getValue(Type.STRING);
        if (val.length() > 0) {
            doc.addSimilarityTag(val);
            return true;
        }
        return false;
    }

    @Override
    protected void indexSimilarityBinaries(ElasticDocument doc, PropertyDefinition pd, Blob blob) throws IOException {
        // without this check, if the vector size is not correct, the entire document will be skipped
        if (pd.getSimilaritySearchDenseVectorSize() == blob.length() / 8) {
            // see https://www.elastic.co/blog/text-similarity-search-with-vectors-in-elasticsearch
            // see https://www.elastic.co/guide/en/elasticsearch/reference/current/dense-vector.html
            doc.addSimilarityField(pd.name, blob);
        } else {
            LOG.warn("[{}] Ignoring binary property {} for path {}. Expected dimension is {} but got {}",
                    getIndexName(), pd.name, this.path, pd.getSimilaritySearchDenseVectorSize(), blob.length() / 8);
        }
    }

    @Override
    protected void indexSimilarityStrings(ElasticDocument doc, PropertyDefinition pd, String value) {
        // TODO : not implemented
    }

    @Override
    protected boolean augmentCustomFields(String path, ElasticDocument doc, NodeState document) {
        // TODO : not implemented
        return false;
    }

    @Override
    protected boolean indexDynamicBoost(ElasticDocument doc, String parent, String nodeName, String token, double boost) {
        if (token.length() > 0) {
            doc.addDynamicBoostField(nodeName, token, boost);
            return true;
        }
        return false;
    }
}