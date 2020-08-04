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

import java.io.IOException;
import java.util.List;

class ElasticDocumentMaker extends FulltextDocumentMaker<ElasticDocument> {

    ElasticDocumentMaker(@Nullable FulltextBinaryTextExtractor textExtractor,
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
        // evaluate path restrictions is enabled by default in elastic. Always index ancestors
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
        // We we don't need to add anything.
        // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-terms-aggregation.html
        return false;
    }

    @Override
    protected void indexAnalyzedProperty(ElasticDocument doc, String pname, String value, PropertyDefinition pd) {
        // no need to do anything here. Analyzed properties are persisted in Elastic
        // using multi fields. The analyzed properties are set calling #indexTypedProperty
    }

    @Override
    protected void indexSuggestValue(ElasticDocument doc, String value) {
        doc.addSuggest(value);
    }

    @Override
    protected void indexSpellcheckValue(ElasticDocument doc, String value) {
        // TODO: Figure out how to do spellcheck with ES (interwebs seems to say that it should be simple
        // and don't need anything extra in indexed document
    }

    @Override
    protected void indexFulltextValue(ElasticDocument doc, String value) {
        doc.addFulltext(value);
    }

    @Override
    protected boolean isFulltextValuePersisted() {
        return false;
    }

    @Override
    protected void indexTypedProperty(ElasticDocument doc, PropertyState property, String pname, PropertyDefinition pd, int i) {
        int tag = property.getType().tag();

        Object f;
        if (tag == Type.LONG.tag()) {
            f = property.getValue(Type.LONG, i);
        } else if (tag == Type.DATE.tag()) {
            f = property.getValue(Type.DATE, i);
        } else if (tag == Type.DOUBLE.tag()) {
            f = property.getValue(Type.DOUBLE, i);
        } else if (tag == Type.BOOLEAN.tag()) {
            f = property.getValue(Type.BOOLEAN, i).toString();
        } else {
            f = property.getValue(Type.STRING, i);
        }

        doc.addProperty(pname, f);
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
        doc.notNullProp(pd.name);
    }

    @Override
    protected void indexNullProperty(ElasticDocument doc, PropertyDefinition pd) {
        doc.nullProp(pd.name);
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
        // TODO : not implemented
        return false;
    }

    @Override
    protected void indexSimilarityBinaries(ElasticDocument doc, PropertyDefinition pd, Blob blob) throws IOException {
        // TODO : not implemented
        // see https://www.elastic.co/blog/text-similarity-search-with-vectors-in-elasticsearch
        // see https://www.elastic.co/guide/en/elasticsearch/reference/current/dense-vector.html
    }

    @Override
    protected void indexSimilarityStrings(ElasticDocument doc, PropertyDefinition pd, String value) throws IOException {
        // TODO : not implemented
    }

    @Override
    protected boolean augmentCustomFields(String path, ElasticDocument doc, NodeState document) {
        // TODO : not implemented
        return false;
    }

    @Override
    protected boolean indexDynamicBoost(ElasticDocument doc, PropertyDefinition pd, NodeState nodeState,
                                        String propertyName) {
        // TODO : not implemented
        return false;
    }
}
