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
package org.apache.jackrabbit.oak.plugins.index.elasticsearch.index;

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

public class ElasticsearchDocumentMaker extends FulltextDocumentMaker<ElasticsearchDocument> {

    ElasticsearchDocumentMaker(@Nullable FulltextBinaryTextExtractor textExtractor,
                               @NotNull IndexDefinition definition,
                               IndexDefinition.IndexingRule indexingRule, @NotNull String path) {
        super(textExtractor, definition, indexingRule, path);
    }

    @Override
    protected ElasticsearchDocument initDoc() {
        return new ElasticsearchDocument(path);
    }

    @Override
    protected ElasticsearchDocument finalizeDoc(ElasticsearchDocument doc, boolean dirty, boolean facet) throws IOException {
        if (doc.getId() == null) {
            throw new IOException("Couldn't generate id for doc - (More details during initDoc)" + doc);
        }
        return doc;
    }

    // TODO: needed only for oak-lucene. Should be removed from oak-search
    @Override
    protected boolean isFacetingEnabled() {
        return false;
    }

    @Override
    protected boolean indexTypeOrderedFields(ElasticsearchDocument doc, String pname, int tag, PropertyState property, PropertyDefinition pd) {
        // TODO: check the conjecture below
        // ES doesn't seem to require special mapping to sort so we don't need to add anything
        return false;
    }

    @Override
    protected boolean addBinary(ElasticsearchDocument doc, String path, List<String> binaryValues) {
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
    protected boolean indexFacetProperty(ElasticsearchDocument doc, int tag, PropertyState property, String pname) {
        // faceting on ES works automatically for keyword (propertyIndex) and subsequently query params.
        // We we don't need to add anything.
        // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-terms-aggregation.html
        return false;
    }

    @Override
    protected void indexAnalyzedProperty(ElasticsearchDocument doc, String pname, String value, PropertyDefinition pd) {
        doc.addProperty(FieldNames.createAnalyzedFieldName(pname), value);
    }

    @Override
    protected void indexSuggestValue(ElasticsearchDocument doc, String value) {
        doc.addSuggest(value);
    }

    @Override
    protected void indexSpellcheckValue(ElasticsearchDocument doc, String value) {
        // TODO: Figure out how to do spellcheck with ES (interwebs seems to say that it should be simple
        // and don't need anything extra in indexed document
    }

    @Override
    protected void indexFulltextValue(ElasticsearchDocument doc, String value) {
        // Note: diversion from lucene impl - here we are storing even these cases and not just binary
        doc.addFulltext(value);
    }

    @Override
    protected boolean indexTypedProperty(ElasticsearchDocument doc, PropertyState property, String pname, PropertyDefinition pd) {
        int tag = property.getType().tag();
        boolean fieldAdded = false;
        for (int i = 0; i < property.count(); i++) {
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

            if (includePropertyValue(property, i, pd)){
                doc.addProperty(pname, f);
                fieldAdded = true;
            }
        }
        return fieldAdded;
    }

    @Override
    protected void indexAncestors(ElasticsearchDocument doc, String path) {
        doc.indexAncestors(path);
    }

    @Override
    protected void indexNotNullProperty(ElasticsearchDocument doc, PropertyDefinition pd) {
        doc.notNullProp(pd.name);
    }

    @Override
    protected void indexNullProperty(ElasticsearchDocument doc, PropertyDefinition pd) {
        doc.nullProp(pd.name);
    }

    @Override
    protected void indexAggregateValue(ElasticsearchDocument doc, Aggregate.NodeIncludeResult result, String value, PropertyDefinition pd) {
        if (result.isRelativeNode()) {
            doc.addFulltextRelative(result.rootIncludePath, value);
        } else {
            doc.addFulltext(value);
        }
    }

    @Override
    protected void indexNodeName(ElasticsearchDocument doc, String value) {
        doc.addProperty(FieldNames.NODE_NAME, value);
    }

    @Override
    protected boolean indexSimilarityTag(ElasticsearchDocument doc, PropertyState property) {
        // TODO : not implemented
        return false;
    }

    @Override
    protected void indexSimilarityBinaries(ElasticsearchDocument doc, PropertyDefinition pd, Blob blob) throws IOException {
        // TODO : not implemented
    }

    @Override
    protected void indexSimilarityStrings(ElasticsearchDocument doc, PropertyDefinition pd, String value) throws IOException {
        // TODO : not implemented
    }

    @Override
    protected boolean augmentCustomFields(String path, ElasticsearchDocument doc, NodeState document) {
        // TODO : not implemented
        return false;
    }
}
