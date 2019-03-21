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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.io.IOException;
import java.util.List;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.FacetsConfigProvider;
import org.apache.jackrabbit.oak.plugins.index.search.Aggregate;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.IndexFormatVersion;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.binary.FulltextBinaryTextExtractor;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextDocumentMaker;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.document.*;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newAncestorsField;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newDepthField;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newFulltextField;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newPathField;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newPropertyField;

public class LuceneDocumentMaker extends FulltextDocumentMaker<Document> {
    private static final Logger log = LoggerFactory.getLogger(LuceneDocumentMaker.class);

    private final FacetsConfigProvider facetsConfigProvider;
    private final IndexAugmentorFactory augmentorFactory;

    public LuceneDocumentMaker(IndexDefinition definition,
                               IndexDefinition.IndexingRule indexingRule,
                               String path) {
        this(null, null, null, definition, indexingRule, path);
    }

    public LuceneDocumentMaker(@Nullable FulltextBinaryTextExtractor textExtractor,
                               @Nullable FacetsConfigProvider facetsConfigProvider,
                               @Nullable IndexAugmentorFactory augmentorFactory,
                               IndexDefinition definition,
                               IndexDefinition.IndexingRule indexingRule,
                               String path) {
        super(textExtractor, definition, indexingRule, path);
        this.facetsConfigProvider = facetsConfigProvider;
        this.augmentorFactory = augmentorFactory;
    }

    @Override
    protected void indexAnalyzedProperty(Document doc, String pname, String value, PropertyDefinition pd) {
        String analyzedPropName = constructAnalyzedPropertyName(pname);
        doc.add(newPropertyField(analyzedPropName, value, !pd.skipTokenization(pname), pd.stored));
    }

    @Override
    protected void indexSuggestValue(Document doc, String value) {
        doc.add(FieldFactory.newSuggestField(value));
    }

    @Override
    protected void indexSpellcheckValue(Document doc, String value) {
        doc.add(newPropertyField(FieldNames.SPELLCHECK, value, true, false));
    }

    @Override
    protected void indexFulltextValue(Document doc, String value) {
        doc.add(newFulltextField(value));
    }

    @Override
    protected void indexAncestors(Document doc, String path) {
        doc.add(newAncestorsField(PathUtils.getParentPath(path)));
        doc.add(newDepthField(path));
    }

    @Override
    protected boolean indexTypedProperty(Document doc, PropertyState property, String pname, PropertyDefinition pd) {
        int tag = property.getType().tag();
        boolean fieldAdded = false;
        for (int i = 0; i < property.count(); i++) {
            Field f;
            if (tag == Type.LONG.tag()) {
                f = new LongField(pname, property.getValue(Type.LONG, i), Field.Store.NO);
            } else if (tag == Type.DATE.tag()) {
                String date = property.getValue(Type.DATE, i);
                f = new LongField(pname, FieldFactory.dateToLong(date), Field.Store.NO);
            } else if (tag == Type.DOUBLE.tag()) {
                f = new DoubleField(pname, property.getValue(Type.DOUBLE, i), Field.Store.NO);
            } else if (tag == Type.BOOLEAN.tag()) {
                f = new StringField(pname, property.getValue(Type.BOOLEAN, i).toString(), Field.Store.NO);
            } else {
                f = new StringField(pname, property.getValue(Type.STRING, i), Field.Store.NO);
            }

            if (includePropertyValue(property, i, pd)){
                doc.add(f);
                fieldAdded = true;
            }
        }
        return fieldAdded;
    }

    @Override
    protected void indexNotNullProperty(Document doc, PropertyDefinition pd) {
        doc.add(new StringField(FieldNames.NOT_NULL_PROPS, pd.name, Field.Store.NO));
    }

    @Override
    protected void indexNullProperty(Document doc, PropertyDefinition pd) {
        doc.add(new StringField(FieldNames.NULL_PROPS, pd.name, Field.Store.NO));
    }

    private String constructAnalyzedPropertyName(String pname) {
        if (definition.getVersion().isAtLeast(IndexFormatVersion.V2)){
            return FieldNames.createAnalyzedFieldName(pname);
        }
        return pname;
    }

    @Override
    protected boolean addBinary(Document doc, String path, List<String> binaryValues) {
        boolean added = false;
        for (String binaryValue : binaryValues) {
            if (path != null) {
                doc.add(newFulltextField(path, binaryValue, true));
            } else {
                doc.add(newFulltextField(binaryValue, true));
            }

            added = true;
        }

        return added;
    }

    @Override
    protected boolean indexFacetProperty(Document doc, int tag, PropertyState property, String pname) {
        String facetFieldName = FieldNames.createFacetFieldName(pname);
        getFacetsConfig().setIndexFieldName(pname, facetFieldName);

        boolean fieldAdded = false;
        try {
            if (tag == Type.STRINGS.tag() && property.isArray()) {
                getFacetsConfig().setMultiValued(pname, true);
                Iterable<String> values = property.getValue(Type.STRINGS);
                for (String value : values) {
                    if (value != null && value.length() > 0) {
                        doc.add(new SortedSetDocValuesFacetField(pname, value));
                    }
                }
                fieldAdded = true;
            } else if (tag == Type.STRING.tag()) {
                String value = property.getValue(Type.STRING);
                if (value.length() > 0) {
                    doc.add(new SortedSetDocValuesFacetField(pname, value));
                    fieldAdded = true;
                }
            }

        } catch (Throwable e) {
            log.warn("[{}] Ignoring facet property. Could not convert property {} of type {} to type {} for path {}",
                    getIndexName(), pname,
                    Type.fromTag(property.getType().tag(), false),
                    Type.fromTag(tag, false), path, e);
        }
        return fieldAdded;
    }

    @Override
    protected void indexAggregateValue(Document doc, Aggregate.NodeIncludeResult result, String value, PropertyDefinition pd) {
        Field field = result.isRelativeNode() ?
                newFulltextField(result.rootIncludePath, value) : newFulltextField(value) ;
        if (pd != null) {
            field.setBoost(pd.boost);
        }
        doc.add(field);
    }

    @Override
    protected Document initDoc() {
        Document doc = new Document();
        doc.add(newPathField(path));
        return doc;
    }

    @Override
    protected  boolean augmentCustomFields(final String path, final Document doc, final NodeState document) {
        boolean dirty = false;

        if (augmentorFactory != null) {
            Iterable<Field> augmentedFields = augmentorFactory
                    .getIndexFieldProvider(indexingRule.getNodeTypeName())
                    .getAugmentedFields(path, document, definition.getDefinitionNodeState());

            for (Field field : augmentedFields) {
                doc.add(field);
                dirty = true;
            }
        }

        return dirty;
    }

    @Override
    protected Document finalizeDoc(Document doc, boolean dirty, boolean facet) throws IOException {
        if (facet && isFacetingEnabled()) {
            doc = getFacetsConfig().build(doc);
        }

        List<IndexableField> fields = doc.getFields();

        // because of LUCENE-5833 we have to merge the suggest fields into a single one
        Field suggestField = null;
        for (IndexableField f : fields) {
            if (FieldNames.SUGGEST.equals(f.name())) {
                if (suggestField == null) {
                    suggestField = FieldFactory.newSuggestField(f.stringValue());
                } else {
                    suggestField = FieldFactory.newSuggestField(suggestField.stringValue(), f.stringValue());
                }
            }
        }

        doc.removeFields(FieldNames.SUGGEST);
        if (suggestField != null) {
            doc.add(suggestField);
        }

        return doc;
    }

    @Override
    protected boolean isFacetingEnabled(){
        return facetsConfigProvider != null;
    }

    @Override
    protected boolean indexTypeOrderedFields(Document doc, String pname, int tag, PropertyState property, PropertyDefinition pd) {
        String name = FieldNames.createDocValFieldName(pname);
        boolean fieldAdded = false;
        Field f = null;
        try {
            if (tag == Type.LONG.tag()) {
                //TODO Distinguish fields which need to be used for search and for sort
                //If a field is only used for Sort then it can be stored with less precision
                f = new NumericDocValuesField(name, property.getValue(Type.LONG));
            } else if (tag == Type.DATE.tag()) {
                String date = property.getValue(Type.DATE);
                f = new NumericDocValuesField(name, FieldFactory.dateToLong(date));
            } else if (tag == Type.DOUBLE.tag()) {
                f = new DoubleDocValuesField(name, property.getValue(Type.DOUBLE));
            } else if (tag == Type.BOOLEAN.tag()) {
                f = new SortedDocValuesField(name,
                        new BytesRef(property.getValue(Type.BOOLEAN).toString()));
            } else if (tag == Type.STRING.tag()) {
                f = new SortedDocValuesField(name,
                        new BytesRef(property.getValue(Type.STRING)));
            }

            if (f != null && includePropertyValue(property, 0, pd)) {
                doc.add(f);
                fieldAdded = true;
            }
        } catch (Exception e) {
            log.warn(
                    "[{}] Ignoring ordered property. Could not convert property {} of type {} to type {} for path {}",
                    getIndexName(), pname,
                    Type.fromTag(property.getType().tag(), false),
                    Type.fromTag(tag, false), path, e);
        }
        return fieldAdded;
    }

    private FacetsConfig getFacetsConfig(){
        return facetsConfigProvider.getFacetsConfig();
    }

    @Override
    protected void indexNodeName(Document doc, String value) {
        doc.add(new StringField(FieldNames.NODE_NAME, value, Field.Store.NO));
    }

    @Override
    protected boolean indexSimilarityTag(Document doc, PropertyState property) {
        doc.add(new TextField(FieldNames.SIMILARITY_TAGS, property.getValue(Type.STRING), Field.Store.YES));
        return true;
    }

    @Override
    protected void indexSimilarityStrings(Document doc, PropertyDefinition pd, String value) throws IOException {
        for (Field f : FieldFactory.newSimilarityFields(pd.name, value)) {
            doc.add(f);
        }
        if (pd.similarityRerank) {
            for (Field f : FieldFactory.newBinSimilarityFields(pd.name, value)) {
                doc.add(f);
            }
        }
    }

    @Override
    protected void indexSimilarityBinaries(Document doc, PropertyDefinition pd, Blob blob) throws IOException {
        for (Field f : FieldFactory.newSimilarityFields(pd.name, blob)) {
            doc.add(f);
        }
        if (pd.similarityRerank) {
            for (Field f : FieldFactory.newBinSimilarityFields(pd.name, blob)) {
                doc.add(f);
            }
        }
    }
}