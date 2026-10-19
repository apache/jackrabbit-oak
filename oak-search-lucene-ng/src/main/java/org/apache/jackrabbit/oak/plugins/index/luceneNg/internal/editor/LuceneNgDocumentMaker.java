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
package org.apache.jackrabbit.oak.plugins.index.luceneNg.internal.editor;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.LuceneNgIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.Aggregate;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition.IndexingRule;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.binary.FulltextBinaryTextExtractor;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextDocumentMaker;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.util.ISO8601;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.PropertyType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Builds a Lucene 9 {@link Document} for a single node, implementing the abstract hooks
 * required by the shared {@link FulltextDocumentMaker} framework (the same framework
 * {@code oak-lucene} and {@code oak-search-elastic} use).
 *
 * <p>Field-<em>selection</em> gating (which hook fires for which {@link PropertyDefinition}
 * flag) is handled entirely by the framework's {@code makeDocument} template method; these
 * hooks only create the fields once invoked.</p>
 *
 * <p>{@link #indexAggregateValue} routes a matched child/relative node's text into the
 * parent's {@code :fulltext} field, giving this module index-time <b>aggregation</b>.</p>
 */
public class LuceneNgDocumentMaker extends FulltextDocumentMaker<Document> {

    private static final Logger LOG = LoggerFactory.getLogger(LuceneNgDocumentMaker.class);

    private static final int MAX_FIELD_LENGTH = IndexDefinition.DEFAULT_MAX_FIELD_LENGTH;

    private final FacetsConfig facetsConfig;

    /**
     * @param textExtractor optional binary text extractor; this module has no binary
     *                      extraction support (see {@link #addBinary}) so this is normally
     *                      {@code null}. Retained for parity with the framework contract.
     * @param definition    the (LuceneNg) index definition
     * @param indexingRule  the indexing rule matched for the node being indexed
     * @param path          the content path of the node being indexed
     * @param facetsConfig  the pre-built facets configuration (dimensions registered by the
     *                      editor context); used by {@link #finalizeDoc} to build facet fields
     */
    public LuceneNgDocumentMaker(@Nullable FulltextBinaryTextExtractor textExtractor,
                                 IndexDefinition definition,
                                 IndexingRule indexingRule,
                                 String path,
                                 FacetsConfig facetsConfig) {
        super(textExtractor, definition, indexingRule, path);
        this.facetsConfig = facetsConfig;
    }

    @Override
    protected Document initDoc() {
        Document doc = new Document();
        // Path fields are always added — they use the ":path" / ":parent" prefixes which
        // cannot collide with JCR property names.
        doc.add(new StringField(FieldNames.PATH, path, Field.Store.YES));
        int lastSlash = path.lastIndexOf('/');
        String parentPath = lastSlash == 0 ? "/" : path.substring(0, lastSlash);
        doc.add(new StringField(LuceneNgIndexConstants.FIELD_PARENT_PATH, parentPath, Field.Store.NO));
        return doc;
    }

    @Override
    protected Document finalizeDoc(Document doc, boolean dirty, boolean facet) throws IOException {
        return (facet && facetsConfig != null) ? facetsConfig.build(doc) : doc;
    }

    @Override
    protected boolean isFacetingEnabled() {
        return facetsConfig != null;
    }

    // -------------------------------------------------------------------------
    // Typed / property-index fields
    // -------------------------------------------------------------------------

    /**
     * Adds the property-index (exact-match / point) field for the value at position {@code i}.
     * The framework's {@code addTypedFields} iterates array values and calls this once per value,
     * so this method handles a single value only.
     *
     * <p>The declared type decides the field and which value types are accepted; other values are
     * skipped. Long/Date write long points, Double/Decimal write double points, so a field keeps
     * one Lucene field type across all documents. Undeclared and String definitions index every
     * non-binary value as a string term. Boolean, Name, Path, Reference/WeakReference and URI
     * definitions index matching values (and strings) as string terms. A declared Binary type
     * produces no field.</p>
     *
     * <p>For an ordered multi-valued property this also writes each value's sort doc-value: the
     * framework only calls {@link #indexTypeOrderedFields} for single values. Single and
     * multi-valued properties write the same doc-values type (SORTED_NUMERIC for numbers,
     * SORTED_SET for strings), because Lucene rejects a document whose field has a different
     * doc-values type than in other documents. Sorting uses each document's smallest value.</p>
     */
    @Override
    protected void indexTypedProperty(Document doc, PropertyState property, String pname,
                                      PropertyDefinition pd, int i) {
        boolean sortArrayValue = pd.ordered && property.isArray();
        switch (pd.getType()) {
            case PropertyType.LONG:
            case PropertyType.DATE: {
                Long value = longValue(pd, property, i);
                if (value == null) {
                    LOG.debug("Skipping property '{}': declared {} but value cannot be converted",
                            pname, PropertyType.nameFromValue(pd.getType()));
                    return;
                }
                doc.add(new LongPoint(pname, value));
                if (sortArrayValue) {
                    doc.add(new SortedNumericDocValuesField(pname, value));
                }
                return;
            }
            case PropertyType.DOUBLE:
            case PropertyType.DECIMAL: {
                Double value = doubleValue(pd, property, i);
                if (value == null) {
                    LOG.debug("Skipping property '{}': declared {} but value cannot be converted",
                            pname, PropertyType.nameFromValue(pd.getType()));
                    return;
                }
                doc.add(new DoublePoint(pname, value));
                if (sortArrayValue) {
                    doc.add(new SortedNumericDocValuesField(pname, NumericUtils.doubleToSortableLong(value)));
                }
                return;
            }
            default:
                if (!indexedAsString(pd.getType(), property.getType().tag())) {
                    LOG.debug("Skipping property '{}': declared {} does not accept {} values", pname,
                            PropertyType.nameFromValue(pd.getType()),
                            PropertyType.nameFromValue(property.getType().tag()));
                    return;
                }
                String value = property.getValue(Type.STRING, i);
                if (value.length() < MAX_FIELD_LENGTH) {
                    doc.add(new StringField(pname, value, Field.Store.NO));
                }
                if (sortArrayValue) {
                    doc.add(new SortedSetDocValuesField(pname, new BytesRef(truncated(value))));
                }
        }
    }

    /**
     * Adds the sort doc-value of a <em>single-valued</em> ordered property; see
     * {@link #indexTypedProperty} for multi-valued properties and the doc-values types used.
     */
    @Override
    protected boolean indexTypeOrderedFields(Document doc, String pname, int tag, PropertyState property,
                                             PropertyDefinition pd) {
        switch (pd.getType()) {
            case PropertyType.LONG:
            case PropertyType.DATE: {
                Long value = longValue(pd, property, 0);
                if (value == null) {
                    return false;
                }
                doc.add(new SortedNumericDocValuesField(pname, value));
                return true;
            }
            case PropertyType.DOUBLE:
            case PropertyType.DECIMAL: {
                Double value = doubleValue(pd, property, 0);
                if (value == null) {
                    return false;
                }
                doc.add(new SortedNumericDocValuesField(pname, NumericUtils.doubleToSortableLong(value)));
                return true;
            }
            default:
                if (!indexedAsString(pd.getType(), property.getType().tag())) {
                    return false;
                }
                doc.add(new SortedSetDocValuesField(pname, new BytesRef(truncated(property.getValue(Type.STRING)))));
                return true;
        }
    }

    /**
     * Whether a value of {@code valueType} is indexed as a string for a property declared as
     * {@code declaredType} (one of the string-backed types; undeclared reads as String).
     */
    private static boolean indexedAsString(int declaredType, int valueType) {
        switch (declaredType) {
            case PropertyType.BINARY:
                return false;
            case PropertyType.BOOLEAN:
                return oneOf(valueType, PropertyType.STRING, PropertyType.LONG, PropertyType.DOUBLE,
                        PropertyType.BOOLEAN, PropertyType.NAME);
            case PropertyType.NAME:
                return oneOf(valueType, PropertyType.STRING, PropertyType.NAME);
            case PropertyType.PATH:
                return oneOf(valueType, PropertyType.STRING, PropertyType.PATH);
            case PropertyType.REFERENCE:
            case PropertyType.WEAKREFERENCE:
                return oneOf(valueType, PropertyType.STRING, PropertyType.REFERENCE, PropertyType.WEAKREFERENCE);
            case PropertyType.URI:
                return oneOf(valueType, PropertyType.STRING, PropertyType.URI);
            default:
                return valueType != PropertyType.BINARY;
        }
    }

    private static boolean oneOf(int valueType, int... acceptedValueTypes) {
        for (int accepted : acceptedValueTypes) {
            if (valueType == accepted) {
                return true;
            }
        }
        return false;
    }

    private static String truncated(String value) {
        return value.length() <= MAX_FIELD_LENGTH ? value : value.substring(0, MAX_FIELD_LENGTH);
    }

    /** Value {@code i} as a long for a property declared Long or Date (milliseconds since epoch). */
    @Nullable
    private Long longValue(PropertyDefinition pd, PropertyState property, int i) {
        return pd.getType() == PropertyType.DATE ? readAsDateMillis(property, i) : readAsLong(property, i);
    }

    /** Value {@code i} as a double for a property declared Double or Decimal. */
    @Nullable
    private Double doubleValue(PropertyDefinition pd, PropertyState property, int i) {
        if (pd.getType() == PropertyType.DECIMAL && property.getType().tag() == PropertyType.DECIMAL) {
            return property.getValue(Type.DECIMAL, i).doubleValue();
        }
        return readAsDouble(property, i);
    }

    // -------------------------------------------------------------------------
    // Fulltext / analyzed / aggregation
    // -------------------------------------------------------------------------

    @Override
    protected void indexAnalyzedProperty(Document doc, String pname, String value, PropertyDefinition pd) {
        // Writes the per-property analyzed field consumed by property-scoped fulltext queries
        // (CONTAINS(propertyName, ...)), as resolved on the read side by
        // LuceneNgIndex#tokenToQuery via the same FieldNames.createAnalyzedFieldName(pname).
        String analyzedFieldName = FieldNames.createAnalyzedFieldName(pname);
        boolean tokenized = !pd.skipTokenization(pname);
        doc.add(tokenized
                ? new TextField(analyzedFieldName, value, pd.stored ? Field.Store.YES : Field.Store.NO)
                : new StringField(analyzedFieldName, value, pd.stored ? Field.Store.YES : Field.Store.NO));
    }

    /**
     * Whether the nodeScope fulltext property currently being indexed is {@code useInExcerpt}
     * ({@code pd.stored}). Captured in {@link #isFulltextValuePersistedAtNode(PropertyDefinition)},
     * which the framework invokes for each nodeScope value immediately before
     * {@link #indexFulltextValue(Document, String)}, so the {@code :fulltext} field is <em>stored</em>
     * exactly for properties with {@code useInExcerpt} set. Storing is required for the
     * query-side {@link org.apache.lucene.search.uhighlight.UnifiedHighlighter} to build
     * {@code rep:excerpt} snippets; without it excerpt/highlighting is broken (see
     * {@code LuceneNgHighlightingTest}).
     */
    private boolean storeFulltextForExcerpt;

    @Override
    protected boolean isFulltextValuePersistedAtNode(PropertyDefinition pd) {
        storeFulltextForExcerpt = pd.stored; // useInExcerpt
        return super.isFulltextValuePersistedAtNode(pd);
    }

    @Override
    protected void indexFulltextValue(Document doc, String value) {
        // The node-scope fulltext sink. TextField is tokenized/analyzed by the default analyzer.
        // Stored when the source property is useInExcerpt so the UnifiedHighlighter can read the
        // original text back to build rep:excerpt (see storeFulltextForExcerpt).
        doc.add(new TextField(FieldNames.FULLTEXT, value,
                storeFulltextForExcerpt ? Field.Store.YES : Field.Store.NO));
    }

    @Override
    protected void indexAggregateValue(Document doc, Aggregate.NodeIncludeResult result,
                                       String value, PropertyDefinition pd) {
        // Text from an aggregated child/relative node is folded into this (parent) document's
        // ":fulltext" field, so a fulltext query on the parent matches the child's content.
        //
        // oak-lucene additionally keys relative-node aggregates to a relative fulltext field and
        // applies pd.boost. This module does neither: it has no relative-fulltext field on the
        // query side, and Lucene 9 removed per-field index-time boosts. All aggregated values are
        // therefore folded into node-scope ":fulltext", which is the aggregation behaviour this
        // module supports. Aggregated content is not stored (excerpts over aggregated child content
        // are out of scope); added directly rather than via indexFulltextValue so it never inherits
        // the node-scope property's store flag.
        doc.add(new TextField(FieldNames.FULLTEXT, value, Field.Store.NO));
    }

    // -------------------------------------------------------------------------
    // Facets
    // -------------------------------------------------------------------------

    @Override
    protected boolean indexFacetProperty(Document doc, int tag, PropertyState property, String pname) {
        // Dimension -> index-field-name mapping and multi-valued flags are registered on the
        // shared FacetsConfig by the editor context.
        // Lucene facet labels cannot be empty; empty values are skipped, as in oak-lucene.
        boolean added = false;
        if (!property.isArray()) {
            String value = convertToString(property);
            if (value != null && !value.isEmpty()) {
                doc.add(new SortedSetDocValuesFacetField(pname, value));
                added = true;
            }
        } else {
            for (String value : convertAllToStrings(property)) {
                if (!value.isEmpty()) {
                    doc.add(new SortedSetDocValuesFacetField(pname, value));
                    added = true;
                }
            }
        }
        return added;
    }

    // -------------------------------------------------------------------------
    // Node name
    // -------------------------------------------------------------------------

    @Override
    protected void indexNodeName(Document doc, String value) {
        // The framework's addNodeNameField already strips the namespace prefix (local name only)
        // before calling this hook, so no stripping is done here.
        doc.add(new StringField(FieldNames.NODE_NAME, value, Field.Store.NO));
    }

    // -------------------------------------------------------------------------
    // Ancestors / path restrictions
    // -------------------------------------------------------------------------

    /**
     * Adds one {@link FieldNames#ANCESTORS} term per ancestor below the root, so that a descendant
     * path restriction is a single term query.
     */
    @Override
    protected void indexAncestors(Document doc, String path) {
        for (String ancestor = PathUtils.getParentPath(path); !PathUtils.denotesRoot(ancestor);
             ancestor = PathUtils.getParentPath(ancestor)) {
            doc.add(new StringField(FieldNames.ANCESTORS, ancestor, Field.Store.NO));
        }
    }

    // -------------------------------------------------------------------------
    // Documented no-ops: features not supported by this module (see README parity table)
    // -------------------------------------------------------------------------

    @Override
    protected boolean addBinary(Document doc, String path, List<String> binaryValues) {
        // Not supported — this module has no binary/Tika text extraction (see README
        // "Known limitations").
        return false;
    }

    @Override
    protected boolean indexDynamicBoost(Document doc, String parent, String nodeName, String value, double confidence) {
        return false; // dynamic boost: not supported (README parity table)
    }

    @Override
    protected boolean indexSimilarityTag(Document doc, String value) {
        return false; // similarity / MLT: not supported (README parity table)
    }

    @Override
    protected void indexSimilarityBinaries(Document doc, PropertyDefinition pd, Blob blob) {
        // no-op — similarity / MLT not supported
    }

    @Override
    protected void indexSimilarityStrings(Document doc, PropertyDefinition pd, String value) {
        // no-op — similarity / MLT not supported
    }

    @Override
    protected boolean augmentCustomFields(String path, Document doc, NodeState document) {
        return false; // IndexFieldProvider augmentors: not supported (README parity table)
    }

    @Override
    protected void indexSuggestValue(Document doc, String value) {
        // no-op — suggestions not supported (README parity table)
    }

    @Override
    protected void indexSpellcheckValue(Document doc, String value) {
        // no-op — spellcheck not supported (README parity table)
    }

    @Override
    protected void indexNotNullProperty(Document doc, PropertyDefinition pd) {
        doc.add(new StringField(FieldNames.NOT_NULL_PROPS, pd.name, Field.Store.NO));
    }

    @Override
    protected void indexNullProperty(Document doc, PropertyDefinition pd) {
        doc.add(new StringField(FieldNames.NULL_PROPS, pd.name, Field.Store.NO));
    }

    // -------------------------------------------------------------------------
    // Value conversion helpers (ported verbatim from LuceneNgIndexEditor)
    // -------------------------------------------------------------------------

    /** Reads value {@code i} as a Long, converting from Double/String. Returns null if unconvertible. */
    @Nullable
    private Long readAsLong(PropertyState prop, int i) {
        switch (prop.getType().tag()) {
            case PropertyType.LONG:
                return prop.getValue(Type.LONG, i);
            case PropertyType.DOUBLE:
                return prop.getValue(Type.DOUBLE, i).longValue();
            case PropertyType.STRING:
                try {
                    return Long.parseLong(prop.getValue(Type.STRING, i).trim());
                } catch (NumberFormatException e) {
                    return null;
                }
            default:
                return null;
        }
    }

    /** Reads value {@code i} as a Double, converting from Long/String. Returns null if unconvertible. */
    @Nullable
    private Double readAsDouble(PropertyState prop, int i) {
        switch (prop.getType().tag()) {
            case PropertyType.DOUBLE:
                return prop.getValue(Type.DOUBLE, i);
            case PropertyType.LONG:
                return prop.getValue(Type.LONG, i).doubleValue();
            case PropertyType.STRING:
                try {
                    return Double.parseDouble(prop.getValue(Type.STRING, i).trim());
                } catch (NumberFormatException e) {
                    return null;
                }
            default:
                return null;
        }
    }

    /** Reads value {@code i} as millis-since-epoch (ISO 8601 for Date/String). Returns null if unconvertible. */
    @Nullable
    private Long readAsDateMillis(PropertyState prop, int i) {
        String dateStr;
        switch (prop.getType().tag()) {
            case PropertyType.DATE:
                dateStr = prop.getValue(Type.DATE, i);
                break;
            case PropertyType.STRING:
                dateStr = prop.getValue(Type.STRING, i).trim();
                break;
            default:
                return null;
        }
        try {
            return ISO8601.parse(dateStr).getTimeInMillis();
        } catch (Exception e) {
            LOG.debug("Cannot parse date value '{}': {}", dateStr, e.getMessage());
            return null;
        }
    }

    @Nullable
    private String convertToString(PropertyState prop) {
        try {
            switch (prop.getType().tag()) {
                case PropertyType.STRING:
                    return prop.getValue(Type.STRING);
                case PropertyType.LONG:
                    return String.valueOf(prop.getValue(Type.LONG));
                case PropertyType.DOUBLE:
                    return String.valueOf(prop.getValue(Type.DOUBLE));
                case PropertyType.DATE:
                    return String.valueOf(ISO8601.parse(prop.getValue(Type.DATE)).getTimeInMillis());
                case PropertyType.BOOLEAN:
                    return String.valueOf(prop.getValue(Type.BOOLEAN));
                default:
                    return null;
            }
        } catch (Exception e) {
            LOG.error("Failed to convert property value to string for faceting ({})",
                    e.getClass().getSimpleName());
            return null;
        }
    }

    private Iterable<String> convertAllToStrings(PropertyState prop) {
        List<String> result = new ArrayList<>();
        try {
            switch (prop.getType().tag()) {
                case PropertyType.STRING:
                    prop.getValue(Type.STRINGS).forEach(result::add);
                    break;
                case PropertyType.LONG:
                    prop.getValue(Type.LONGS).forEach(v -> result.add(String.valueOf(v)));
                    break;
                case PropertyType.DOUBLE:
                    prop.getValue(Type.DOUBLES).forEach(v -> result.add(String.valueOf(v)));
                    break;
                case PropertyType.DATE:
                    for (String d : prop.getValue(Type.DATES)) {
                        try {
                            result.add(String.valueOf(ISO8601.parse(d).getTimeInMillis()));
                        } catch (Exception e) {
                            LOG.error("Failed to parse DATE facet value ({})", e.getClass().getSimpleName());
                        }
                    }
                    break;
                case PropertyType.BOOLEAN:
                    prop.getValue(Type.BOOLEANS).forEach(v -> result.add(String.valueOf(v)));
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            LOG.error("Failed to convert property values to strings for faceting ({})",
                    e.getClass().getSimpleName());
        }
        return result;
    }
}
