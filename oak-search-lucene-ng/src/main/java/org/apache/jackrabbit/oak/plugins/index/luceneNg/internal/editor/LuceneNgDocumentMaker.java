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
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.util.BytesRef;
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
 * <p>The Lucene field types produced here are a direct port of the hand-rolled
 * {@code LuceneNgIndexEditor} (declared-type dispatch, single-value ordered doc-values,
 * string/facet/node-name handling). The field-<em>selection</em> gating (which hook fires
 * for which {@link PropertyDefinition} flag) is handled entirely by the framework's
 * {@code makeDocument} template method; these hooks only create the fields once invoked.</p>
 *
 * <p>Reusing the framework brings index-time <b>aggregation</b> to this module for the first
 * time: {@link #indexAggregateValue} routes a matched child/relative node's text into the
 * parent's {@code :fulltext} field.</p>
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
        // cannot collide with JCR property names. Ported from LuceneNgIndexEditor.indexNode.
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
     * <p>Port of {@code LuceneNgIndexEditor.indexProperty}'s declared-type dispatch: when the
     * index definition declares Long/Double/Date, the value is converted and a numeric point
     * field is written (guaranteeing a consistent Lucene field type across all documents);
     * otherwise the field type is driven by the actual Oak value type (String exact-match).</p>
     */
    @Override
    protected void indexTypedProperty(Document doc, PropertyState property, String pname,
                                      PropertyDefinition pd, int i) {
        if (pd.isTypeDefined()) {
            switch (pd.getType()) {
                case PropertyType.LONG: {
                    Long lv = readAsLong(property, i);
                    if (lv != null) {
                        doc.add(new LongPoint(pname, lv));
                    } else {
                        LOG.debug("Skipping property '{}': declared Long but value cannot be converted", pname);
                    }
                    return;
                }
                case PropertyType.DOUBLE: {
                    Double dv = readAsDouble(property, i);
                    if (dv != null) {
                        doc.add(new DoublePoint(pname, dv));
                    } else {
                        LOG.debug("Skipping property '{}': declared Double but value cannot be converted", pname);
                    }
                    return;
                }
                case PropertyType.DATE: {
                    Long millis = readAsDateMillis(property, i);
                    if (millis != null) {
                        doc.add(new LongPoint(pname, millis));
                    } else {
                        LOG.debug("Skipping property '{}': declared Date but value cannot be converted", pname);
                    }
                    return;
                }
                default:
                    // Declared as String (or another non-numeric type): fall through to
                    // actual-type dispatch so string/boolean handling is unchanged.
                    indexByActualType(doc, property, pname, pd, i);
                    return;
            }
        }
        // No explicit type declaration: drive field type from the actual Oak value type.
        indexByActualType(doc, property, pname, pd, i);
    }

    /**
     * Indexes the value at position {@code i} using the property's actual Oak value type
     * (port of {@code LuceneNgIndexEditor.indexByActualType} / the exact-match portion of
     * {@code indexStringProperty}). Numeric/boolean values are indexed as string exact-match
     * fields and, matching the pre-refactor editor, only when the property is single-valued.
     * Binary values are ignored here (never call {@code getValue(STRING)} on a binary).
     */
    private void indexByActualType(Document doc, PropertyState property, String pname,
                                   PropertyDefinition pd, int i) {
        switch (property.getType().tag()) {
            case PropertyType.LONG:
                if (!property.isArray()) {
                    doc.add(new StringField(pname, String.valueOf(property.getValue(Type.LONG, i)), Field.Store.NO));
                }
                break;
            case PropertyType.DOUBLE:
                if (!property.isArray()) {
                    doc.add(new StringField(pname, String.valueOf(property.getValue(Type.DOUBLE, i)), Field.Store.NO));
                }
                break;
            case PropertyType.BOOLEAN:
                if (!property.isArray()) {
                    doc.add(new StringField(pname, String.valueOf(property.getValue(Type.BOOLEAN, i)), Field.Store.NO));
                }
                break;
            case PropertyType.STRING: {
                String sv = property.getValue(Type.STRING, i);
                if (sv.length() < MAX_FIELD_LENGTH) {
                    doc.add(new StringField(pname, sv, Field.Store.NO));
                }
                // Multi-valued ordered String: write the SORTED_SET sort doc-value here, per value.
                // The framework's addTypedOrderedFields rejects arrays before indexTypeOrderedFields
                // ever runs, so without this a multi-valued ordered String would contribute the
                // "pname" StringField (doc-values type NONE) with NO doc-value, while a single-valued
                // sibling writes SORTED_SET for the same field name -> Lucene rejects the whole
                // document ("Inconsistency of field data structures ... expected SORTED_SET, but it
                // has NONE"), silently dropping it. Writing SORTED_SET for every value (matching the
                // single-valued branch in indexTypeOrderedFields and the pre-refactor hand-rolled
                // editor) keeps the field's doc-values type consistent across cardinalities AND
                // restores multi-valued sort (the query side already uses a SortedSetSortField for
                // SORTED_SET fields, selecting the minimum value). Single-valued values are handled by
                // indexTypeOrderedFields, so only the array case is written here to avoid duplication.
                if (pd.ordered && property.isArray()) {
                    doc.add(new SortedSetDocValuesField(pname, new BytesRef(
                            sv.length() <= MAX_FIELD_LENGTH ? sv : sv.substring(0, MAX_FIELD_LENGTH))));
                }
                break;
            }
            default:
                break;
        }
    }

    /**
     * Adds the ordered doc-values (sort) field for a <em>single-valued</em> property. The framework's
     * {@code FulltextDocumentMaker.addTypedOrderedFields} rejects <em>all</em> array-valued properties
     * (with a warning) before this hook is ever called, so only single values reach here. Multi-valued
     * ordered <em>String</em> properties are handled elsewhere: {@link #indexByActualType} writes their
     * sort doc-values per value (that method runs in the {@code propertyIndex} path, which the framework
     * <em>does</em> call for each array element). Both paths write a {@link SortedSetDocValuesField}
     * under the plain property name, so a field indexed as ordered String has a consistent SORTED_SET
     * doc-values type whether a given node stores one value or many — which is required both for
     * multi-valued sort to work (the query side sorts SORTED_SET fields via {@code SortedSetSortField},
     * selecting the minimum value) and to avoid a doc-values-type inconsistency that would otherwise make
     * Lucene drop a document in a mixed single/multi-valued commit. This matches the pre-refactor
     * hand-rolled editor.
     *
     * <p>Note the doc-values field name is the plain property name (as in the pre-refactor editor), not
     * {@code createDocValFieldName}, keeping written indexes readable across the migration. The ordered
     * <em>String</em> case uses a {@link SortedSetDocValuesField} (rather than {@link SortedDocValuesField})
     * so its type matches the multi-valued values written by {@link #indexByActualType} for the same field
     * name; a single-element sorted set sorts identically to a single sorted value.</p>
     */
    @Override
    protected boolean indexTypeOrderedFields(Document doc, String pname, int tag, PropertyState property,
                                             PropertyDefinition pd) {
        switch (tag) {
            case PropertyType.LONG: {
                Long lv = readAsLong(property, 0);
                if (lv == null) {
                    return false;
                }
                doc.add(new NumericDocValuesField(pname, lv));
                return true;
            }
            case PropertyType.DOUBLE: {
                Double dv = readAsDouble(property, 0);
                if (dv == null) {
                    return false;
                }
                doc.add(new DoubleDocValuesField(pname, dv));
                return true;
            }
            case PropertyType.DATE: {
                Long millis = readAsDateMillis(property, 0);
                if (millis == null) {
                    return false;
                }
                doc.add(new NumericDocValuesField(pname, millis));
                return true;
            }
            case PropertyType.BOOLEAN: {
                String bv = String.valueOf(property.getValue(Type.BOOLEAN));
                doc.add(new SortedDocValuesField(pname, new BytesRef(bv)));
                return true;
            }
            case PropertyType.STRING: {
                String sv = property.getValue(Type.STRING);
                doc.add(new SortedSetDocValuesField(pname, new BytesRef(
                        sv.length() <= MAX_FIELD_LENGTH ? sv : sv.substring(0, MAX_FIELD_LENGTH))));
                return true;
            }
            default:
                return false;
        }
    }

    // -------------------------------------------------------------------------
    // Fulltext / analyzed / aggregation
    // -------------------------------------------------------------------------

    @Override
    protected void indexAnalyzedProperty(Document doc, String pname, String value, PropertyDefinition pd) {
        // No-op: this module writes no per-property analyzed field (no "full:<prop>" field).
        // Node-scope fulltext content is served entirely by the ":fulltext" TextField added via
        // indexFulltextValue (nodeScopeIndex) and indexAggregateValue. Kept as a documented no-op
        // to preserve the pre-refactor field output exactly (LuceneNgIndexEditor never produced a
        // separate analyzed field either).
    }

    /**
     * Whether the nodeScope fulltext property currently being indexed is {@code useInExcerpt}
     * ({@code pd.stored}). Captured in {@link #isFulltextValuePersistedAtNode(PropertyDefinition)},
     * which the framework invokes for each nodeScope value immediately before
     * {@link #indexFulltextValue(Document, String)}, so the {@code :fulltext} field is <em>stored</em>
     * for exactly the properties the pre-refactor editor stored it for. Storing is required for the
     * query-side {@link org.apache.lucene.search.uhighlight.UnifiedHighlighter} to build
     * {@code rep:excerpt} snippets; without it excerpt/highlighting is broken (see
     * {@code LuceneNgHighlightingTest}). Restores behaviour lost when this module adopted the shared
     * {@code FulltextDocumentMaker} (the hand-rolled editor wrote {@code TextField(:fulltext, v,
     * pd.stored ? YES : NO)}).
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
        // The concrete payoff of the framework migration: text from an aggregated child/relative
        // node is folded into this (parent) document's ":fulltext" field, so a fulltext query on
        // the parent matches the child's content.
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
        // Port of LuceneNgIndexEditor.indexFacetField. Dimension -> index-field-name mapping and
        // multi-valued flags are registered on the shared FacetsConfig by the editor context.
        boolean added = false;
        if (!property.isArray()) {
            String value = convertToString(property);
            if (value != null) {
                doc.add(new SortedSetDocValuesFacetField(pname, value));
                added = true;
            }
        } else {
            for (String value : convertAllToStrings(property)) {
                doc.add(new SortedSetDocValuesFacetField(pname, value));
                added = true;
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

    @Override
    protected void indexAncestors(Document doc, String path) {
        // No-op. The framework only calls this when definition.evaluatePathRestrictions() is true
        // (default false). This module has never indexed ancestor path terms: its query side uses
        // the ":parent" field (written in initDoc) for direct-child path queries and does not read
        // FieldNames.ANCESTORS / :depth at all. Porting oak-lucene's ancestor/depth fields would
        // add fields nothing here consumes; keeping this a no-op preserves pre-refactor behaviour.
        // Ancestor-based path-restriction support would be a separate, future enhancement.
    }

    // -------------------------------------------------------------------------
    // Documented no-ops: features not supported by this module (see README parity table)
    // -------------------------------------------------------------------------

    @Override
    protected boolean addBinary(Document doc, String path, List<String> binaryValues) {
        // Not supported — this module has no binary/Tika text extraction (see README
        // "Known limitations"). Matches pre-refactor behaviour: binaries were never indexed.
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
        // no-op — not-null marker fields are not part of this module's feature set
    }

    @Override
    protected void indexNullProperty(Document doc, PropertyDefinition pd) {
        // no-op — see indexNotNullProperty
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
            LOG.error("Failed to convert property value to string for faceting", e);
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
                            LOG.error("Failed to parse date: {}", d, e);
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
            LOG.error("Failed to convert property values to strings for faceting", e);
        }
        return result;
    }
}
