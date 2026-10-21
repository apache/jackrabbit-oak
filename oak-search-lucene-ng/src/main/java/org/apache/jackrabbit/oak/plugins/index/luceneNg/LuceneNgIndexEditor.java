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
package org.apache.jackrabbit.oak.plugins.index.luceneNg;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.directory.OakDirectory;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition.IndexingRule;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.util.ISO8601;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.util.BytesRef;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.PropertyType;
import java.io.IOException;

/**
 * IndexEditor for Lucene 9.
 *
 * <p>Only indexes properties that are explicitly declared in the index definition's
 * {@code indexRules}. This mirrors the behaviour of the legacy {@code oak-lucene}
 * module and avoids the Lucene doc-values type-consistency constraint: since the
 * declared type for a property is fixed at index-definition time, every document
 * that contributes a doc-values field for that property will use the same type.</p>
 */
public class LuceneNgIndexEditor implements Editor {
    private static final Logger LOG = LoggerFactory.getLogger(LuceneNgIndexEditor.class);

    private final String path;
    private final String indexPath;
    private final NodeBuilder definition;
    private final NodeState root;
    private final IndexWriter indexWriter;
    private final boolean isRoot;
    private final LuceneNgIndexDefinition indexDefinition;
    private final IndexUpdateCallback callback;
    private final FacetsConfig facetsConfig;

    /**
     * Creates a new LuceneNgIndexEditor (root editor with new IndexWriter).
     *
     * @param path           the content path being indexed (starts at "/")
     * @param indexPath      the index definition path (e.g. "/oak:index/myIndex")
     * @param storageBuilder the NodeBuilder at the index storage path
     *                       ({@code /oak:index/<idx>/lucene9})
     * @param definition     the index definition NodeBuilder
     * @param root           the root node state
     * @param reindex        whether to wipe existing data (full reindex)
     */
    public LuceneNgIndexEditor(@NotNull String path,
                               @NotNull String indexPath,
                               @NotNull NodeBuilder storageBuilder,
                               @NotNull NodeBuilder definition,
                               @NotNull NodeState root,
                               boolean reindex,
                               @NotNull IndexUpdateCallback callback) throws IOException {
        this.path = path;
        this.indexPath = indexPath;
        this.definition = definition;
        this.root = root;
        this.isRoot = true;
        this.callback = callback;
        this.indexDefinition = new LuceneNgIndexDefinition(root, definition.getNodeState(), indexPath);
        this.facetsConfig = buildFacetsConfig(this.indexDefinition);

        String indexName = PathUtils.getName(indexPath);
        OakDirectory directory = new OakDirectory(storageBuilder, indexName, false);
        IndexWriterConfig config = new IndexWriterConfig();
        if (reindex) {
            config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            LOG.debug("Reindexing: wiping existing index data for {}", indexPath);
        }
        try {
            this.indexWriter = new IndexWriter(directory, config);
        } catch (IOException e) {
            directory.close();
            throw e;
        }

        LOG.debug("Created LuceneNgIndexEditor for index: {}", indexPath);
    }

    /**
     * Convenience constructor for tests: uses {@link LuceneNgIndexStorage#getOrCreateStorageBuilder(NodeBuilder)}
     * under {@code definition} as the Lucene directory root.
     */
    public LuceneNgIndexEditor(@NotNull String path,
                               @NotNull NodeBuilder definition,
                               @NotNull NodeState root) throws IOException {
        this(path, "/oak:index/default", LuceneNgIndexStorage.getOrCreateStorageBuilder(definition), definition, root, false, () -> {});
    }

    /**
     * Convenience constructor for tests that need to verify callback behaviour.
     */
    public LuceneNgIndexEditor(@NotNull String path,
                               @NotNull NodeBuilder definition,
                               @NotNull NodeState root,
                               @NotNull IndexUpdateCallback callback) throws IOException {
        this(path, "/oak:index/default", LuceneNgIndexStorage.getOrCreateStorageBuilder(definition), definition, root, false, callback);
    }

    /**
     * Creates a child LuceneNgIndexEditor that shares the parent's IndexWriter
     * and pre-built IndexDefinition.
     */
    private LuceneNgIndexEditor(@NotNull String path,
                                @NotNull String indexPath,
                                @NotNull NodeBuilder definition,
                                @NotNull NodeState root,
                                @NotNull IndexWriter sharedWriter,
                                @NotNull LuceneNgIndexDefinition indexDefinition,
                                @NotNull FacetsConfig facetsConfig,
                                @NotNull IndexUpdateCallback callback) {
        this.path = path;
        this.indexPath = indexPath;
        this.definition = definition;
        this.root = root;
        this.indexWriter = sharedWriter;
        this.isRoot = false;
        this.indexDefinition = indexDefinition;
        this.facetsConfig = facetsConfig;
        this.callback = callback;
    }

    @Override
    public void enter(@NotNull NodeState before, @NotNull NodeState after)
            throws CommitFailedException {
        if (indexDefinition.getFilterResult(path) == PathFilter.Result.INCLUDE) {
            try {
                indexNode(after);
            } catch (IOException | RuntimeException e) {
                throw new CommitFailedException("Lucene9", 1,
                        "Failed to index node at " + path, e);
            }
        }
    }

    @Override
    public void leave(@NotNull NodeState before, @NotNull NodeState after)
            throws CommitFailedException {
        if (isRoot) {
            try {
                indexWriter.commit();
                LOG.debug("Committed Lucene 9 index");
            } catch (IOException e) {
                throw new CommitFailedException("Lucene9", 2,
                        "Failed to commit index", e);
            } finally {
                try {
                    indexWriter.close();
                } catch (IOException e) {
                    LOG.warn("Failed to close IndexWriter for {}", indexPath, e);
                }
            }
        }
    }

    @Override
    public void propertyAdded(@NotNull PropertyState after) throws CommitFailedException {}

    @Override
    public void propertyChanged(@NotNull PropertyState before, @NotNull PropertyState after)
            throws CommitFailedException {}

    @Override
    public void propertyDeleted(@NotNull PropertyState before) throws CommitFailedException {}

    @Override
    @Nullable
    public Editor childNodeAdded(@NotNull String name, @NotNull NodeState after)
            throws CommitFailedException {
        String childPath = buildChildPath(name);
        if (indexDefinition.getFilterResult(childPath) == PathFilter.Result.EXCLUDE) {
            return null;
        }
        return new LuceneNgIndexEditor(childPath, indexPath, definition, root,
                indexWriter, indexDefinition, facetsConfig, callback);
    }

    @Override
    @Nullable
    public Editor childNodeChanged(@NotNull String name,
                                   @NotNull NodeState before,
                                   @NotNull NodeState after)
            throws CommitFailedException {
        String childPath = buildChildPath(name);
        if (indexDefinition.getFilterResult(childPath) == PathFilter.Result.EXCLUDE) {
            return null;
        }
        return new LuceneNgIndexEditor(childPath, indexPath, definition, root,
                indexWriter, indexDefinition, facetsConfig, callback);
    }

    @Override
    @Nullable
    public Editor childNodeDeleted(@NotNull String name, @NotNull NodeState before)
            throws CommitFailedException {
        String childPath = buildChildPath(name);
        try {
            indexWriter.deleteDocuments(new Term(FieldNames.PATH, childPath));
            indexWriter.deleteDocuments(new PrefixQuery(new Term(FieldNames.PATH, childPath + "/")));
            LOG.debug("Deleted index documents for removed node: {}", childPath);
        } catch (IOException e) {
            throw new CommitFailedException("Lucene9", 3,
                    "Failed to delete index documents for " + childPath, e);
        }
        return null;
    }

    private String buildChildPath(String name) {
        if (path.isEmpty() || path.equals("/")) {
            return "/" + name;
        }
        return path + "/" + name;
    }

    /**
     * Traverses {@code relativePath} (a sequence of child-node names separated by {@code /})
     * starting from {@code base} and returns the resulting {@link NodeState}, or {@code null}
     * if any step along the path is missing.
     *
     * <p>An empty path returns {@code base} itself.</p>
     */
    @Nullable
    private NodeState traverseRelativePath(@NotNull NodeState base, @NotNull String relativePath) {
        if (relativePath.isEmpty()) {
            return base;
        }
        NodeState current = base;
        for (String segment : PathUtils.elements(relativePath)) {
            current = current.getChildNode(segment);
            if (!current.exists()) {
                return null;
            }
        }
        return current;
    }

    // -------------------------------------------------------------------------
    // Indexing
    // -------------------------------------------------------------------------

    private static FacetsConfig buildFacetsConfig(LuceneNgIndexDefinition definition) {
        FacetsConfig config = new FacetsConfig();
        for (IndexingRule rule : definition.getDefinedRules()) {
            for (PropertyDefinition pd : rule.getProperties()) {
                if (pd.facet) {
                    config.setIndexFieldName(pd.name, FieldNames.createFacetFieldName(pd.name));
                    config.setMultiValued(pd.name, true);
                }
            }
        }
        return config;
    }

    /**
     * Indexes the properties of {@code node} into Lucene, respecting index rules.
     *
     * <p>Only nodes whose {@code jcr:primaryType} (or mixin types) match a declared
     * {@code indexRule} are indexed. Within a matching rule, only properties that
     * have an explicit {@link PropertyDefinition} with {@code index=true} produce
     * Lucene fields. This guarantees that the Lucene doc-values type for a given
     * field name is always the same across all documents, since the declared property
     * type is fixed at index-definition time.</p>
     */
    private void indexNode(NodeState node) throws IOException {
        // Resolve the indexing rule for this node's primary type / mixins.
        // Returns null when no rule covers this node type — skip entirely.
        IndexingRule rule = indexDefinition.getApplicableIndexingRule(node);
        if (rule == null) {
            LOG.trace("No applicable rule for node at {} (primaryType={})", path,
                    node.getString("jcr:primaryType"));
            return;
        }

        Document doc = new Document();

        // Path fields are always added — they use the ":path" / ":parent" prefixes
        // which cannot collide with JCR property names.
        doc.add(new StringField(FieldNames.PATH, path, Field.Store.YES));
        int lastSlash = path.lastIndexOf('/');
        String parentPath = lastSlash == 0 ? "/" : path.substring(0, lastSlash);
        doc.add(new StringField(LuceneNgIndexConstants.FIELD_PARENT_PATH, parentPath, Field.Store.NO));

        boolean hasIndexedProperty = false;

        // NODE_NAME field: local name (namespace prefix stripped) for localname() queries.
        // Only written when the indexing rule declares indexNodeName=true.
        if (rule.isNodeNameIndexed()) {
            String localName = PathUtils.getName(path);
            int colon = localName.indexOf(':');
            String value = colon < 0 ? localName : localName.substring(colon + 1);
            if (!value.isEmpty()) {
                doc.add(new StringField(FieldNames.NODE_NAME, value, Field.Store.NO));
                hasIndexedProperty = true;
            }
        }

        for (PropertyState prop : node.getProperties()) {
            String propName = prop.getName();

            // Hidden properties (e.g. jcr:primaryType stored as ":primaryType") are skipped.
            if (propName.startsWith(":")) {
                continue;
            }

            // Only index direct (non-relative) properties declared in the rule.
            PropertyDefinition pd = rule.getConfig(propName);
            if (pd == null || !pd.index || pd.relative) {
                continue;
            }

            boolean added = indexProperty(doc, prop, propName, pd);
            if (added) {
                hasIndexedProperty = true;
            }
        }

        // Second pass: relative properties (pd.name contains '/', e.g. "jcr:content/metadata/dc:title").
        // Traverse the child-node path and index the leaf property into this document.
        for (PropertyDefinition pd : rule.getProperties()) {
            if (!pd.relative || !pd.index || pd.isRegexp) {
                continue;
            }
            String relPath = pd.name;                                  // e.g. "jcr:content/metadata/dc:title"
            String leafName = PathUtils.getName(relPath);              // e.g. "dc:title"
            String relParentPath = PathUtils.getParentPath(relPath);   // e.g. "jcr:content/metadata"
            NodeState childNode = traverseRelativePath(node, relParentPath);
            if (childNode == null) {
                continue;
            }
            PropertyState prop = childNode.getProperty(leafName);
            if (prop == null) {
                continue;
            }
            // Use pd.name as the Lucene field name so property-index queries
            // using the full relative path hit the right field.
            boolean added = indexProperty(doc, prop, pd.name, pd);
            if (added) {
                hasIndexedProperty = true;
            }
        }

        if (!hasIndexedProperty) {
            return;
        }

        indexWriter.updateDocument(new Term(FieldNames.PATH, path), facetsConfig.build(doc));
        LOG.debug("Indexed node at path: {}", path);
        try {
            callback.indexUpdate();
        } catch (CommitFailedException e) {
            throw new IOException("IndexUpdateCallback failed at " + path, e);
        }
    }

    /**
     * Adds Lucene fields for a single property according to its {@link PropertyDefinition}.
     *
     * <p>The Lucene field type is driven by the <em>declared</em> type in the index definition
     * ({@code pd.getType()}), not the actual Oak property type. This guarantees that all
     * documents contribute the same Lucene field schema for a given field name — a requirement
     * enforced by Lucene 9's {@code IndexingChain}.
     *
     * <p>When a property is explicitly declared as Long/Double/Date but the actual Oak value is
     * a String, the value is converted. If conversion fails, the property is skipped for this
     * document (no field added) rather than falling through to an incompatible field type.</p>
     *
     * @return {@code true} if at least one field was added to {@code doc}
     */
    private boolean indexProperty(Document doc, PropertyState prop,
                                  String propName, PropertyDefinition pd) {
        int maxFieldLength = IndexDefinition.DEFAULT_MAX_FIELD_LENGTH;
        boolean added = false;

        if (pd.isTypeDefined()) {
            // The declaration fixes the Lucene field type. Convert the actual value to match.
            switch (pd.getType()) {
                case PropertyType.LONG: {
                    Long lv = readAsLong(prop);
                    if (lv != null) {
                        doc.add(new LongPoint(propName, lv));
                        if (pd.ordered) {
                            doc.add(new NumericDocValuesField(propName, lv));
                        }
                        added = true;
                    } else {
                        LOG.debug("Skipping property '{}': declared Long but value '{}' cannot be converted",
                                propName, prop.getValue(org.apache.jackrabbit.oak.api.Type.STRING));
                    }
                    break;
                }
                case PropertyType.DOUBLE: {
                    Double dv = readAsDouble(prop);
                    if (dv != null) {
                        doc.add(new DoublePoint(propName, dv));
                        if (pd.ordered) {
                            doc.add(new DoubleDocValuesField(propName, dv));
                        }
                        added = true;
                    } else {
                        LOG.debug("Skipping property '{}': declared Double but value cannot be converted", propName);
                    }
                    break;
                }
                case PropertyType.DATE: {
                    Long millis = readAsDateMillis(prop);
                    if (millis != null) {
                        doc.add(new LongPoint(propName, millis));
                        if (pd.ordered) {
                            doc.add(new NumericDocValuesField(propName, millis));
                        }
                        added = true;
                    } else {
                        LOG.debug("Skipping property '{}': declared Date but value cannot be converted", propName);
                    }
                    break;
                }
                default:
                    // Declared as String (or another non-numeric type): fall through to
                    // the actual-type dispatch below so string/boolean handling is unchanged.
                    added = indexByActualType(doc, prop, propName, pd, maxFieldLength);
                    break;
            }
        } else {
            // No explicit type declaration: drive field type from the actual Oak value type.
            added = indexByActualType(doc, prop, propName, pd, maxFieldLength);
        }

        // Facet field — only when pd.facet is true
        if (added && pd.facet) {
            added = indexFacetField(doc, prop, propName) || added;
        }

        return added;
    }

    /**
     * Indexes a property using its actual Oak value type (legacy path, used when no explicit
     * type is declared in the index definition).
     */
    private boolean indexByActualType(Document doc, PropertyState prop,
                                      String propName, PropertyDefinition pd, int maxFieldLength) {
        switch (prop.getType().tag()) {
            case PropertyType.LONG:
                if (!prop.isArray()) {
                    long lv = prop.getValue(org.apache.jackrabbit.oak.api.Type.LONG);
                    doc.add(new StringField(propName, String.valueOf(lv), Field.Store.NO));
                    return true;
                }
                break;
            case PropertyType.DOUBLE:
                if (!prop.isArray()) {
                    double dv = prop.getValue(org.apache.jackrabbit.oak.api.Type.DOUBLE);
                    doc.add(new StringField(propName, String.valueOf(dv), Field.Store.NO));
                    return true;
                }
                break;
            case PropertyType.BOOLEAN:
                if (!prop.isArray()) {
                    boolean bv = prop.getValue(org.apache.jackrabbit.oak.api.Type.BOOLEAN);
                    doc.add(new StringField(propName, String.valueOf(bv), Field.Store.NO));
                    return true;
                }
                break;
            case PropertyType.STRING:
                return indexStringProperty(doc, prop, propName, pd, maxFieldLength);
            default:
                break;
        }
        return false;
    }

    /**
     * Reads a property value as a Long, converting from String if necessary.
     * Returns {@code null} when the value is an array, an unsupported type, or unparseable.
     */
    @Nullable
    private Long readAsLong(PropertyState prop) {
        if (prop.isArray()) {
            return null;
        }
        switch (prop.getType().tag()) {
            case PropertyType.LONG:
                return prop.getValue(org.apache.jackrabbit.oak.api.Type.LONG);
            case PropertyType.DOUBLE:
                return prop.getValue(org.apache.jackrabbit.oak.api.Type.DOUBLE).longValue();
            case PropertyType.STRING:
                try {
                    return Long.parseLong(prop.getValue(org.apache.jackrabbit.oak.api.Type.STRING).trim());
                } catch (NumberFormatException e) {
                    return null;
                }
            default:
                return null;
        }
    }

    /**
     * Reads a property value as a Double, converting from String if necessary.
     * Returns {@code null} when the value is an array, an unsupported type, or unparseable.
     */
    @Nullable
    private Double readAsDouble(PropertyState prop) {
        if (prop.isArray()) {
            return null;
        }
        switch (prop.getType().tag()) {
            case PropertyType.DOUBLE:
                return prop.getValue(org.apache.jackrabbit.oak.api.Type.DOUBLE);
            case PropertyType.LONG:
                return prop.getValue(org.apache.jackrabbit.oak.api.Type.LONG).doubleValue();
            case PropertyType.STRING:
                try {
                    return Double.parseDouble(prop.getValue(org.apache.jackrabbit.oak.api.Type.STRING).trim());
                } catch (NumberFormatException e) {
                    return null;
                }
            default:
                return null;
        }
    }

    /**
     * Reads a property value as milliseconds-since-epoch for date indexing,
     * converting from ISO 8601 string if necessary.
     * Returns {@code null} when the value cannot be converted.
     */
    @Nullable
    private Long readAsDateMillis(PropertyState prop) {
        if (prop.isArray()) {
            return null;
        }
        String dateStr;
        switch (prop.getType().tag()) {
            case PropertyType.DATE:
                dateStr = prop.getValue(org.apache.jackrabbit.oak.api.Type.DATE);
                break;
            case PropertyType.STRING:
                dateStr = prop.getValue(org.apache.jackrabbit.oak.api.Type.STRING).trim();
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

    private boolean indexStringProperty(Document doc, PropertyState prop,
                                        String propName, PropertyDefinition pd,
                                        int maxFieldLength) {
        Field.Store fulltextStore = pd.stored ? Field.Store.YES : Field.Store.NO;
        boolean added = false;

        if (!prop.isArray()) {
            String sv = prop.getValue(org.apache.jackrabbit.oak.api.Type.STRING);
            // An ordered property is implicitly indexed (needed for sorting).
            if ((pd.propertyIndex || pd.ordered) && sv.length() < maxFieldLength) {
                doc.add(new StringField(propName, sv, Field.Store.NO));
                if (pd.ordered) {
                    doc.add(new SortedDocValuesField(propName, new BytesRef(
                            sv.length() <= maxFieldLength ? sv : sv.substring(0, maxFieldLength))));
                }
                added = true;
            }
            if (pd.nodeScopeIndex) {
                doc.add(new TextField(FieldNames.FULLTEXT, sv, fulltextStore));
                added = true;
            }
        } else {
            for (String sv : prop.getValue(org.apache.jackrabbit.oak.api.Type.STRINGS)) {
                if ((pd.propertyIndex || pd.ordered) && sv.length() < maxFieldLength) {
                    doc.add(new StringField(propName, sv, Field.Store.NO));
                    added = true;
                }
                if (pd.nodeScopeIndex) {
                    doc.add(new TextField(FieldNames.FULLTEXT, sv, fulltextStore));
                    added = true;
                }
            }
        }
        return added;
    }

    private boolean indexFacetField(Document doc, PropertyState prop, String propName) {
        boolean added = false;

        if (!prop.isArray()) {
            String value = convertToString(prop);
            if (value != null) {
                doc.add(new SortedSetDocValuesFacetField(propName, value));
                added = true;
            }
        } else {
            for (String value : convertAllToStrings(prop)) {
                doc.add(new SortedSetDocValuesFacetField(propName, value));
                added = true;
            }
        }
        return added;
    }

    // -------------------------------------------------------------------------
    // Type conversion helpers (for faceting)
    // -------------------------------------------------------------------------

    @Nullable
    private String convertToString(PropertyState prop) {
        try {
            switch (prop.getType().tag()) {
                case PropertyType.STRING:
                    return prop.getValue(org.apache.jackrabbit.oak.api.Type.STRING);
                case PropertyType.LONG:
                    return String.valueOf(prop.getValue(org.apache.jackrabbit.oak.api.Type.LONG));
                case PropertyType.DOUBLE:
                    return String.valueOf(prop.getValue(org.apache.jackrabbit.oak.api.Type.DOUBLE));
                case PropertyType.DATE:
                    return String.valueOf(
                            ISO8601.parse(prop.getValue(org.apache.jackrabbit.oak.api.Type.DATE))
                                   .getTimeInMillis());
                case PropertyType.BOOLEAN:
                    return String.valueOf(prop.getValue(org.apache.jackrabbit.oak.api.Type.BOOLEAN));
                default:
                    return null;
            }
        } catch (Exception e) {
            LOG.error("Failed to convert property value to string for faceting", e);
            return null;
        }
    }

    @NotNull
    private Iterable<String> convertAllToStrings(PropertyState prop) {
        java.util.List<String> result = new java.util.ArrayList<>();
        try {
            switch (prop.getType().tag()) {
                case PropertyType.STRING:
                    prop.getValue(org.apache.jackrabbit.oak.api.Type.STRINGS).forEach(result::add);
                    break;
                case PropertyType.LONG:
                    prop.getValue(org.apache.jackrabbit.oak.api.Type.LONGS)
                            .forEach(v -> result.add(String.valueOf(v)));
                    break;
                case PropertyType.DOUBLE:
                    prop.getValue(org.apache.jackrabbit.oak.api.Type.DOUBLES)
                            .forEach(v -> result.add(String.valueOf(v)));
                    break;
                case PropertyType.DATE:
                    for (String d : prop.getValue(org.apache.jackrabbit.oak.api.Type.DATES)) {
                        try {
                            result.add(String.valueOf(ISO8601.parse(d).getTimeInMillis()));
                        } catch (Exception e) {
                            LOG.error("Failed to parse date: {}", d, e);
                        }
                    }
                    break;
                case PropertyType.BOOLEAN:
                    prop.getValue(org.apache.jackrabbit.oak.api.Type.BOOLEANS)
                            .forEach(v -> result.add(String.valueOf(v)));
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
