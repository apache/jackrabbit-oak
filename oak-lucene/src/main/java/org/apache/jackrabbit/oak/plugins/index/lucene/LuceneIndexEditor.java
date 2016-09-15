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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.CountingInputStream;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.io.LazyInputStream;
import org.apache.jackrabbit.oak.plugins.index.IndexEditor;
import org.apache.jackrabbit.oak.plugins.index.PathFilter;
import org.apache.jackrabbit.oak.plugins.index.fulltext.ExtractedText;
import org.apache.jackrabbit.oak.plugins.index.fulltext.ExtractedText.ExtractionResult;
import org.apache.jackrabbit.oak.plugins.index.lucene.Aggregate.Matcher;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriter;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.StringPropertyState;
import org.apache.jackrabbit.oak.plugins.tree.TreeFactory;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.BlobByteSource;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.util.BytesRef;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.WriteOutContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.JcrConstants.JCR_DATA;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.*;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.ConfigUtil.getPrimaryTypeName;

/**
 * {@link IndexEditor} implementation that is responsible for keeping the
 * {@link LuceneIndex} up to date
 *
 * @see LuceneIndex
 */
public class LuceneIndexEditor implements IndexEditor, Aggregate.AggregateRoot {

    private static final Logger log =
            LoggerFactory.getLogger(LuceneIndexEditor.class);

    private static final long SMALL_BINARY = Long.getLong("oak.lucene.smallBinary", 16 * 1024);

    static final String TEXT_EXTRACTION_ERROR = "TextExtractionError";

    private final LuceneIndexEditorContext context;

    /** Name of this node, or {@code null} for the root node. */
    private final String name;

    /** Parent editor or {@code null} if this is the root editor. */
    private final LuceneIndexEditor parent;

    /** Path of this editor, built lazily in {@link #getPath()}. */
    private String path;

    private boolean propertiesChanged = false;

    private List<PropertyState> propertiesModified = Lists.newArrayList();

    /**
     * Flag indicating if the current tree being traversed has a deleted parent.
     */
    private final boolean isDeleted;

    private Tree afterTree;

    private Tree beforeTree;

    private IndexDefinition.IndexingRule indexingRule;

    private List<Matcher> currentMatchers = Collections.emptyList();

    private final MatcherState matcherState;

    private final PathFilter.Result pathFilterResult;

    LuceneIndexEditor(LuceneIndexEditorContext context) throws CommitFailedException {
        this.parent = null;
        this.name = null;
        this.path = "/";
        this.context = context;
        this.isDeleted = false;
        this.matcherState = MatcherState.NONE;
        this.pathFilterResult = context.getDefinition().getPathFilter().filter(PathUtils.ROOT_PATH);
    }

    private LuceneIndexEditor(LuceneIndexEditor parent, String name,
                              MatcherState matcherState,
                              PathFilter.Result pathFilterResult,
            boolean isDeleted) {
        this.parent = parent;
        this.name = name;
        this.path = null;
        this.context = parent.context;
        this.isDeleted = isDeleted;
        this.matcherState = matcherState;
        this.pathFilterResult = pathFilterResult;
    }

    public String getPath() {
        //TODO Use the tree instance to determine path
        if (path == null) { // => parent != null
            path = concat(parent.getPath(), name);
        }
        return path;
    }

    @Override
    public void enter(NodeState before, NodeState after)
            throws CommitFailedException {
        if (EmptyNodeState.MISSING_NODE == before && parent == null){
            context.enableReindexMode();
        }

        if (parent == null){
            afterTree = TreeFactory.createReadOnlyTree(after);
            beforeTree = TreeFactory.createReadOnlyTree(before);
        } else {
            afterTree = parent.afterTree.getChild(name);
            beforeTree = parent.beforeTree.getChild(name);
        }

        //Only check for indexing if the result is include.
        //In case like TRAVERSE nothing needs to be indexed for those
        //path
        if (pathFilterResult == PathFilter.Result.INCLUDE) {
            //For traversal in deleted sub tree before state has to be used
            Tree current = afterTree.exists() ? afterTree : beforeTree;
            indexingRule = getDefinition().getApplicableIndexingRule(current);

            if (indexingRule != null) {
                currentMatchers = indexingRule.getAggregate().createMatchers(this);
            }
        }
    }

    @Override
    public void leave(NodeState before, NodeState after)
            throws CommitFailedException {
        if (propertiesChanged || !before.exists()) {
            String path = getPath();
            if (addOrUpdate(path, after, before.exists())) {
                long indexed = context.incIndexedNodes();
                if (indexed % 1000 == 0) {
                    log.debug("[{}] => Indexed {} nodes...", getIndexName(), indexed);
                }
            }
        }

        for (Matcher m : matcherState.affectedMatchers){
            m.markRootDirty();
        }

        if (parent == null) {
            try {
                context.closeWriter();
            } catch (IOException e) {
                throw new CommitFailedException("Lucene", 4,
                        "Failed to close the Lucene index", e);
            }
            if (context.getIndexedNodes() > 0) {
                log.debug("[{}] => Indexed {} nodes, done.", getIndexName(), context.getIndexedNodes());
            }
        }
    }

    @Override
    public void propertyAdded(PropertyState after) {
        markPropertyChanged(after.getName());
        checkAggregates(after.getName());
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) {
        markPropertyChanged(before.getName());
        propertiesModified.add(before);
        checkAggregates(before.getName());
    }

    @Override
    public void propertyDeleted(PropertyState before) {
        markPropertyChanged(before.getName());
        propertiesModified.add(before);
        checkAggregates(before.getName());
    }

    @Override
    public Editor childNodeAdded(String name, NodeState after) {
        PathFilter.Result filterResult = getPathFilterResult(name);
        if (filterResult != PathFilter.Result.EXCLUDE) {
            return new LuceneIndexEditor(this, name, getMatcherState(name, after), filterResult, false);
        }
        return null;
    }

    @Override
    public Editor childNodeChanged(
            String name, NodeState before, NodeState after) {
        PathFilter.Result filterResult = getPathFilterResult(name);
        if (filterResult != PathFilter.Result.EXCLUDE) {
            return new LuceneIndexEditor(this, name, getMatcherState(name, after), filterResult, false);
        }
        return null;
    }

    @Override
    public Editor childNodeDeleted(String name, NodeState before)
            throws CommitFailedException {
        PathFilter.Result filterResult = getPathFilterResult(name);
        if (filterResult == PathFilter.Result.EXCLUDE) {
            return null;
        }

        if (!isDeleted) {
            // tree deletion is handled on the parent node
            String path = concat(getPath(), name);
            try {
                LuceneIndexWriter writer = context.getWriter();
                // Remove all index entries in the removed subtree
                writer.deleteDocuments(path);
                this.context.indexUpdate();
            } catch (IOException e) {
                throw new CommitFailedException("Lucene", 5,
                        "Failed to remove the index entries of"
                                + " the removed subtree " + path, e);
            }
        }

        MatcherState ms = getMatcherState(name, before);
        if (!ms.isEmpty()){
            return new LuceneIndexEditor(this, name, ms, filterResult, true);
        }
        return null; // no need to recurse down the removed subtree
    }

    LuceneIndexEditorContext getContext() {
        return context;
    }

    private boolean addOrUpdate(String path, NodeState state, boolean isUpdate)
            throws CommitFailedException {
        try {
            Document d = makeDocument(path, state, isUpdate);
            if (d != null) {
                if (log.isTraceEnabled()) {
                    log.trace("[{}] Indexed document for {} is {}", getIndexName(), path, d);
                }
                context.indexUpdate();
                context.getWriter().updateDocument(path, d);
                return true;
            }
        } catch (IOException e) {
            throw new CommitFailedException("Lucene", 3,
                    "Failed to index the node " + path, e);
        } catch (IllegalArgumentException ie) {
            log.warn("Failed to index the node [{}]", path, ie);
        }
        return false;
    }

    private Document makeDocument(String path, NodeState state, boolean isUpdate) throws IOException {
        if (!isIndexable()) {
            return null;
        }

        boolean facet = false;

        List<Field> fields = new ArrayList<Field>();
        boolean dirty = false;

        //We 'intentionally' are indexing node names only on root state as we don't support indexing relative or
        //regex for node name indexing
        PropertyState nodenamePS =
                new StringPropertyState(FieldNames.NODE_NAME, getName(path));
        for (PropertyState property : Iterables.concat(state.getProperties(), Collections.singleton(nodenamePS))) {
            String pname = property.getName();

            if (!isVisible(pname) && !FieldNames.NODE_NAME.equals(pname)) {
                continue;
            }

            PropertyDefinition pd = indexingRule.getConfig(pname);

            if (pd == null || !pd.index){
                continue;
            }

            if (pd.ordered) {
                dirty |= addTypedOrderedFields(fields, property, pname, pd);
            }

            dirty |= indexProperty(path, fields, state, property, pname, pd);

            facet |= pd.facet;
        }

        dirty |= indexAggregates(path, fields, state);
        dirty |= indexNullCheckEnabledProps(path, fields, state);
        dirty |= indexNotNullCheckEnabledProps(path, fields, state);

        dirty |= augmentCustomFields(path, fields, state);

        // Check if a node having a single property was modified/deleted
        if (!dirty) {
            dirty = indexIfSinglePropertyRemoved();
        }

        if (isUpdate && !dirty) {
            // updated the state but had no relevant changes
            return null;
        }

        String name = getName(path);
        if (indexingRule.isNodeNameIndexed()){
            addNodeNameField(fields, name);
            dirty = true;
        }

        //For property index no use making an empty document if
        //none of the properties are indexed
        if(!indexingRule.indexesAllNodesOfMatchingType() && !dirty){
            return null;
        }

        Document document = new Document();
        document.add(newPathField(path));


        if (indexingRule.isFulltextEnabled()) {
            document.add(newFulltextField(name));
        }

        if (getDefinition().evaluatePathRestrictions()){
            document.add(newAncestorsField(PathUtils.getParentPath(path)));
            document.add(newDepthField(path));
        }

        // because of LUCENE-5833 we have to merge the suggest fields into a single one
        Field suggestField = null;
        for (Field f : fields) {
            if (FieldNames.SUGGEST.equals(f.name())) {
                if (suggestField == null) {
                    suggestField = FieldFactory.newSuggestField(f.stringValue());
                } else {
                    suggestField = FieldFactory.newSuggestField(suggestField.stringValue(), f.stringValue());
                }
            } else {
                document.add(f);
            }
        }
        if (suggestField != null) {
            document.add(suggestField);
        }

        if (facet) {
            document = context.getFacetsConfig().build(document);
        }

        //TODO Boost at document level

        return document;
    }

    private boolean addFacetFields(List<Field> fields, PropertyState property, String pname, PropertyDefinition pd) {
        String facetFieldName = FieldNames.createFacetFieldName(pname);
        context.getFacetsConfig().setIndexFieldName(pname, facetFieldName);
        int tag = property.getType().tag();
        int idxDefinedTag = pd.getType();
        // Try converting type to the defined type in the index definition
        if (tag != idxDefinedTag) {
            log.debug("[{}] Facet property defined with type {} differs from property {} with type {} in "
                            + "path {}",
                    getIndexName(),
                    Type.fromTag(idxDefinedTag, false), property.toString(),
                    Type.fromTag(tag, false), getPath());
            tag = idxDefinedTag;
        }

        boolean fieldAdded = false;
        try {
            if (tag == Type.STRINGS.tag() && property.isArray()) {
                context.getFacetsConfig().setMultiValued(pname, true);
                Iterable<String> values = property.getValue(Type.STRINGS);
                for (String value : values) {
                    if (value != null && value.length() > 0) {
                        fields.add(new SortedSetDocValuesFacetField(pname, value));
                    }
                }
                fieldAdded = true;
            } else if (tag == Type.STRING.tag()) {
                String value = property.getValue(Type.STRING);
                if (value.length() > 0) {
                    fields.add(new SortedSetDocValuesFacetField(pname, value));
                    fieldAdded = true;
                }
            }

        } catch (Throwable e) {
            log.warn("[{}] Ignoring facet property. Could not convert property {} of type {} to type {} for path {}",
                    getIndexName(), pname,
                    Type.fromTag(property.getType().tag(), false),
                    Type.fromTag(tag, false), getPath(), e);
        }
        return fieldAdded;
    }

    private boolean indexProperty(String path,
                                  List<Field> fields,
                                  NodeState state,
                                  PropertyState property,
                                  String pname,
                                  PropertyDefinition pd) {
        boolean includeTypeForFullText = indexingRule.includePropertyType(property.getType().tag());

        boolean dirty = false;
        if (Type.BINARY.tag() == property.getType().tag()
                && includeTypeForFullText) {
            fields.addAll(newBinary(property, state, null, path + "@" + pname));
            dirty = true;
        } else {
            if (pd.propertyIndex && pd.includePropertyType(property.getType().tag())) {
                dirty |= addTypedFields(fields, property, pname);
            }

            if (pd.fulltextEnabled() && includeTypeForFullText) {
                for (String value : property.getValue(Type.STRINGS)) {
                    if (pd.analyzed && pd.includePropertyType(property.getType().tag())) {
                        String analyzedPropName = constructAnalyzedPropertyName(pname);
                        fields.add(newPropertyField(analyzedPropName, value, !pd.skipTokenization(pname), pd.stored));
                    }

                    if (pd.useInSuggest) {
                        fields.add(FieldFactory.newSuggestField(value));
                    }

                    if (pd.useInSpellcheck) {
                        fields.add(newPropertyField(FieldNames.SPELLCHECK, value, true, false));
                    }

                    if (pd.nodeScopeIndex) {
                        Field field = newFulltextField(value);
                        fields.add(field);
                    }
                    dirty = true;
                }
            }
            if (pd.facet) {
                dirty |= addFacetFields(fields, property, pname, pd);
            }

        }

        return dirty;
    }

    private String constructAnalyzedPropertyName(String pname) {
        if (context.getDefinition().getVersion().isAtLeast(IndexFormatVersion.V2)){
            return FieldNames.createAnalyzedFieldName(pname);
        }
        return pname;
    }

    private boolean addTypedFields(List<Field> fields, PropertyState property, String pname) {
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

            fields.add(f);
            fieldAdded = true;
        }
        return fieldAdded;
    }

    private boolean addTypedOrderedFields(List<Field> fields,
                                          PropertyState property,
                                          String pname,
                                          PropertyDefinition pd) {
        // Ignore and warn if property multi-valued as not supported
        if (property.getType().isArray()) {
            log.warn(
                    "[{}] Ignoring ordered property {} of type {} for path {} as multivalued ordered property not supported",
                    getIndexName(), pname,
                    Type.fromTag(property.getType().tag(), true), getPath());
            return false;
        }

        int tag = property.getType().tag();
        int idxDefinedTag = pd.getType();
        // Try converting type to the defined type in the index definition
        if (tag != idxDefinedTag) {
            log.debug(
                    "[{}] Ordered property defined with type {} differs from property {} with type {} in "
                            + "path {}",
                    getIndexName(),
                    Type.fromTag(idxDefinedTag, false), property.toString(),
                    Type.fromTag(tag, false), getPath());
            tag = idxDefinedTag;
        }

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

            if (f != null) {
                fields.add(f);
                fieldAdded = true;
            }
        } catch (Exception e) {
            log.warn(
                    "[{}] Ignoring ordered property. Could not convert property {} of type {} to type {} for path {}",
                    getIndexName(), pname,
                    Type.fromTag(property.getType().tag(), false),
                    Type.fromTag(tag, false), getPath(), e);
        }
        return fieldAdded;
    }

    private static boolean isVisible(String name) {
        return name.charAt(0) != ':';
    }

    private List<Field> newBinary(
            PropertyState property, NodeState state, String nodePath, String path) {
        List<Field> fields = new ArrayList<Field>();
        Metadata metadata = new Metadata();

        //jcr:mimeType is mandatory for a binary to be indexed
        String type = state.getString(JcrConstants.JCR_MIMETYPE);

        if (type == null || !isSupportedMediaType(type)) {
            log.trace(
                    "[{}] Ignoring binary content for node {} due to unsupported (or null) jcr:mimeType [{}]",
                    getIndexName(), path, type);
            return fields;
        }

        metadata.set(Metadata.CONTENT_TYPE, type);
        if (JCR_DATA.equals(property.getName())) {
            String encoding = state.getString(JcrConstants.JCR_ENCODING);
            if (encoding != null) { // not mandatory
                metadata.set(Metadata.CONTENT_ENCODING, encoding);
            }
        }

        for (Blob v : property.getValue(Type.BINARIES)) {
            String value = parseStringValue(v, metadata, path, property.getName());
            if (value == null){
                continue;
            }

            if (nodePath != null){
                fields.add(newFulltextField(nodePath, value, true));
            } else {
                fields.add(newFulltextField(value, true));
            }
        }
        return fields;
    }

    private boolean augmentCustomFields(final String path, final List<Field> fields,
                                        final NodeState document) {
        boolean dirty = false;

        IndexAugmentorFactory augmentorFactory = context.getAugmentorFactory();
        if (augmentorFactory != null) {
            IndexDefinition defn = getDefinition();
            Iterable<Field> augmentedFields = augmentorFactory
                    .getIndexFieldProvider(indexingRule.getNodeTypeName())
                    .getAugmentedFields(path, document, defn.getDefinitionNodeState());

            for (Field field : augmentedFields) {
                fields.add(field);
                dirty = true;
            }
        }

        return dirty;
    }

    //~-------------------------------------------------------< NullCheck Support >

    private boolean indexNotNullCheckEnabledProps(String path, List<Field> fields, NodeState state) {
        boolean fieldAdded = false;
        for (PropertyDefinition pd : indexingRule.getNotNullCheckEnabledProperties()) {
            if (isPropertyNotNull(state, pd)) {
                fields.add(new StringField(FieldNames.NOT_NULL_PROPS, pd.name, Field.Store.NO));
                fieldAdded = true;
            }
        }
        return fieldAdded;
    }

    private boolean indexNullCheckEnabledProps(String path, List<Field> fields, NodeState state) {
        boolean fieldAdded = false;
        for (PropertyDefinition pd : indexingRule.getNullCheckEnabledProperties()) {
            if (isPropertyNull(state, pd)) {
                fields.add(new StringField(FieldNames.NULL_PROPS, pd.name, Field.Store.NO));
                fieldAdded = true;
            }
        }
        return fieldAdded;
    }

    private boolean indexIfSinglePropertyRemoved() {
        boolean dirty = false;
        for (PropertyState ps : propertiesModified) {
            PropertyDefinition pd = indexingRule.getConfig(ps.getName());
            if (pd != null
                    && pd.index
                    && (pd.includePropertyType(ps.getType().tag())
                            || indexingRule.includePropertyType(ps.getType().tag()))) {
                dirty = true;
                break;
            }
        }
        return dirty;
    }

    /**
     * Determine if the property as defined by PropertyDefinition exists or not.
     *
     * <p>For relative property if the intermediate nodes do not exist then property is
     * <bold>not</bold> considered to be null</p>
     *
     * @return true if the property does not exist
     */
    private boolean isPropertyNull(NodeState state, PropertyDefinition pd){
        NodeState propertyNode = getPropertyNode(state, pd);
        if (!propertyNode.exists()){
            return false;
        }
        return !propertyNode.hasProperty(pd.nonRelativeName);
    }

    /**
     * Determine if the property as defined by PropertyDefinition exists or not.
     *
     * <p>For relative property if the intermediate nodes do not exist then property is
     * considered to be null</p>
     *
     * @return true if the property exists
     */
    private boolean isPropertyNotNull(NodeState state, PropertyDefinition pd){
        NodeState propertyNode = getPropertyNode(state, pd);
        if (!propertyNode.exists()){
            return false;
        }
        return propertyNode.hasProperty(pd.nonRelativeName);
    }

    private static NodeState getPropertyNode(NodeState nodeState, PropertyDefinition pd) {
        if (!pd.relative){
            return nodeState;
        }
        NodeState node = nodeState;
        for (String name : pd.ancestors) {
            node = node.getChildNode(name);
        }
        return node;
    }

    //~-------------------------------------------------------< Aggregate >

    @Override
    public void markDirty() {
        propertiesChanged = true;
    }

    private MatcherState getMatcherState(String name, NodeState after) {
        List<Matcher> matched = Lists.newArrayList();
        List<Matcher> inherited = Lists.newArrayList();
        for (Matcher m : Iterables.concat(matcherState.inherited, currentMatchers)) {
            Matcher result = m.match(name, after);
            if (result.getStatus() == Matcher.Status.MATCH_FOUND){
                matched.add(result);
            }

            if (result.getStatus() != Matcher.Status.FAIL){
                inherited.addAll(result.nextSet());
            }
        }

        if (!matched.isEmpty() || !inherited.isEmpty()) {
            return new MatcherState(matched, inherited);
        }
        return MatcherState.NONE;
    }

    private boolean indexAggregates(final String path, final List<Field> fields,
                                    final NodeState state) {
        final AtomicBoolean dirtyFlag = new AtomicBoolean();
        indexingRule.getAggregate().collectAggregates(state, new Aggregate.ResultCollector() {
            @Override
            public void onResult(Aggregate.NodeIncludeResult result) {
                boolean dirty = indexAggregatedNode(path, fields, result);
                if (dirty) {
                    dirtyFlag.set(true);
                }
            }

            @Override
            public void onResult(Aggregate.PropertyIncludeResult result) {
                boolean dirty = false;
                if (result.pd.ordered) {
                    dirty |= addTypedOrderedFields(fields, result.propertyState,
                            result.propertyPath, result.pd);
                }
                dirty |= indexProperty(path, fields, state, result.propertyState,
                        result.propertyPath, result.pd);

                if (dirty) {
                    dirtyFlag.set(true);
                }
            }
        });
        return dirtyFlag.get();
    }
    /**
     * Create the fulltext field from the aggregated nodes. If result is for aggregate for a relative node
     * include then
     * @param path current node path
     * @param fields indexed fields
     * @param result aggregate result
     * @return true if a field was created for passed node result
     */
    private boolean indexAggregatedNode(String path, List<Field> fields, Aggregate.NodeIncludeResult result) {
        //rule for node being aggregated might be null if such nodes
        //are not indexed on there own. In such cases we rely in current
        //rule for some checks
        IndexDefinition.IndexingRule ruleAggNode = context.getDefinition()
                .getApplicableIndexingRule(getPrimaryTypeName(result.nodeState));
        boolean dirty = false;

        for (PropertyState property : result.nodeState.getProperties()){
            String pname = property.getName();
            String propertyPath = PathUtils.concat(result.nodePath, pname);

            if (!isVisible(pname)) {
                continue;
            }

            //Check if type is indexed
            int type = property.getType().tag();
            if (ruleAggNode != null ) {
                if (!ruleAggNode.includePropertyType(type)) {
                    continue;
                }
            } else if (!indexingRule.includePropertyType(type)){
                continue;
            }

            //Check if any explicit property defn is defined via relative path
            // and is marked to exclude this property from being indexed. We exclude
            //it from aggregation if
            // 1. Its not to be indexed i.e. index=false
            // 2. Its explicitly excluded from aggregation i.e. excludeFromAggregation=true
            PropertyDefinition pdForRootNode = indexingRule.getConfig(propertyPath);
            if (pdForRootNode != null && (!pdForRootNode.index || pdForRootNode.excludeFromAggregate)) {
                continue;
            }

            if (Type.BINARY == property.getType()) {
                String aggreagtedNodePath = PathUtils.concat(path, result.nodePath);
                //Here the fulltext is being created for aggregate root hence nodePath passed
                //should be null
                String nodePath = result.isRelativeNode() ? result.rootIncludePath : null;
                fields.addAll(newBinary(property, result.nodeState, nodePath, aggreagtedNodePath + "@" + pname));
                dirty = true;
            } else {
                PropertyDefinition pd = null;
                if (ruleAggNode != null){
                    pd = ruleAggNode.getConfig(pname);
                }

                if (pd != null && !pd.nodeScopeIndex){
                    continue;
                }

                for (String value : property.getValue(Type.STRINGS)) {
                    Field field = result.isRelativeNode() ?
                            newFulltextField(result.rootIncludePath, value) : newFulltextField(value) ;
                    if (pd != null) {
                        field.setBoost(pd.boost);
                    }
                    fields.add(field);
                    dirty = true;
                }
            }
        }
        return dirty;
    }

    /**
     * Determines which all matchers are affected by this property change
     *
     * @param name modified property name
     */
    private void checkAggregates(String name) {
        for (Matcher m : matcherState.matched) {
            if (!matcherState.affectedMatchers.contains(m)
                    && m.aggregatesProperty(name)) {
                matcherState.affectedMatchers.add(m);
            }
        }
    }

    private static class MatcherState {
        final static MatcherState NONE = new MatcherState(Collections.<Matcher>emptyList(),
                Collections.<Matcher>emptyList());

        final List<Matcher> matched;
        final List<Matcher> inherited;
        final Set<Matcher> affectedMatchers;

        public MatcherState(List<Matcher> matched,
                            List<Matcher> inherited){
            this.matched = matched;
            this.inherited = inherited;

            //Affected matches would only be used when there are
            //some matched matchers
            if (matched.isEmpty()){
                affectedMatchers = Collections.emptySet();
            } else {
                affectedMatchers = Sets.newIdentityHashSet();
            }
        }

        public boolean isEmpty() {
            return matched.isEmpty() && inherited.isEmpty();
        }
    }

    private void markPropertyChanged(String name) {
        if (isIndexable()
                && !propertiesChanged
                && indexingRule.isIndexed(name)) {
            propertiesChanged = true;
        }
    }

    private IndexDefinition getDefinition() {
        return context.getDefinition();
    }

    private boolean isIndexable(){
        return indexingRule != null;
    }

    private PathFilter.Result getPathFilterResult(String childNodeName) {
        return context.getDefinition().getPathFilter().filter(concat(getPath(), childNodeName));
    }

    private boolean isSupportedMediaType(String type) {
        return context.isSupportedMediaType(type);
    }

    private String parseStringValue(Blob v, Metadata metadata, String path, String propertyName) {
        if (!context.isAsyncIndexing()){
            //Skip text extraction for sync indexing
            return null;
        }
        String text = context.getExtractedTextCache().get(path, propertyName, v, context.isReindex());
        if (text == null){
            text = parseStringValue0(v, metadata, path);
        }
        return text;
    }

    private String parseStringValue0(Blob v, Metadata metadata, String path) {
        WriteOutContentHandler handler = new WriteOutContentHandler(context.getDefinition().getMaxExtractLength());
        long start = System.currentTimeMillis();
        long bytesRead = 0;
        long length = v.length();
        if (log.isDebugEnabled()) {
            log.debug("Extracting {}, {} bytes, id {}", path, length, v.getContentIdentity());
        }
        String oldThreadName = null;
        if (length > SMALL_BINARY) {
            Thread t = Thread.currentThread();
            oldThreadName = t.getName();
            t.setName(oldThreadName + ": Extracting " + path + ", " + length + " bytes");
        }
        try {
            CountingInputStream stream = new CountingInputStream(new LazyInputStream(new BlobByteSource(v)));
            try {
                context.getParser().parse(stream, handler, metadata, new ParseContext());
            } finally {
                bytesRead = stream.getCount();
                stream.close();
            }
        } catch (LinkageError e) {
            // Capture and ignore errors caused by extraction libraries
            // not being present. This is equivalent to disabling
            // selected media types in configuration, so we can simply
            // ignore these errors.
        } catch (Throwable t) {
            // Capture and report any other full text extraction problems.
            // The special STOP exception is used for normal termination.
            if (!handler.isWriteLimitReached(t)) {
                log.debug(
                        "[{}] Failed to extract text from a binary property: {}."
                        + " This is a fairly common case, and nothing to"
                        + " worry about. The stack trace is included to"
                        + " help improve the text extraction feature.",
                        getIndexName(), path, t);
                context.getExtractedTextCache().put(v, ExtractedText.ERROR);
                return TEXT_EXTRACTION_ERROR;
            }
        } finally {
            if (oldThreadName != null) {
                Thread.currentThread().setName(oldThreadName);
            }
        }
        String result = handler.toString();
        if (bytesRead > 0) {
            long time = System.currentTimeMillis() - start;
            int len = result.length();
            context.recordTextExtractionStats(time, bytesRead, len);
            if (log.isDebugEnabled()) {
                log.debug("Extracting {} took {} ms, {} bytes read, {} text size", 
                        path, time, bytesRead, len);
            }
        }
        context.getExtractedTextCache().put(v,  new ExtractedText(ExtractionResult.SUCCESS, result));
        return result;
    }

    private String getIndexName() {
        return context.getDefinition().getIndexName();
    }


    /**
     * Extracts the local name of the current node ignoring any namespace prefix
     *
     * @param name node name
     */
    private static void addNodeNameField(List<Field> fields, String name) {
        //TODO Need to check if it covers all cases
        int colon = name.indexOf(':');
        String value = colon < 0 ? name : name.substring(colon + 1);

        //For now just add a single term. Later we can look into using different analyzer
        //to analyze the node name and add multiple terms. Like add multiple terms for a
        //cameCase file name to allow faster like search
        fields.add(new StringField(FieldNames.NODE_NAME, value, Field.Store.NO));
    }

}
