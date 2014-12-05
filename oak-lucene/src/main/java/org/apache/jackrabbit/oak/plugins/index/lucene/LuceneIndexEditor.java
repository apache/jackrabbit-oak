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

import static org.apache.jackrabbit.JcrConstants.JCR_DATA;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newDepthField;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newFulltextField;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newAncestorsField;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newPathField;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newPropertyField;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TermFactory.newPathTerm;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.ConfigUtil.getPrimaryTypeName;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexEditor;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.lucene.Aggregate.Matcher;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.tree.ImmutableTree;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.WriteOutContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link IndexEditor} implementation that is responsible for keeping the
 * {@link LuceneIndex} up to date
 * 
 * @see LuceneIndex
 */
public class LuceneIndexEditor implements IndexEditor, Aggregate.AggregateRoot {

    private static final Logger log =
            LoggerFactory.getLogger(LuceneIndexEditor.class);

    private final LuceneIndexEditorContext context;

    /** Name of this node, or {@code null} for the root node. */
    private final String name;

    /** Parent editor or {@code null} if this is the root editor. */
    private final LuceneIndexEditor parent;

    /** Path of this editor, built lazily in {@link #getPath()}. */
    private String path;

    private boolean propertiesChanged = false;

    private final NodeState root;

    /**
     * Flag indicating if the current tree being traversed has a deleted parent.
     */
    private final boolean isDeleted;

    private ImmutableTree current;

    private IndexDefinition.IndexingRule indexingRule;

    private List<Matcher> currentMatchers = Collections.emptyList();

    private final MatcherState matcherState;

    LuceneIndexEditor(NodeState root, NodeBuilder definition,
        IndexUpdateCallback updateCallback) throws CommitFailedException {
        this.parent = null;
        this.name = null;
        this.path = "/";
        this.context = new LuceneIndexEditorContext(root, definition,
                updateCallback);
        this.root = root;
        this.isDeleted = false;
        this.matcherState = MatcherState.NONE;
    }

    private LuceneIndexEditor(LuceneIndexEditor parent, String name,
                              MatcherState matcherState,
            boolean isDeleted) {
        this.parent = parent;
        this.name = name;
        this.path = null;
        this.context = parent.context;
        this.root = parent.root;
        this.isDeleted = isDeleted;
        this.matcherState = matcherState;
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

        //For traversal in deleted sub tree before state has to be used
        NodeState currentState = after.exists() ? after : before;
        if (parent == null){
            current = new ImmutableTree(currentState);
        } else {
            current = new ImmutableTree(parent.current, name, currentState);
        }

        indexingRule = getDefinition().getApplicableIndexingRule(current);

        if (indexingRule != null) {
            currentMatchers = indexingRule.getAggregate().createMatchers(this);
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
                    log.debug("Indexed {} nodes...", indexed);
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
                log.debug("Indexed {} nodes, done.", context.getIndexedNodes());
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
        checkAggregates(before.getName());
    }

    @Override
    public void propertyDeleted(PropertyState before) {
        markPropertyChanged(before.getName());
        checkAggregates(before.getName());
    }

    @Override
    public Editor childNodeAdded(String name, NodeState after) {
        return new LuceneIndexEditor(this, name, getMatcherState(name, after), false);
    }

    @Override
    public Editor childNodeChanged(
            String name, NodeState before, NodeState after) {
        return new LuceneIndexEditor(this, name, getMatcherState(name, after), false);
    }

    @Override
    public Editor childNodeDeleted(String name, NodeState before)
            throws CommitFailedException {

        if (!isDeleted) {
            // tree deletion is handled on the parent node
            String path = concat(getPath(), name);
            try {
                IndexWriter writer = context.getWriter();
                // Remove all index entries in the removed subtree
                writer.deleteDocuments(newPathTerm(path));
                writer.deleteDocuments(new PrefixQuery(newPathTerm(path + "/")));
                this.context.indexUpdate();
            } catch (IOException e) {
                throw new CommitFailedException("Lucene", 5,
                        "Failed to remove the index entries of"
                                + " the removed subtree " + path, e);
            }
        }

        MatcherState ms = getMatcherState(name, before);
        if (!ms.isEmpty()){
            return new LuceneIndexEditor(this, name, ms, true);
        }
        return null; // no need to recurse down the removed subtree
    }

    private boolean addOrUpdate(String path, NodeState state, boolean isUpdate)
            throws CommitFailedException {
        try {
            Document d = makeDocument(path, state, isUpdate);
            if (d != null) {
                context.getWriter().updateDocument(newPathTerm(path), d);
                return true;
            }
        } catch (IOException e) {
            throw new CommitFailedException("Lucene", 3,
                    "Failed to index the node " + path, e);
        } catch (IllegalArgumentException ie) {
            throw new CommitFailedException("Lucene", 3,
                "Failed to index the node " + path, ie);
        }
        return false;
    }

    private Document makeDocument(String path, NodeState state, boolean isUpdate) throws CommitFailedException {
        if (!isIndexable()) {
            return null;
        }

        List<Field> fields = new ArrayList<Field>();
        boolean dirty = false;
        for (PropertyState property : state.getProperties()) {
            String pname = property.getName();

            if (!isVisible(pname)) {
                continue;
            }

            PropertyDefinition pd = indexingRule.getConfig(pname);

            if (pd == null || !pd.index){
                continue;
            }

            if (pd.ordered) {
                dirty |= addTypedOrderedFields(fields, property, pname, pd);
            }

            dirty |= indexProperty(path, fields, state, property, pname, false, pd);
        }

        dirty |= indexAggregates(path, fields, state);

        if (isUpdate && !dirty) {
            // updated the state but had no relevant changes
            return null;
        }

        //For property index no use making an empty document if
        //none of the properties are indexed
        if(!indexingRule.isFulltextEnabled() && !dirty){
            return null;
        }

        Document document = new Document();
        document.add(newPathField(path));
        String name = getName(path);

        //TODO Possibly index nodeName without tokenization for node name based queries
        if (indexingRule.isFulltextEnabled()) {
            document.add(newFulltextField(name));
        }

        if (getDefinition().evaluatePathRestrictions()){
            document.add(newAncestorsField(PathUtils.getParentPath(path)));
            document.add(newDepthField(path));
        }

        for (Field f : fields) {
            document.add(f);
        }

        //TODO Boost at document level

        return document;
    }

    private boolean indexProperty(String path,
                                  List<Field> fields,
                                  NodeState state,
                                  PropertyState property,
                                  String pname,
                                  boolean aggregateMode,
                                  PropertyDefinition pd) throws CommitFailedException {
        boolean includeTypeForFullText = indexingRule.includePropertyType(property.getType().tag());
        if (Type.BINARY.tag() == property.getType().tag()
                && includeTypeForFullText) {
            this.context.indexUpdate();
            fields.addAll(newBinary(property, state, null, path + "@" + pname));
            return true;
        }  else {
            boolean dirty = false;

            if (pd.propertyIndex && pd.includePropertyType(property.getType().tag())){
                dirty |= addTypedFields(fields, property, pname);
            }

            if (pd.fulltextEnabled() && includeTypeForFullText) {
                for (String value : property.getValue(Type.STRINGS)) {
                    this.context.indexUpdate();
                    if (pd.analyzed && pd.includePropertyType(property.getType().tag())) {
                        String analyzedPropName = constructAnalyzedPropertyName(pname);
                        fields.add(newPropertyField(analyzedPropName, value, !pd.skipTokenization(pname), pd.stored));
                    }

                    if (pd.nodeScopeIndex && !aggregateMode) {
                        Field field = newFulltextField(value);
                        field.setBoost(pd.boost);
                        fields.add(field);
                    }
                    dirty = true;
                }
            }
            return dirty;
        }
    }

    private String constructAnalyzedPropertyName(String pname) {
        if (context.getDefinition().getVersion().isAtLeast(IndexFormatVersion.V2)){
            return FieldNames.createAnalyzedFieldName(pname);
        }
        return pname;
    }

    private boolean addTypedFields(List<Field> fields, PropertyState property, String pname) throws CommitFailedException {
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

            this.context.indexUpdate();
            fields.add(f);
            fieldAdded = true;
        }
        return fieldAdded;
    }

    private boolean addTypedOrderedFields(List<Field> fields,
                                          PropertyState property,
                                          String pname,
                                          PropertyDefinition pd) throws CommitFailedException {
        int tag = property.getType().tag();
        int idxDefinedTag = pd.getType();
        // Try converting type to the defined type in the index definition
        if (tag != idxDefinedTag) {
            log.debug(
                "Ordered property defined with type {} differs from property {} with type {} in " +
                    "path {}",
                Type.fromTag(idxDefinedTag, false), property.toString(), Type.fromTag(tag, false),
                getPath());
            tag = idxDefinedTag;
        }

        String name = FieldNames.createDocValFieldName(pname);
        boolean fieldAdded = false;
        for (int i = 0; i < property.count(); i++) {
            Field f = null;
            try {
                if (tag == Type.LONG.tag()) {
                    //TODO Distinguish fields which need to be used for search and for sort
                    //If a field is only used for Sort then it can be stored with less precision
                    f = new NumericDocValuesField(name, property.getValue(Type.LONG, i));
                } else if (tag == Type.DATE.tag()) {
                    String date = property.getValue(Type.DATE, i);
                    f = new NumericDocValuesField(name, FieldFactory.dateToLong(date));
                } else if (tag == Type.DOUBLE.tag()) {
                    f = new DoubleDocValuesField(name, property.getValue(Type.DOUBLE, i));
                } else if (tag == Type.BOOLEAN.tag()) {
                    f = new SortedDocValuesField(name,
                        new BytesRef(property.getValue(Type.BOOLEAN, i).toString()));
                } else if (tag == Type.STRING.tag()) {
                    f = new SortedDocValuesField(name,
                        new BytesRef(property.getValue(Type.STRING, i)));
                }

                if (f != null) {
                    this.context.indexUpdate();
                    fields.add(f);
                    fieldAdded = true;
                }
            } catch (Exception e) {
                log.warn(
                    "Ignoring ordered property. Could not convert property {} of type {} to type " +
                        "{} for path {}",
                    pname, Type.fromTag(property.getType().tag(), false),
                    Type.fromTag(tag, false), getPath(), e);
            }
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
        if (JCR_DATA.equals(property.getName())) {
            String type = state.getString(JcrConstants.JCR_MIMETYPE);
            if (type != null) { // not mandatory
                metadata.set(Metadata.CONTENT_TYPE, type);
            }
            String encoding = state.getString(JcrConstants.JCR_ENCODING);
            if (encoding != null) { // not mandatory
                metadata.set(Metadata.CONTENT_ENCODING, encoding);
            }
        }

        for (Blob v : property.getValue(Type.BINARIES)) {
            if (nodePath != null){
                fields.add(newFulltextField(nodePath, parseStringValue(v, metadata, path)));
            } else {
                fields.add(newFulltextField(parseStringValue(v, metadata, path)));
            }

        }
        return fields;
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
                                    final NodeState state) throws CommitFailedException {
        final AtomicBoolean dirtyFlag = new AtomicBoolean();
        indexingRule.getAggregate().collectAggregates(state, new Aggregate.ResultCollector() {
            @Override
            public void onResult(Aggregate.NodeIncludeResult result) throws CommitFailedException {
                boolean dirty = indexAggregatedNode(path, fields, result);
                if (dirty) {
                    dirtyFlag.set(true);
                }
            }

            @Override
            public void onResult(Aggregate.PropertyIncludeResult result) throws CommitFailedException {
                boolean dirty = false;
                if (result.pd.ordered) {
                    dirty |= addTypedOrderedFields(fields, result.propertyState,
                            result.propertyPath, result.pd);
                }
                dirty |= indexProperty(path, fields, state, result.propertyState,
                        result.propertyPath, true, result.pd);

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
     * @throws CommitFailedException
     */
    private boolean indexAggregatedNode(String path, List<Field> fields, Aggregate.NodeIncludeResult result)
            throws CommitFailedException {
        //rule for node being aggregated might be null if such nodes
        //are not indexed on there own. In such cases we rely in current
        //rule for some checks
        IndexDefinition.IndexingRule ruleAggNode = context.getDefinition()
                .getApplicableIndexingRule(getPrimaryTypeName(result.nodeState));
        boolean dirty = false;

        for (PropertyState property : result.nodeState.getProperties()){
            String pname = property.getName();

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

            if (Type.BINARY == property.getType()) {
                String aggreagtedNodePath = PathUtils.concat(path, result.nodePath);
                this.context.indexUpdate();
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
                    this.context.indexUpdate();
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

    private String parseStringValue(Blob v, Metadata metadata, String path) {
        WriteOutContentHandler handler = new WriteOutContentHandler();
        try {
            InputStream stream = v.getNewStream();
            try {
                context.getParser().parse(stream, handler, metadata, new ParseContext());
            } finally {
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
                log.debug("Failed to extract text from a binary property: "
                        + path
                        + " This is a fairly common case, and nothing to"
                        + " worry about. The stack trace is included to"
                        + " help improve the text extraction feature.", t);
                return "TextExtractionError";
            }
        }
        return handler.toString();
    }

}
