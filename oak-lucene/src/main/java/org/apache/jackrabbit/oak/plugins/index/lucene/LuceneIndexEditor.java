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
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newFulltextField;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newPathField;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newPropertyField;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TermFactory.newPathTerm;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexEditor;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.nodetype.TypePredicate;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.analysis.Analyzer;
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
public class LuceneIndexEditor implements IndexEditor {

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

    private NodeState root;

    private final Predicate typePredicate;

    LuceneIndexEditor(NodeState root, NodeBuilder definition, Analyzer analyzer,
        IndexUpdateCallback updateCallback) throws CommitFailedException {
        this.parent = null;
        this.name = null;
        this.path = "/";
        this.context = new LuceneIndexEditorContext(definition, analyzer,
                updateCallback);
        this.root = root;
        if (context.getDefinition().hasDeclaredNodeTypes()) {
            typePredicate = new TypePredicate(root, context.getDefinition().getDeclaringNodeTypes());
        } else {
            typePredicate = Predicates.alwaysTrue();
        }
    }

    private LuceneIndexEditor(LuceneIndexEditor parent, String name) {
        this.parent = parent;
        this.name = name;
        this.path = null;
        this.context = parent.context;
        this.root = parent.root;
        this.typePredicate = parent.typePredicate;
    }

    public String getPath() {
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
        propertiesChanged = true;
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) {
        propertiesChanged = true;
    }

    @Override
    public void propertyDeleted(PropertyState before) {
        propertiesChanged = true;
    }

    @Override
    public Editor childNodeAdded(String name, NodeState after) {
        return new LuceneIndexEditor(this, name);
    }

    @Override
    public Editor childNodeChanged(
            String name, NodeState before, NodeState after) {
        return new LuceneIndexEditor(this, name);
    }

    @Override
    public Editor childNodeDeleted(String name, NodeState before)
            throws CommitFailedException {
        String path = concat(getPath(), name);

        try {
            IndexWriter writer = context.getWriter();
            // Remove all index entries in the removed subtree
            writer.deleteDocuments(newPathTerm(path));
            writer.deleteDocuments(new PrefixQuery(newPathTerm(path + "/")));
            this.context.indexUpdate();
        } catch (IOException e) {
            throw new CommitFailedException(
                    "Lucene", 5, "Failed to remove the index entries of"
                    + " the removed subtree " + path, e);
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
        }
        return false;
    }

    private Document makeDocument(String path, NodeState state, boolean isUpdate) throws CommitFailedException {
        //TODO Possibly we can add support for compound properties like foo/bar
        //i.e. support for relative path restrictions

        // Check for declaringNodeType validity
        if (!typePredicate.apply(state)) {
            return null;
        }

        List<Field> fields = new ArrayList<Field>();
        boolean dirty = false;
        for (PropertyState property : state.getProperties()) {
            String pname = property.getName();

            if (!isVisible(pname)) {
                continue;
            }

            if (context.getDefinition().isOrdered(pname)) {
                dirty = addTypedOrderedFields(fields, property);
            }

            if (context.includeProperty(pname)) {
                //In case of fulltext we also check if given type is enabled for indexing
                //TODO Use context.includePropertyType however that cause issue. Need
                //to make filtering based on type consistent both on indexing side and
                //query size
                if(context.isFullTextEnabled()
                        && (context.getPropertyTypes() & (1 << property.getType()
                        .tag())) == 0){
                    continue;
                }

                if (Type.BINARY.tag() == property.getType().tag()) {
                    this.context.indexUpdate();
                    fields.addAll(newBinary(property, state, path + "@" + pname));
                    dirty = true;
                } else if(!context.isFullTextEnabled()
                        && FieldFactory.canCreateTypedField(property.getType())){
                    dirty = addTypedFields(fields, property);
                } else {
                    for (String value : property.getValue(Type.STRINGS)) {
                        this.context.indexUpdate();
                        fields.add(newPropertyField(pname, value,
                                !context.skipTokenization(pname),
                                context.isStored(pname)));
                        if (context.isFullTextEnabled()) {
                            fields.add(newFulltextField(value));
                        }
                        dirty = true;
                    }
                }
            }
        }

        if (isUpdate && !dirty) {
            // updated the state but had no relevant changes
            return null;
        }

        //For property index no use making an empty document if
        //none of the properties are indexed
        if(!context.isFullTextEnabled() && !dirty){
            return null;
        }

        Document document = new Document();
        document.add(newPathField(path));
        String name = getName(path);

        //TODO Possibly index nodeName without tokenization for node name based queries
        if (context.isFullTextEnabled()) {
            document.add(newFulltextField(name));
        }
        for (Field f : fields) {
            document.add(f);
        }
        return document;
    }

    private boolean addTypedFields(List<Field> fields, PropertyState property) throws CommitFailedException {
        int tag = property.getType().tag();
        String name = property.getName();
        boolean fieldAdded = false;
        for (int i = 0; i < property.count(); i++) {
            Field f = null;
            if (tag == Type.LONG.tag()) {
                f = new LongField(name, property.getValue(Type.LONG, i), Field.Store.NO);
            } else if (tag == Type.DATE.tag()) {
                String date = property.getValue(Type.DATE, i);
                f = new LongField(name, FieldFactory.dateToLong(date), Field.Store.NO);
            } else if (tag == Type.DOUBLE.tag()) {
                f = new DoubleField(name, property.getValue(Type.DOUBLE, i), Field.Store.NO);
            } else if (tag == Type.BOOLEAN.tag()) {
                f = new StringField(name, property.getValue(Type.BOOLEAN, i).toString(), Field.Store.NO);
            }

            if (f != null) {
                this.context.indexUpdate();
                fields.add(f);
                fieldAdded = true;
            }
        }
        return fieldAdded;
    }

    private boolean addTypedOrderedFields(List<Field> fields, PropertyState property) throws CommitFailedException {
        int tag = property.getType().tag();
        String name = FieldNames.createDocValFieldName(property.getName());
        boolean fieldAdded = false;
        for (int i = 0; i < property.count(); i++) {
            Field f = null;
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
        }
        return fieldAdded;
    }

    private static boolean isVisible(String name) {
        return name.charAt(0) != ':';
    }

    private List<Field> newBinary(
            PropertyState property, NodeState state, String path) {
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
            fields.add(newFulltextField(parseStringValue(v, metadata, path)));
        }
        return fields;
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
