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
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.getString;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newFulltextField;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newPathField;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newPropertyField;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.ANALYZER;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INCLUDE_PROPERTY_TYPES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INDEX_DATA_CHILD_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PERSISTENCE_PATH;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.VERSION;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TermFactory.newPathTerm;
import static org.apache.lucene.store.NoLockFactory.getNoLockFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicLong;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexEditor;
import org.apache.jackrabbit.oak.plugins.index.lucene.aggregation.AggregatedState;
import org.apache.jackrabbit.oak.plugins.index.lucene.aggregation.NodeAggregator;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
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

    private static final IndexWriterConfig config = getIndexWriterConfig();

    private static final NodeAggregator aggregator = new NodeAggregator();

    private static final Parser parser = new AutoDetectParser();

    private AtomicLong indexedNodes;

    private static IndexWriterConfig getIndexWriterConfig() {
        // FIXME: Hack needed to make Lucene work in an OSGi environment
        Thread thread = Thread.currentThread();
        ClassLoader loader = thread.getContextClassLoader();
        thread.setContextClassLoader(IndexWriterConfig.class.getClassLoader());
        try {
            IndexWriterConfig config = new IndexWriterConfig(VERSION, ANALYZER);
            config.setMergeScheduler(new SerialMergeScheduler());
            return config;
        } finally {
            thread.setContextClassLoader(loader);
        }
    }

    private static Directory newIndexDirectory(NodeBuilder definition)
            throws CommitFailedException {
        String path = getString(definition, PERSISTENCE_PATH);
        if (path == null) {
            return new ReadWriteOakDirectory(
                    definition.child(INDEX_DATA_CHILD_NAME));
        } else {
            try {
                File file = new File(path);
                file.mkdirs();
                // TODO: close() is never called
                // TODO: no locking used
                // --> using the FS backend for the index is in any case
                //     troublesome in clustering scenarios and for backup
                //     etc. so instead of fixing these issues we'd better
                //     work on making the in-content index work without
                //     problems (or look at the Solr indexer as alternative)
                return FSDirectory.open(file, getNoLockFactory());
            } catch (IOException e) {
                throw new CommitFailedException(
                        "Lucene", 1, "Failed to open the index in " + path, e);
            }
        }
    }

    /** Parent editor, or {@code null} if this is the root editor. */
    private final LuceneIndexEditor parent;

    /** Name of this node, or {@code null} for the root node. */
    private final String name;

    /** Path of this editor, built lazily in {@link #getPath()}. */
    private String path;

    /** Index definition node builder */
    private final NodeBuilder definition;

    private final int propertyTypes;

    private IndexWriter writer = null;

    private boolean propertiesChanged = false;

    LuceneIndexEditor(NodeBuilder definition) throws CommitFailedException {
        this.parent = null;
        this.name = null;
        this.path = "/";
        this.definition = definition;

        PropertyState ps = definition.getProperty(INCLUDE_PROPERTY_TYPES);
        if (ps != null) {
            int types = 0;
            for (String inc : ps.getValue(Type.STRINGS)) {
                try {
                    types |= 1 << PropertyType.valueFromName(inc);
                } catch (IllegalArgumentException e) {
                    log.warn("Unknown property type: " + inc);
                }
            }
            this.propertyTypes = types;
        } else {
            this.propertyTypes = -1;
        }
        this.indexedNodes = new AtomicLong(0);
    }

    private LuceneIndexEditor(LuceneIndexEditor parent, String name) {
        this.parent = parent;
        this.name = name;
        this.path = null;
        this.definition = parent.definition;
        this.writer = parent.writer;
        this.propertyTypes = parent.propertyTypes;
        this.indexedNodes = parent.indexedNodes;
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
        if (parent == null) {
            try {
                writer = new IndexWriter(newIndexDirectory(definition), config);
            } catch (IOException e) {
                throw new CommitFailedException(
                        "Lucene", 2, "Unable to create a new index writer", e);
            }
        }
    }

    @Override
    public void leave(NodeState before, NodeState after)
            throws CommitFailedException {
        if (propertiesChanged || !before.exists()) {
            String path = getPath();
            try {
                writer.updateDocument(
                        newPathTerm(path), makeDocument(path, after));
            } catch (IOException e) {
                throw new CommitFailedException(
                        "Lucene", 3, "Failed to index the node " + path, e);
            }
            long indexed = indexedNodes.incrementAndGet();
            if (indexed % 1000 == 0) {
                log.debug("Indexed {} nodes...", indexed);
            }
        }

        if (parent == null) {
            try {
                writer.close();
            } catch (IOException e) {
                throw new CommitFailedException("Lucene", 4,
                        "Failed to close the Lucene index", e);
            }
            long indexed = indexedNodes.get();
            if (indexed > 0) {
                log.debug("Indexed {} nodes, done.", indexed);
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
        String path = PathUtils.concat(getPath(), name);

        try {
            // Remove all index entries in the removed subtree
            writer.deleteDocuments(newPathTerm(path));
            writer.deleteDocuments(new PrefixQuery(newPathTerm(path + "/")));
        } catch (IOException e) {
            throw new CommitFailedException(
                    "Lucene", 5, "Failed to remove the index entries of"
                    + " the removed subtree " + path, e);
        }

        return null; // no need to recurse down the removed subtree
    }

    private Document makeDocument(String path, NodeState state) {
        Document document = new Document();
        document.add(newPathField(path));

        for (PropertyState property : state.getProperties()) {
            String pname = property.getName();
            if (isVisible(pname)
                    && (propertyTypes & (1 << property.getType().tag())) != 0) {
                if (Type.BINARY.tag() == property.getType().tag()) {
                    addBinaryValue(document, property, state);
                } else {
                    for (String value : property.getValue(Type.STRINGS)) {
                        document.add(newPropertyField(pname, value));
                        document.add(newFulltextField(value));
                    }
                }
            }
        }

        for (AggregatedState agg : aggregator.getAggregates(state)) {
            for (PropertyState property : agg.getProperties()) {
                String pname = property.getName();
                if (isVisible(pname)
                        && (propertyTypes & (1 << property.getType().tag())) != 0) {
                    if (Type.BINARY.tag() == property.getType().tag()) {
                        addBinaryValue(document, property, agg.get());
                    } else {
                        for (String v : property.getValue(Type.STRINGS)) {
                            document.add(newFulltextField(v));
                        }
                    }
                }
            }
        }

        return document;
    }

    private static boolean isVisible(String name) {
        return name.charAt(0) != ':';
    }

    private void addBinaryValue(
            Document doc, PropertyState property, NodeState state) {
        Metadata metadata = new Metadata();
        if (JCR_DATA.equals(property.getName())) {
            String type = getString(state, JcrConstants.JCR_MIMETYPE);
            if (type != null) { // not mandatory
                metadata.set(Metadata.CONTENT_TYPE, type);
            }
            String encoding = getString(state, JcrConstants.JCR_ENCODING);
            if (encoding != null) { // not mandatory
                metadata.set(Metadata.CONTENT_ENCODING, encoding);
            }
        }

        for (Blob v : property.getValue(Type.BINARIES)) {
            doc.add(newFulltextField(parseStringValue(v, metadata)));
        }
    }

    private String parseStringValue(Blob v, Metadata metadata) {
        WriteOutContentHandler handler = new WriteOutContentHandler();
        try {
            InputStream stream = v.getNewStream();
            try {
                parser.parse(stream, handler, metadata, new ParseContext());
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
                log.debug("Failed to extract text from a binary property."
                        + " This is a fairly common case, and nothing to"
                        + " worry about. The stack trace is included to"
                        + " help improve the text extraction feature.", t);
                return "TextExtractionError";
            }
        }
        return handler.toString();
    }

}
