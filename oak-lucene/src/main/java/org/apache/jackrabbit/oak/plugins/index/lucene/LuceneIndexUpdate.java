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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.getString;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newFulltextField;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newPathField;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newPropertyField;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.ANALYZER;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INCLUDE_PROPERTY_TYPES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INDEX_DATA_CHILD_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INDEX_DATA_CHILD_NAME_FS;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INDEX_PATH;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PERSISTENCE_FILE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PERSISTENCE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PERSISTENCE_OAK;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PERSISTENCE_PATH;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TO_WRITE_LOCK_MS;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TO_MAX_RETRIES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TO_SLEEP_MS;

import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.VERSION;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TermFactory.newPathTerm;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.aggregation.AggregatedState;
import org.apache.jackrabbit.oak.plugins.index.lucene.aggregation.NodeAggregator;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.util.Text;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.WriteOutContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class LuceneIndexUpdate implements Closeable {

    private static final Logger log = LoggerFactory
            .getLogger(LuceneIndexUpdate.class);

    private static IndexWriterConfig getIndexWriterConfig() {
        // FIXME: Hack needed to make Lucene work in an OSGi environment
        Thread thread = Thread.currentThread();
        ClassLoader loader = thread.getContextClassLoader();
        thread.setContextClassLoader(IndexWriterConfig.class.getClassLoader());
        try {
            IndexWriterConfig config = new IndexWriterConfig(VERSION, ANALYZER);
            config.setMergeScheduler(new SerialMergeScheduler());
            config.setWriteLockTimeout(TO_WRITE_LOCK_MS);
            return config;
        } finally {
            thread.setContextClassLoader(loader);
        }
    }

    private static final IndexWriterConfig config = getIndexWriterConfig();

    /**
     * Parser used for extracting text content from binary properties for full
     * text indexing.
     */
    private final Parser parser;

    /**
     * The media types supported by the parser used.
     */
    private Set<MediaType> supportedMediaTypes;

    private final String path;

    private final Set<String> updates = new TreeSet<String>();

    private final IndexWriter writer;

    private final Set<Integer> propertyTypes;

    private final NodeAggregator aggregator;

    public LuceneIndexUpdate(String path, NodeBuilder index, Parser parser)
            throws CommitFailedException {
        this.path = path;
        this.parser = parser;
        this.propertyTypes = buildPropertyTypes(index);
        this.writer = newIndexWriter(index, path);
        this.aggregator = new NodeAggregator(index);
    }

    private static IndexWriter newIndexWriter(NodeBuilder index, String path)
            throws CommitFailedException {
        String type = getString(index, PERSISTENCE_NAME);
        if (type == null || PERSISTENCE_OAK.equalsIgnoreCase(type)) {
            try {
                return new IndexWriter(new ReadWriteOakDirectory(
                        index.child(INDEX_DATA_CHILD_NAME)), config);
            } catch (IOException e) {
                throw new CommitFailedException("Lucene", 1,
                        "Failed to update the full text search index", e);
            }
        }

        if (PERSISTENCE_FILE.equalsIgnoreCase(type)) {
            File f = getIndexChildFolder(getString(index, PERSISTENCE_PATH),
                    path);
            f.mkdirs();
            index.setProperty(INDEX_PATH, f.getAbsolutePath());
            try {
                Directory d = FSDirectory.open(f);
                return newIndexWriterTO(d, 0, TO_MAX_RETRIES, TO_SLEEP_MS);
            } catch (IOException e) {
                throw new CommitFailedException("Lucene", 1,
                        "Failed to update the full text search index", e);
            }
        }

        throw new CommitFailedException("Lucene", 1,
                "Unknown lucene persistence setting");
    }

    private static IndexWriter newIndexWriterTO(Directory d, int retry,
            int max, int sleep) throws IOException {
        try {
            return new IndexWriter(d, config);
        } catch (LockObtainFailedException lofe) {
            log.debug("Unable to create a new index writer ({}/{}): {}",
                    new Object[] { retry, max, lofe.getMessage() });
            retry++;
            if (retry > max) {
                log.debug("Unable to create a new index writer, giving up.");
                return null;
            }
            try {
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException e) {
                //
            }
            return newIndexWriterTO(d, retry, max, sleep);
        }
    }

    private static File getIndexChildFolder(String root, String path)
            throws CommitFailedException {
        File rootf = new File(".");
        if (root != null) {
            if (root.startsWith("..") || root.startsWith("/")) {
                throw new CommitFailedException("Lucene", 1,
                        "Index config path should be a descendant of the repository directory.");
            }
            root = root.toLowerCase();
            for (String p : root.split("/")) {
                rootf = new File(rootf, p);
            }
        }
        // TODO factor in the 'path' argument to not have overlapping lucene
        // index defs
        if (!PathUtils.denotesRoot(path)) {
            String elements = path.toLowerCase();
            if (elements.startsWith("/")) {
                elements = elements.substring(1);
            }
            for (String p : elements.split("/")) {
                rootf = new File(rootf, Text.escapeIllegalJcrChars(p));
            }
        }

        File f = new File(rootf, INDEX_DATA_CHILD_NAME_FS);
        f.mkdirs();
        return f;
    }

    private Set<Integer> buildPropertyTypes(NodeBuilder index) {
        PropertyState ps = index.getProperty(INCLUDE_PROPERTY_TYPES);
        if (ps == null) {
            return new HashSet<Integer>();
        }
        Set<Integer> includes = new HashSet<Integer>();
        for (String inc : ps.getValue(Type.STRINGS)) {
            // TODO add more types as needed
            if (Type.STRING.toString().equalsIgnoreCase(inc)) {
                includes.add(Type.STRING.tag());
            } else if (Type.BINARY.toString().equalsIgnoreCase(inc)) {
                includes.add(Type.STRING.tag());
            }
        }
        return includes;
    }

    public void insert(String path, NodeBuilder value)
            throws CommitFailedException {
        if (writer == null) {
            // noop
            return;
        }

        // null value can come from a deleted node, followed by a deleted
        // property event which would trigger an update on the previously
        // deleted node
        if (value == null) {
            return;
        }
        checkArgument(path.startsWith(this.path));
        String key = path.substring(this.path.length());
        if ("".equals(key)) {
            key = "/";
        }
        if (!key.startsWith("/")) {
            key = "/" + key;
        }
        if (updates.contains(key)) {
            return;
        }
        updates.add(key);
        try {
            writer.updateDocument(newPathTerm(key),
                    makeDocument(key, value.getNodeState()));
        } catch (IOException e) {
            throw new CommitFailedException("Lucene", 1,
                    "Failed to update the full text search index", e);
        }
    }

    public void remove(String path) throws CommitFailedException {
        if (writer == null) {
            // noop
            return;
        }

        checkArgument(path.startsWith(this.path));
        try {
            deleteSubtreeWriter(writer, path.substring(this.path.length()));
        } catch (IOException e) {
            throw new CommitFailedException("Lucene", 1,
                    "Failed to update the full text search index", e);
        }
    }

    public void apply() throws CommitFailedException {
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException e) {
                throw new CommitFailedException("Lucene", 1,
                        "Failed to update the full text search index", e);
            }
        }
    }

    private void deleteSubtreeWriter(IndexWriter writer, String path)
            throws IOException {
        // TODO verify the removal of the entire sub-hierarchy
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        writer.deleteDocuments(newPathTerm(path));
        if (!path.endsWith("/")) {
            path += "/";
        }
        writer.deleteDocuments(new PrefixQuery(newPathTerm(path)));
    }

    private Document makeDocument(String path, NodeState state) {
        Document document = new Document();
        document.add(newPathField(path));
        for (PropertyState property : state.getProperties()) {
            String pname = property.getName();
            if (isVisible(pname) && propertyTypes.isEmpty()
                    || propertyTypes.contains(property.getType().tag())) {
                if (Type.BINARY.tag() == property.getType().tag()) {
                    addBinaryValue(document, property, state);
                } else {
                    for (String v : property.getValue(Type.STRINGS)) {
                        document.add(newPropertyField(pname, v));
                        document.add(newFulltextField(v));
                    }
                }
            }
        }
        for (AggregatedState agg : aggregator.getAggregates(state)) {
            for (PropertyState property : agg.getProperties()) {
                String pname = property.getName();
                if (isVisible(pname) && propertyTypes.isEmpty()
                        || propertyTypes.contains(property.getType().tag())) {
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

    private void addBinaryValue(Document doc, PropertyState property,
            NodeState state) {
        String type = getString(state, JcrConstants.JCR_MIMETYPE);
        if (type == null || !isSupportedMediaType(type)) {
            return;
        }
        Metadata metadata = new Metadata();
        metadata.set(Metadata.CONTENT_TYPE, type);
        // jcr:encoding is not mandatory
        String encoding = getString(state, JcrConstants.JCR_ENCODING);
        if (encoding != null) {
            metadata.set(Metadata.CONTENT_ENCODING, encoding);
        }

        for (Blob v : property.getValue(Type.BINARIES)) {
            doc.add(newFulltextField(parseStringValue(v, metadata)));
        }
    }

    /**
     * Returns <code>true</code> if the provided type is among the types
     * supported by the Tika parser we are using.
     * 
     * @param type
     *            the type to check.
     * @return whether the type is supported by the Tika parser we are using.
     */
    private boolean isSupportedMediaType(final String type) {
        if (supportedMediaTypes == null) {
            supportedMediaTypes = parser.getSupportedTypes(null);
        }
        return supportedMediaTypes.contains(MediaType.parse(type));
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

    @Override
    public void close() throws IOException {
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException e) {
                //
            }
        }
    }
}
