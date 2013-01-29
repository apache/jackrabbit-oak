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

import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newPathField;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newPropertyField;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TermFactory.newPathTerm;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.search.PrefixQuery;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.WriteOutContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

class LuceneIndexUpdate implements Closeable, LuceneIndexConstants {

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

    private final NodeBuilder index;

    private final Map<String, NodeState> insert = new TreeMap<String, NodeState>();

    private final Set<String> remove = new TreeSet<String>();

    public LuceneIndexUpdate(String path, NodeBuilder index, Parser parser) {
        this.path = path;
        this.index = index;
        this.parser = parser;
    }

    public void insert(String path, NodeBuilder value) {
        Preconditions.checkArgument(path.startsWith(this.path));
        if (!insert.containsKey(path)) {
            String key = path.substring(this.path.length());
            if ("".equals(key)) {
                key = "/";
            }
            // null value can come from a deleted node, followed by a deleted
            // property event which would trigger an update on the previously
            // deleted node
            if (value != null) {
                insert.put(key, value.getNodeState());
            }
        }
    }

    public void remove(String path) {
        Preconditions.checkArgument(path.startsWith(this.path));
        remove.add(path.substring(this.path.length()));
    }

    boolean getAndResetReindexFlag() {
        boolean reindex = index.getProperty(REINDEX_PROPERTY_NAME) != null
                && index.getProperty(REINDEX_PROPERTY_NAME).getValue(
                        Type.BOOLEAN);
        index.setProperty(REINDEX_PROPERTY_NAME, false);
        return reindex;
    }

    public void apply() throws CommitFailedException {
        if(remove.isEmpty() && insert.isEmpty()){
            return;
        }
        IndexWriter writer = null;
        try {
            writer = new IndexWriter(new ReadWriteOakDirectory(
                    index.child(INDEX_DATA_CHILD_NAME)), config);
            for (String p : remove) {
                deleteSubtreeWriter(writer, p);
            }
            for (String p : insert.keySet()) {
                NodeState ns = insert.get(p);
                addSubtreeWriter(writer, p, ns);
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new CommitFailedException(
                    "Failed to update the full text search index", e);
        } finally {
            remove.clear();
            insert.clear();
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    //
                }
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

    private void addSubtreeWriter(IndexWriter writer, String path,
            NodeState state) throws IOException {
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        writer.updateDocument(newPathTerm(path), makeDocument(path, state));
    }

    private Document makeDocument(String path, NodeState state) {
        Document document = new Document();
        document.add(newPathField(path));
        for (PropertyState property : state.getProperties()) {
            switch (property.getType().tag()) {
            case PropertyType.BINARY:
                addBinaryValue(document, property, state);
                break;
            default:
                String pname = property.getName();
                for (String v : property.getValue(Type.STRINGS)) {
                    document.add(newPropertyField(pname, v));
                }
                break;
            }
        }
        return document;
    }

    private void addBinaryValue(Document doc, PropertyState property,
            NodeState state) {
        String type = getOrNull(state, JcrConstants.JCR_MIMETYPE);
        if (type == null || !isSupportedMediaType(type)) {
            return;
        }
        Metadata metadata = new Metadata();
        metadata.set(Metadata.CONTENT_TYPE, type);
        // jcr:encoding is not mandatory
        String encoding = getOrNull(state, JcrConstants.JCR_ENCODING);
        if (encoding != null) {
            metadata.set(Metadata.CONTENT_ENCODING, encoding);
        }

        String name = property.getName();
        for (Blob v : property.getValue(Type.BINARIES)) {
            doc.add(newPropertyField(name, parseStringValue(v, metadata)));
        }
    }

    private static String getOrNull(NodeState state, String name) {
        PropertyState p = state.getProperty(name);
        if (p != null) {
            return p.getValue(Type.STRING);
        }
        return null;
    }

    /**
     * Returns <code>true</code> if the provided type is among the types
     * supported by the Tika parser we are using.
     *
     * @param type  the type to check.
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
        remove.clear();
        insert.clear();
    }

    @Override
    public String toString() {
        return "LuceneIndexUpdate [path=" + path + ", insert=" + insert
                + ", remove=" + remove + "]";
    }
}
