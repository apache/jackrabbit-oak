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
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.PrefixQuery;
import org.apache.tika.exception.TikaException;

import com.google.common.base.Preconditions;

class LuceneIndexUpdate implements Closeable, LuceneIndexConstants {

    private static IndexWriterConfig getIndexWriterConfig() {
        // FIXME: Hack needed to make Lucene work in an OSGi environment
        Thread thread = Thread.currentThread();
        ClassLoader loader = thread.getContextClassLoader();
        thread.setContextClassLoader(IndexWriterConfig.class.getClassLoader());
        try {
            return new IndexWriterConfig(VERSION, ANALYZER);
        } finally {
            thread.setContextClassLoader(loader);
        }
    }

    private static final IndexWriterConfig config = getIndexWriterConfig();

    private final String path;

    private final NodeBuilder index;

    private final Map<String, NodeState> insert = new TreeMap<String, NodeState>();

    private final Set<String> remove = new TreeSet<String>();

    public LuceneIndexUpdate(String path, NodeBuilder index) {
        this.path = path;
        this.index = index;
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
        // for (ChildNodeEntry entry : state.getChildNodeEntries()) {
        // if (NodeStateUtils.isHidden(entry.getName())) {
        // continue;
        // }
        // addSubtreeWriter(writer, concat(path, entry.getName()),
        // entry.getNodeState(), paths);
        // }
    }

    private static Document makeDocument(String path, NodeState state) {
        Document document = new Document();
        document.add(newPathField(path));
        for (PropertyState property : state.getProperties()) {
            String pname = property.getName();
            switch (property.getType().tag()) {
            case PropertyType.BINARY:
                for (Blob v : property.getValue(Type.BINARIES)) {
                    document.add(newPropertyField(pname, parseStringValue(v)));
                }
                break;
            default:
                for (String v : property.getValue(Type.STRINGS)) {
                    document.add(newPropertyField(pname, v));
                }
                break;
            }
        }
        return document;
    }

    private static String parseStringValue(Blob v) {
        try {
            return TIKA.parseToString(v.getNewStream());
        } catch (IOException e) {
        } catch (TikaException e) {
        }
        return "";
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
