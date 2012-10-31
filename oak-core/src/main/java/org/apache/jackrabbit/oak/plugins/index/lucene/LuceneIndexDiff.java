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

import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newPathField;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newPropertyField;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TermFactory.newPathTerm;

import java.io.Closeable;
import java.io.IOException;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;

public class LuceneIndexDiff implements NodeStateDiff, Closeable {

    private static final Tika TIKA = new Tika();

    private final IndexWriter writer;

    private final NodeBuilder node;

    private final String path;

    private boolean modified;

    private IOException exception;

    public LuceneIndexDiff(IndexWriter writer, NodeBuilder node, String path) {
        this.writer = writer;
        this.node = node;
        this.path = path;
    }

    public void postProcess() throws IOException {
        if (exception != null) {
            throw exception;
        }
        if (modified) {
            writer.updateDocument(newPathTerm(path),
                    makeDocument(path, node.getNodeState()));
        }
    }

    // -----------------------------------------------------< NodeStateDiff >--

    @Override
    public void propertyAdded(PropertyState after) {
        modified = true;
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) {
        modified = true;
    }

    @Override
    public void propertyDeleted(PropertyState before) {
        modified = true;
    }

    @Override
    public void childNodeAdded(String name, NodeState after) {
        if (NodeStateUtils.isHidden(name)) {
            return;
        }
        if (exception == null) {
            try {
                addSubtree(concat(path, name), after);
            } catch (IOException e) {
                exception = e;
            }
        }
    }

    private void addSubtree(String path, NodeState state) throws IOException {
        writer.updateDocument(newPathTerm(path), makeDocument(path, state));
        for (ChildNodeEntry entry : state.getChildNodeEntries()) {
            if (NodeStateUtils.isHidden(entry.getName())) {
                continue;
            }
            addSubtree(concat(path, entry.getName()), entry.getNodeState());
        }
    }

    @Override
    public void childNodeChanged(String name, NodeState before, NodeState after) {
        if (NodeStateUtils.isHidden(name)) {
            return;
        }
        if (exception == null && node.hasChildNode(name)) {
            LuceneIndexDiff diff = new LuceneIndexDiff(writer,
                    node.child(name), concat(path, name));
            after.compareAgainstBaseState(before, diff);
            try {
                diff.postProcess();
            } catch (IOException e) {
                exception = e;
            }
        }
    }

    @Override
    public void childNodeDeleted(String name, NodeState before) {
        if (NodeStateUtils.isHidden(name)) {
            return;
        }
        if (exception == null) {
            try {
                deleteSubtree(concat(path, name), before);
            } catch (IOException e) {
                exception = e;
            }
        }
    }

    private void deleteSubtree(String path, NodeState state) throws IOException {
        writer.deleteDocuments(newPathTerm(path));
        for (ChildNodeEntry entry : state.getChildNodeEntries()) {
            if (NodeStateUtils.isHidden(entry.getName())) {
                continue;
            }
            deleteSubtree(concat(path, entry.getName()), entry.getNodeState());
        }
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

    // -----------------------------------------------------< Closeable >--

    @Override
    public void close() throws IOException {
        writer.close();
    }

}
