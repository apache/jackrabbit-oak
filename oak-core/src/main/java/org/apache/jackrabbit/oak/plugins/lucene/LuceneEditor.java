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
package org.apache.jackrabbit.oak.plugins.lucene;

import java.io.IOException;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.commit.CommitEditor;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.Version;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;

public class LuceneEditor implements CommitEditor {

    private static final Tika TIKA = new Tika();

    private static final Version VERSION = Version.LUCENE_36;

    private static final Analyzer ANALYZER = new StandardAnalyzer(VERSION);

    private final String[] path;

    public LuceneEditor(String... path) {
        this.path = path;
    }

    @Override
    public NodeState editCommit(
            NodeStore store, NodeState before, NodeState after)
            throws CommitFailedException {
        try {
            OakDirectory directory = new OakDirectory(store, after, path);

            IndexWriter writer = new IndexWriter(
                    directory, new IndexWriterConfig(VERSION, ANALYZER));
            try {
                LuceneDiff diff = new LuceneDiff(writer, "");
                after.compareAgainstBaseState(before, diff);
                diff.postProcess(after);
                writer.commit();
            } finally {
                writer.close();
            }

            return directory.getRoot();
        } catch (IOException e) {
            throw new CommitFailedException(
                    "Failed to update the full text search index", e);
        }
    }

    private static class LuceneDiff implements NodeStateDiff {

        private final IndexWriter writer;

        private final String path;

        private boolean modified = false;

        private IOException exception = null;

        public LuceneDiff(IndexWriter writer, String path) {
            this.writer = writer;
            this.path = path;
        }

        public void postProcess(NodeState state) throws IOException {
            if (exception != null) {
                throw exception;
            }
            if (modified) {
                writer.updateDocument(
                        makePathTerm(path),
                        makeDocument(path, state));
            }
        }

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
            if (exception == null) {
                try {
                    addSubtree(path + "/" + name, after);
                } catch (IOException e) {
                    exception = e;
                }
            }
        }

        @Override
        public void childNodeChanged(
                String name, NodeState before, NodeState after) {
            if (exception == null) {
                try {
                    LuceneDiff diff = new LuceneDiff(writer, path + "/" + name);
                    after.compareAgainstBaseState(before, diff);
                    diff.postProcess(after);
                } catch (IOException e) {
                    exception = e;
                }
            }
        }

        @Override
        public void childNodeDeleted(String name, NodeState before) {
            if (exception == null) {
                try {
                    deleteSubtree(path + "/" + name, before);
                } catch (IOException e) {
                    exception = e;
                }
            }
        }

        private void addSubtree(String path, NodeState state)
                throws IOException {
            writer.addDocument(makeDocument(path, state));
            for (ChildNodeEntry entry : state.getChildNodeEntries()) {
                addSubtree(path + "/" + entry.getName(), entry.getNodeState());
            }
        }

        private void deleteSubtree(String path, NodeState state)
                throws IOException {
            writer.deleteDocuments(makePathTerm(path));
            for (ChildNodeEntry entry : state.getChildNodeEntries()) {
                deleteSubtree(path + "/" + entry.getName(), entry.getNodeState());
            }
        }

        private Term makePathTerm(String path) {
            return new Term(":path", path);
        }

        private Document makeDocument(
                String path, NodeState state) {
            Document document = new Document();
            document.add(new Field(
                    ":path", path, Store.YES, Index.NOT_ANALYZED));
            for (PropertyState property : state.getProperties()) {
                String pname = property.getName();
                if (property.isArray()) {
                    for (CoreValue value : property.getValues()) {
                        document.add(makeField(pname, value));
                    }
                } else {
                    document.add(makeField(pname, property.getValue()));
                }
            }
            return document;
        }

        private Field makeField(String name, CoreValue value) {
            String string;
            if (value.getType() != PropertyType.BINARY) {
                string = value.getString();
            } else {
                try {
                    string = TIKA.parseToString(value.getNewStream());
                } catch (IOException e) {
                    string = "";
                } catch (TikaException e) {
                    string = "";
                }
            }
            return new Field(name, string, Store.NO, Index.ANALYZED);
        }

    }

}
