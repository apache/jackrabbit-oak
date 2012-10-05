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

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.index.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.memory.CoreValues;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;

import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.plugins.index.IndexDefinition.INDEX_DATA_CHILD_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newPathField;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newPropertyField;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TermFactory.newPathTerm;

/**
 * This class updates a Lucene index when node content is changed.
 */
class LuceneEditor implements CommitHook, LuceneIndexConstants {

    private static final Tika TIKA = new Tika();

    private static final Version VERSION = Version.LUCENE_40;

    private static final Analyzer ANALYZER = new StandardAnalyzer(VERSION);

    private static final IndexWriterConfig config = getIndexWriterConfig();

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

    private final IndexDefinition index;

    public LuceneEditor(IndexDefinition indexDefinition) {
        this.index = indexDefinition;
    }

    @Override
    public NodeState processCommit(NodeState before, NodeState after)
            throws CommitFailedException {
        NodeBuilder rootBuilder = after.getBuilder();
        NodeBuilder builder = rootBuilder;
        for (String name : elements(index.getPath())) {
            builder = builder.getChildBuilder(name);
        }
        builder = builder.getChildBuilder(INDEX_DATA_CHILD_NAME);
        Directory directory = new ReadWriteOakDirectory(builder);

        try {
            IndexWriter writer = new IndexWriter(directory, config);
            try {
                LuceneDiff diff = new LuceneDiff(writer, "/");
                after.compareAgainstBaseState(before, diff);
                diff.postProcess(after);
            } finally {
                writer.close();
                builder.setProperty(INDEX_UPDATE, System.currentTimeMillis());
            }
            return rootBuilder.getNodeState();
        } catch (IOException e) {
            e.printStackTrace();
            throw new CommitFailedException(
                    "Failed to update the full text search index", e);
        }
    }

    private static class LuceneDiff implements NodeStateDiff {

        private final IndexWriter writer;

        private final String path;

        private boolean modified;

        private IOException exception;

        public LuceneDiff(IndexWriter writer, String path) {
            this.writer = writer;
            this.path = path;
        }

        public void postProcess(NodeState state) throws IOException {
            if (exception != null) {
                throw exception;
            }
            if (modified) {
                writer.updateDocument(newPathTerm(path),
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

        private void addSubtree(String path, NodeState state)
                throws IOException {
            writer.updateDocument(newPathTerm(path), makeDocument(path, state));
            for (ChildNodeEntry entry : state.getChildNodeEntries()) {
                if (NodeStateUtils.isHidden(entry.getName())) {
                    continue;
                }
                addSubtree(concat(path, entry.getName()), entry.getNodeState());
            }
        }

        @Override
        public void childNodeChanged(String name, NodeState before,
                NodeState after) {
            if (NodeStateUtils.isHidden(name)) {
                return;
            }
            if (exception == null) {
                try {
                    LuceneDiff diff = new LuceneDiff(writer, concat(path, name));
                    after.compareAgainstBaseState(before, diff);
                    diff.postProcess(after);
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

        private void deleteSubtree(String path, NodeState state)
                throws IOException {
            writer.deleteDocuments(newPathTerm(path));
            for (ChildNodeEntry entry : state.getChildNodeEntries()) {
                if (NodeStateUtils.isHidden(entry.getName())) {
                    continue;
                }
                deleteSubtree(concat(path, entry.getName()),
                        entry.getNodeState());
            }
        }

        private static Document makeDocument(String path, NodeState state) {
            Document document = new Document();
            document.add(newPathField(path));
            for (PropertyState property : state.getProperties()) {
                String pname = property.getName();
                for (CoreValue value : CoreValues.getValues(property)) {
                    document.add(newPropertyField(pname,
                            parseStringValue(value)));
                }
            }
            return document;
        }

        private static String parseStringValue(CoreValue value) {
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
            return string;
        }

    }

}
