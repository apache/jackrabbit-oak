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

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;

import java.io.IOException;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.util.Version;

/**
 * This class updates a Lucene index when node content is changed.
 */
class LuceneEditor implements IndexHook, LuceneIndexConstants {

    private static final Version VERSION = Version.LUCENE_40;

    private static final Analyzer ANALYZER = new StandardAnalyzer(VERSION);

    private static final IndexWriterConfig config = getIndexWriterConfig();

    private LuceneIndexDiff diff;

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

    private final NodeBuilder root;

    public LuceneEditor(NodeBuilder root) {
        this.root = root;
    }

    // -----------------------------------------------------< IndexHook >--

    @Override
    public NodeStateDiff preProcess() throws CommitFailedException {
        try {
            IndexWriter writer = new IndexWriter(new ReadWriteOakDirectory(
                    getIndexNode(root).child(INDEX_DATA_CHILD_NAME)), config);
            diff = new LuceneIndexDiff(writer, root, "/");
            return diff;
        } catch (IOException e) {
            e.printStackTrace();
            throw new CommitFailedException(
                    "Failed to create writer for the full text search index", e);
        }
    }

    private static NodeBuilder getIndexNode(NodeBuilder node) {
        if (node != null && node.hasChildNode(INDEX_DEFINITIONS_NAME)) {
            NodeBuilder index = node.child(INDEX_DEFINITIONS_NAME);
            for (String indexName : index.getChildNodeNames()) {
                NodeBuilder child = index.child(indexName);
                if (isIndexNodeType(child.getProperty(JCR_PRIMARYTYPE))
                        && isIndexType(child.getProperty(TYPE_PROPERTY_NAME),
                                TYPE_LUCENE)) {
                    return child;
                }
            }
        }
        // did not find a proper child, will use root directly
        return node;
    }

    private static boolean isIndexNodeType(PropertyState ps) {
        return ps != null && !ps.isArray()
                && ps.getValue(Type.STRING).equals(INDEX_DEFINITIONS_NODE_TYPE);
    }

    private static boolean isIndexType(PropertyState ps, String type) {
        return ps != null && !ps.isArray()
                && ps.getValue(Type.STRING).equals(type);
    }

    @Override
    public void postProcess() throws CommitFailedException {
        try {
            diff.postProcess();
        } catch (IOException e) {
            e.printStackTrace();
            throw new CommitFailedException(
                    "Failed to update the full text search index", e);
        }
    }

    @Override
    public void close() throws IOException {
        diff.close();
        diff = null;
    }
}
