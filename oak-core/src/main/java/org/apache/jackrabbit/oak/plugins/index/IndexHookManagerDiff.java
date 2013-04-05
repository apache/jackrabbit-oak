/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_UNKNOWN;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditor;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.commit.VisibleEditor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.collect.Lists;

/**
 * Acts as a composite Editor, it delegates all the diff's events to the
 * existing IndexHooks. <br>
 * This allows for a simultaneous update of all the indexes via a single
 * traversal of the changes.
 */
class IndexHookManagerDiff implements Editor {

    private final IndexHookProvider provider;

    private final NodeBuilder node;

    private Editor inner = new DefaultEditor();

    public IndexHookManagerDiff(IndexHookProvider provider, NodeBuilder node) {
        this.provider = provider;
        this.node = node;
    }

    @Override
    public void enter(NodeState before, NodeState after)
            throws CommitFailedException {
        NodeState ref = node.getNodeState();
        if (ref.hasChildNode(INDEX_DEFINITIONS_NAME)) {
            Set<String> existingTypes = new HashSet<String>();
            Set<String> reindexTypes = new HashSet<String>();
            NodeState index = ref.getChildNode(INDEX_DEFINITIONS_NAME);
            for (String indexName : index.getChildNodeNames()) {
                NodeState indexChild = index.getChildNode(indexName);
                if (isIndexNodeType(indexChild.getProperty(JCR_PRIMARYTYPE))) {
                    PropertyState reindexPS = indexChild
                            .getProperty(REINDEX_PROPERTY_NAME);
                    boolean reindex = reindexPS == null
                            || (reindexPS != null && indexChild.getProperty(
                                    REINDEX_PROPERTY_NAME).getValue(
                                    Type.BOOLEAN));
                    String type = TYPE_UNKNOWN;
                    PropertyState typePS = indexChild
                            .getProperty(TYPE_PROPERTY_NAME);
                    if (typePS != null && !typePS.isArray()) {
                        type = typePS.getValue(Type.STRING);
                    }
                    if (reindex) {
                        reindexTypes.add(type);
                    }
                    existingTypes.add(type);
                }
            }
            existingTypes.remove(TYPE_UNKNOWN);
            reindexTypes.remove(TYPE_UNKNOWN);

            List<IndexHook> hooks = Lists.newArrayList();
            List<IndexHook> reindex = Lists.newArrayList();
            for (String type : existingTypes) {
                List<? extends IndexHook> hooksTmp = provider.getIndexHooks(
                        type, node);
                if (reindexTypes.contains(type)) {
                    reindex.addAll(hooksTmp);
                } else {
                    hooks.addAll(hooksTmp);
                }
            }
            reindex(reindex, ref);
            if (!hooks.isEmpty()) {
                this.inner = VisibleEditor.wrap(CompositeEditor.compose(hooks));
                this.inner.enter(before, after);
            }
        }
    }

    private void reindex(List<IndexHook> hooks, NodeState state)
            throws CommitFailedException {
        if (hooks.isEmpty()) {
            return;
        }
        List<Editor> editors = Lists.newArrayList();
        for (IndexHook ih : hooks) {
            ih.enter(EMPTY_NODE, state);
            Editor e = ih.reindex(state);
            if (e != null) {
                editors.add(e);
            }
        }
        final Editor reindexer = VisibleEditor.wrap(CompositeEditor
                .compose(editors));
        if (reindexer == null) {
            return;
        }
        EditorProvider provider = new EditorProvider() {
            @Override
            public Editor getRootEditor(NodeState before, NodeState after,
                    NodeBuilder builder) {
                return reindexer;
            }
        };
        EditorHook eh = new EditorHook(provider);
        eh.processCommit(EMPTY_NODE, state);
    }

    @Override
    public void leave(NodeState before, NodeState after)
            throws CommitFailedException {
        this.inner.leave(before, after);
    }

    private static boolean isIndexNodeType(PropertyState ps) {
        return ps != null && !ps.isArray()
                && ps.getValue(Type.STRING).equals(INDEX_DEFINITIONS_NODE_TYPE);
    }

    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        inner.propertyAdded(after);
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after)
            throws CommitFailedException {
        inner.propertyChanged(before, after);
    }

    @Override
    public void propertyDeleted(PropertyState before)
            throws CommitFailedException {
        inner.propertyDeleted(before);
    }

    @Override
    public Editor childNodeAdded(String name, NodeState after)
            throws CommitFailedException {
        return inner.childNodeAdded(name, after);
    }

    @Override
    public Editor childNodeChanged(String name, NodeState before,
            NodeState after) throws CommitFailedException {
        return inner.childNodeChanged(name, before, after);
    }

    @Override
    public Editor childNodeDeleted(String name, NodeState before)
            throws CommitFailedException {
        return inner.childNodeDeleted(name, before);
    }

}