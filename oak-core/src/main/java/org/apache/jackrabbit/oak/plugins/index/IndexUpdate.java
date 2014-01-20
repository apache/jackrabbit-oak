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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

import java.util.List;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.commit.VisibleEditor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.base.Objects;

class IndexUpdate implements Editor {

    private final IndexEditorProvider provider;

    private final String async;

    private final NodeState root;

    private final NodeBuilder builder;

    /**
     * Editors for indexes that will be normally updated.
     */
    private final List<Editor> editors = newArrayList();

    /**
     * Editors for indexes that need to be re-indexed.
     */
    private final List<Editor> reindex = newArrayList();

    /**
     * Callback for the 'before' events of the indexing job
     */
    private final IndexUpdateCallback updateCallback;

    IndexUpdate(
            IndexEditorProvider provider, String async,
            NodeState root, NodeBuilder builder,
            IndexUpdateCallback updateCallback) {
        this.provider = checkNotNull(provider);
        this.async = async;
        this.root = checkNotNull(root);
        this.builder = checkNotNull(builder);
        this.updateCallback = updateCallback;
    }

    private IndexUpdate(IndexUpdate parent, String name) {
        checkNotNull(parent);
        this.provider = parent.provider;
        this.async = parent.async;
        this.root = parent.root;
        this.builder = parent.builder.child(checkNotNull(name));
        this.updateCallback = parent.updateCallback;
    }

    @Override
    public void enter(NodeState before, NodeState after)
            throws CommitFailedException {
        collectIndexEditors(builder.getChildNode(INDEX_DEFINITIONS_NAME));

        // no-op when reindex is empty
        CommitFailedException exception = EditorDiff.process(
                CompositeEditor.compose(reindex), MISSING_NODE, after);
        if (exception != null) {
            throw exception;
        }

        for (Editor editor : editors) {
            editor.enter(before, after);
        }
    }

    private void collectIndexEditors(NodeBuilder definitions)
            throws CommitFailedException {
        for (String name : definitions.getChildNodeNames()) {
            NodeBuilder definition = definitions.getChildNode(name);
            if (Objects.equal(async, definition.getString(ASYNC_PROPERTY_NAME))) {
                String type = definition.getString(TYPE_PROPERTY_NAME);
                Editor editor = provider.getIndexEditor(type, definition, root, updateCallback);
                if (editor == null) {
                    // trigger reindexing when an indexer becomes available
                    definition.setProperty(REINDEX_PROPERTY_NAME, true);
                } else if (definition.getBoolean(REINDEX_PROPERTY_NAME)) {
                    definition.setProperty(REINDEX_PROPERTY_NAME, false);
                    // as we don't know the index content node name
                    // beforehand, we'll remove all child nodes
                    for (String rm : definition.getChildNodeNames()) {
                        definition.getChildNode(rm).remove();
                    }
                    reindex.add(VisibleEditor.wrap(editor));
                } else {
                    editors.add(VisibleEditor.wrap(editor));
                }
            }
        }
    }

    @Override
    public void leave(NodeState before, NodeState after)
            throws CommitFailedException {
        for (Editor editor : editors) {
            editor.leave(before, after);
        }
    }

    @Override
    public void propertyAdded(PropertyState after)
            throws CommitFailedException {
        for (Editor editor : editors) {
            editor.propertyAdded(after);
        }
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after)
            throws CommitFailedException {
        for (Editor editor : editors) {
            editor.propertyChanged(before, after);
        }
    }

    @Override
    public void propertyDeleted(PropertyState before)
            throws CommitFailedException {
        for (Editor editor : editors) {
            editor.propertyDeleted(before);
        }
    }

    @Override @Nonnull
    public Editor childNodeAdded(String name, NodeState after)
            throws CommitFailedException {
        List<Editor> children = newArrayListWithCapacity(1 + editors.size());
        children.add(new IndexUpdate(this, name));
        for (Editor editor : editors) {
            Editor child = editor.childNodeAdded(name, after);
            if (child != null) {
                children.add(child);
            }
        }
        return CompositeEditor.compose(children);
    }

    @Override @Nonnull
    public Editor childNodeChanged(
            String name, NodeState before, NodeState after)
            throws CommitFailedException {
        List<Editor> children = newArrayListWithCapacity(1 + editors.size());
        children.add(new IndexUpdate(this, name));
        for (Editor editor : editors) {
            Editor child = editor.childNodeChanged(name, before, after);
            if (child != null) {
                children.add(child);
            }
        }
        return CompositeEditor.compose(children);
    }

    @Override @CheckForNull
    public Editor childNodeDeleted(String name, NodeState before)
            throws CommitFailedException {
        List<Editor> children = newArrayListWithCapacity(editors.size());
        for (Editor editor : editors) {
            Editor child = editor.childNodeDeleted(name, before);
            if (child != null) {
                children.add(child);
            }
        }
        return CompositeEditor.compose(children);
    }

}
