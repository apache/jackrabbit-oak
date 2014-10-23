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
import static org.apache.jackrabbit.oak.api.Type.BOOLEAN;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_REINDEX_VALUE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;
import static org.apache.jackrabbit.oak.spi.commit.CompositeEditor.compose;
import static org.apache.jackrabbit.oak.spi.commit.EditorDiff.process;
import static org.apache.jackrabbit.oak.spi.commit.VisibleEditor.wrap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;

public class IndexUpdate implements Editor {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final IndexEditorProvider provider;

    private final String async;

    private final NodeState root;

    private final NodeBuilder builder;

    /** Parent updater, or {@code null} if this is the root updater. */
    private final IndexUpdate parent;

    /** Name of this node, or {@code null} for the root node. */
    private final String name;

    /** Path of this editor, built lazily in {@link #getPath()}. */
    private String path;

    /**
     * Editors for indexes that will be normally updated.
     */
    private final List<Editor> editors = newArrayList();

    /**
     * Editors for indexes that need to be re-indexed.
     */
    private final Map<String, Editor> reindex = new HashMap<String, Editor>();

    /**
     * Callback for the update events of the indexing job
     */
    private final IndexUpdateCallback updateCallback;

    private MissingIndexProviderStrategy missingProvider = new MissingIndexProviderStrategy();

    public IndexUpdate(
            IndexEditorProvider provider, String async,
            NodeState root, NodeBuilder builder,
            IndexUpdateCallback updateCallback) {
        this.parent = null;
        this.name = null;
        this.path = "/";
        this.provider = checkNotNull(provider);
        this.async = async;
        this.root = checkNotNull(root);
        this.builder = checkNotNull(builder);
        this.updateCallback = checkNotNull(updateCallback);
    }

    private IndexUpdate(IndexUpdate parent, String name) {
        this.parent = checkNotNull(parent);
        this.name = name;
        this.provider = parent.provider;
        this.async = parent.async;
        this.root = parent.root;
        this.builder = parent.builder.getChildNode(checkNotNull(name));
        this.updateCallback = parent.updateCallback;
    }

    @Override
    public void enter(NodeState before, NodeState after)
            throws CommitFailedException {
        collectIndexEditors(builder.getChildNode(INDEX_DEFINITIONS_NAME), before);

        if(!reindex.isEmpty()){
            log.info("Reindexing would be performed for following indexes {}", reindex.keySet());
        }

        // no-op when reindex is empty
        CommitFailedException exception = process(
                wrap(compose(reindex.values())), MISSING_NODE, after);
        if (exception != null) {
            throw exception;
        }

        for (Editor editor : editors) {
            editor.enter(before, after);
        }
    }

    private boolean shouldReindex(NodeBuilder definition, NodeState before,
            String name) {
        PropertyState ps = definition.getProperty(REINDEX_PROPERTY_NAME);
        if (ps != null && ps.getValue(BOOLEAN)) {
            return true;
        }
        // reindex in the case this is a new node, even though the reindex flag
        // might be set to 'false' (possible via content import)
        boolean result = !before.getChildNode(INDEX_DEFINITIONS_NAME).hasChildNode(name);
        if(result){
            log.info("Found a new index node [{}]. Reindexing would be requested", name);
        }
        return result;
    }

    private void collectIndexEditors(NodeBuilder definitions,
            NodeState before) throws CommitFailedException {
        for (String name : definitions.getChildNodeNames()) {
            NodeBuilder definition = definitions.getChildNode(name);
            if (Objects.equal(async, definition.getString(ASYNC_PROPERTY_NAME))) {
                String type = definition.getString(TYPE_PROPERTY_NAME);
                if (type == null) {
                    // probably not an index def
                    continue;
                }
                boolean shouldReindex = shouldReindex(definition,
                        before, name);
                Editor editor = provider.getIndexEditor(type, definition, root, updateCallback);
                if (editor == null) {
                    missingProvider.onMissingIndex(type, definition);
                } else if (shouldReindex) {
                    if (definition.getBoolean(REINDEX_ASYNC_PROPERTY_NAME)
                            && definition.getString(ASYNC_PROPERTY_NAME) == null) {
                        // switch index to an async update mode
                        definition.setProperty(ASYNC_PROPERTY_NAME,
                                ASYNC_REINDEX_VALUE);
                    } else {
                        definition.setProperty(REINDEX_PROPERTY_NAME, false);
                        // as we don't know the index content node name
                        // beforehand, we'll remove all child nodes
                        for (String rm : definition.getChildNodeNames()) {
                            definition.getChildNode(rm).remove();
                        }
                        reindex.put(concat(getPath(), INDEX_DEFINITIONS_NAME, name), editor);
                    }
                } else {
                    editors.add(editor);
                }
            }
        }
    }

    /**
     * Returns the path of this node, building it lazily when first requested.
     */
    private String getPath() {
        if (path == null) {
            path = concat(parent.getPath(), name);
        }
        return path;
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
        return compose(children);
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
        return compose(children);
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
        return compose(children);
    }

    protected Set<String> getReindexedDefinitions() {
        return reindex.keySet();
    }

    public static class MissingIndexProviderStrategy {
        public void onMissingIndex(String type, NodeBuilder definition)
                throws CommitFailedException {
            // trigger reindexing when an indexer becomes available
            definition.setProperty(REINDEX_PROPERTY_NAME, true);
        }
    }

    public IndexUpdate withMissingProviderStrategy(
            MissingIndexProviderStrategy missingProvider) {
        this.missingProvider = missingProvider;
        return this;
    }

}
