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

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Maps.newHashMap;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TYPE_LUCENE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditor;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.commit.SubtreeEditor;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

class IndexTracker {

    /** Logger instance. */
    private static final Logger log =
            LoggerFactory.getLogger(IndexTracker.class);

    private NodeState root = EMPTY_NODE;

    private final Map<String, IndexNode> indices = newHashMap();

    synchronized void close() {
        for (Map.Entry<String, IndexNode> entry : indices.entrySet()) {
            try {
                entry.getValue().close();
            } catch (IOException e) {
                log.error("Failed to close the Lucene index at " + entry.getKey(), e);
            }
        }
        indices.clear();
    }

    synchronized void update(NodeState root) {
        List<Editor> editors = newArrayListWithCapacity(indices.size());
        for (final String path : indices.keySet()) {
            List<String> elements = newArrayList();
            Iterables.addAll(elements, PathUtils.elements(path));
            elements.add(INDEX_DEFINITIONS_NAME);
            elements.add(indices.get(path).getName());
            editors.add(new SubtreeEditor(new DefaultEditor() {
                @Override
                public void leave(NodeState before, NodeState after) {
                    IndexNode index = indices.remove(path);
                    try {
                        index.close();
                    } catch (IOException e) {
                        log.error("Failed to close Lucene index at " + path, e);
                    }

                    try {
                        index = IndexNode.open(index.getName(), after);
                        if (index != null) {
                            indices.put(path, index);
                        }
                    } catch (IOException e) {
                        log.error("Failed to open Lucene index at " + path, e);
                    }
                }
            }, elements.toArray(new String[elements.size()])));
        }
        EditorDiff.process(CompositeEditor.compose(editors), this.root, root);
        this.root = root;
    }

    synchronized IndexNode getIndexNode(String path) {
        IndexNode index = indices.get(path);
        if (index != null) {
            return index;
        }

        NodeState node = root;
        for (String name : PathUtils.elements(path)) {
            node = root.getChildNode(name);
        }
        node = node.getChildNode(INDEX_DEFINITIONS_NAME);

        try {
            for (ChildNodeEntry child : node.getChildNodeEntries()) {
                node = child.getNodeState();
                if (TYPE_LUCENE.equals(node.getString(TYPE_PROPERTY_NAME))) {
                    index = IndexNode.open(child.getName(), node);
                    if (index != null) {
                        indices.put(path, index);
                        return index;
                    }
                }
            }
        } catch (IOException e) {
            log.error("Could not access the Lucene index at " + path, e);
        }

        return null;
    }

}
