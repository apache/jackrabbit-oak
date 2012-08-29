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

import java.io.IOException;
import java.util.Iterator;

import org.apache.jackrabbit.mk.json.JsopReader;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.apache.jackrabbit.mk.simple.NodeImpl;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.query.IndexDefinition;
import org.apache.jackrabbit.oak.spi.query.IndexDefinitionImpl;
import org.apache.jackrabbit.oak.spi.query.IndexUtils;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * A node handler that maps the property value to the key, and the path of the
 * node to the value. Only string and numbers are indexes (arrays, true, false,
 * and null are not indexes).
 */
public class PropertyIndex implements PIndex {

    private final Indexer indexer;
    private final BTree tree;
    private final String propertyName;

    private final IndexDefinition indexDefinition;

    public PropertyIndex(Indexer indexer, String propertyName, boolean unique) {
        this(indexer, propertyName, unique, new IndexDefinitionImpl(
                propertyName, PropertyIndexFactory.TYPE_PREFIX,
                PathUtils.concat(IndexUtils.DEFAULT_INDEX_HOME, propertyName),
                false, null));
    }

    public PropertyIndex(Indexer indexer, String propertyName, boolean unique, IndexDefinition indexDefinition) {
        this.indexer = indexer;
        this.propertyName = propertyName;
        this.tree = new BTree(indexer, Indexer.TYPE_PROPERTY + propertyName +
                (unique ? "," + Indexer.UNIQUE : ""), unique);
        tree.setMinSize(10);
        this.indexDefinition = indexDefinition;
    }

    public static PropertyIndex fromNodeName(Indexer indexer, String nodeName) {
        if (!nodeName.startsWith(Indexer.TYPE_PROPERTY)) {
            return null;
        }
        boolean unique = false;
        if (nodeName.endsWith(Indexer.UNIQUE)) {
            unique = true;
            nodeName = nodeName.substring(0, nodeName.length() - Indexer.UNIQUE.length() - 1);
        }
        String property = nodeName.substring(Indexer.TYPE_PROPERTY.length());
        return new PropertyIndex(indexer, property, unique);
    }

    public String getPropertyName() {
        return propertyName;
    }

    @Override
    public IndexDefinition getDefinition() {
        return indexDefinition;
    }

    @Override
    public void addOrRemoveNode(NodeImpl node, boolean add) {
        String value = node.getProperty(propertyName);
        if (value != null) {
            addOrRemoveRaw(node.getPath(), value, add);
        }
    }

    @Override
    public void addOrRemoveProperty(String nodePath, String propertyName,
            String value, boolean add) {
        if (this.propertyName.equals(propertyName)) {
            addOrRemoveRaw(nodePath, value, add);
        }
    }

    private void addOrRemoveRaw(String nodePath, String value, boolean add) {
        JsopTokenizer t = new JsopTokenizer(value);
        if (t.matches(JsopReader.STRING) || t.matches(JsopReader.NUMBER)) {
            String v = t.getToken();
            addOrRemove(nodePath, v, add);
        }
    }

    private void addOrRemove(String nodePath, String value, boolean add) {
        if (add) {
            tree.add(value, nodePath);
        } else {
            tree.remove(value, nodePath);
        }
    }

    /**
     * Get the path for the given property value. For unique indexes, this will
     * return the only path (if found). For non-unique indexes, this will return
     * only one path.
     *
     * @param propertyValue the value
     * @param revision the revision
     * @return the path, or null if not found
     */
    public String getPath(String propertyValue, String revision) {
        indexer.updateUntil(revision);
        Cursor c = tree.findFirst(propertyValue);
        if (!c.hasNext()) {
            return null;
        }
        String key = c.next();
        if (key.equals(propertyValue)) {
            return c.getValue();
        }
        return null;
    }

    /**
     * Get an iterator over the paths for the given property value. For unique
     * indexes, the iterator will contain at most one element.
     *
     * @param propertyValue the value, or null to return all indexed rows
     * @param revision the revision
     * @return an iterator of the paths (an empty iterator if not found)
     */
    @Override
    public Iterator<String> getPaths(String propertyValue, String revision) {
        indexer.updateUntil(revision);
        Cursor c = tree.findFirst(propertyValue);
        return new Cursor.RangeIterator(c, propertyValue);
    }

    @Override
    public NodeState processCommit(NodeStore store, NodeState before,
            NodeState after) throws CommitFailedException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void close() throws IOException {
        // not needed
    }

}
