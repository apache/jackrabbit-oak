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

import java.util.Iterator;

import org.apache.jackrabbit.mk.json.JsopReader;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.apache.jackrabbit.mk.simple.NodeImpl;

/**
 * A node handler that maps the property value to the key, and the path of the
 * node to the value. Only string and numbers are indexes (arrays, true, false,
 * and null are not indexes).
 */
public class PropertyIndex implements PIndex, PropertyIndexConstants {

    private final BTreeHelper indexer;
    private final BTree tree;
    private final String propertyName;

    public PropertyIndex(BTreeHelper indexer, String propertyName, boolean unique) {
        this.indexer = indexer;
        this.propertyName = propertyName;
        this.tree = new BTree(indexer, TYPE_PROPERTY + propertyName 
                + (unique ? "," + UNIQUE : ""), unique);
        tree.setMinSize(10);
    }

    public static PropertyIndex fromNodeName(BTreeHelper indexer, String nodeName) {
        if (!nodeName.startsWith(TYPE_PROPERTY)) {
            return null;
        }
        boolean unique = false;
        if (nodeName.endsWith(UNIQUE)) {
            unique = true;
            nodeName = nodeName.substring(0, nodeName.length() - UNIQUE.length() - 1);
        }
        String property = nodeName.substring(TYPE_PROPERTY.length());
        return new PropertyIndex(indexer, property, unique);
    }

    public String getPropertyName() {
        return propertyName;
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
    public String getIndexNodeName() {
        return tree.getName();
    }

    @Override
    public boolean isUnique() {
        return tree.isUnique();
    }

}
