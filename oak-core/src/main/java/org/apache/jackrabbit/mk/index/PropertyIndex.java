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
package org.apache.jackrabbit.mk.index;

import java.util.Iterator;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.apache.jackrabbit.mk.simple.NodeImpl;

/**
 * A node handler that maps the property value to the key, and the path of the
 * node to the value. Only string and numbers are indexes (arrays, true, false,
 * and null are not indexes).
 */
public class PropertyIndex implements Index {

    private final Indexer indexer;
    private final BTree tree;
    private final String propertyName;

    public PropertyIndex(Indexer indexer, String propertyName, boolean unique) {
        this.indexer = indexer;
        this.propertyName = propertyName;
        this.tree = new BTree(indexer, (unique ? "id:" : "property:") + propertyName, unique);
        tree.setMinSize(10);
    }

    public static PropertyIndex fromNodeName(Indexer indexer, String nodeName) {
        boolean unique;
        if (nodeName.startsWith("property:")) {
            unique = false;
        } else if (nodeName.startsWith("id:")) {
            unique = true;
        } else {
            return null;
        }
        int index = nodeName.indexOf(':');
        String propertyName = nodeName.substring(0, index);
        return new PropertyIndex(indexer, propertyName, unique);
    }

    @Override
    public String getName() {
        return tree.getName();
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
        if (t.matches(JsopTokenizer.STRING) || t.matches(JsopTokenizer.NUMBER)) {
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
    public boolean isUnique() {
        return tree.isUnique();
    }

}
