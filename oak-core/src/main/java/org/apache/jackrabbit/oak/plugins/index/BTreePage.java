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

import org.apache.jackrabbit.oak.commons.PathUtils;

/**
 * An index page.
 */
abstract public class BTreePage {

    protected final BTree tree;
    protected BTreeNode parent;
    protected String name;
    protected String[] keys;
    protected String[] values;

    BTreePage(BTree tree, BTreeNode parent, String name, String[] keys, String[] values) {
        this.tree = tree;
        this.parent = parent;
        this.name = name;
        this.keys = keys;
        this.values = values;
    }

    abstract void writeCreate();
    abstract void split(BTreeNode newParent, String newPath, int pos, String siblingPath);
    abstract BTreeLeaf firstLeaf();

    void setParent(BTreeNode newParent, String newName, boolean parentIsNew) {
        if (newParent != null) {
            String oldPath = getPath();
            String temp = PathUtils.concat(Indexer.INDEX_CONTENT, "temp");
            tree.bufferMove(
                    PathUtils.concat(tree.getName(), Indexer.INDEX_CONTENT, getPath()),
                    temp);
            if (parentIsNew) {
                newParent.writeCreate();
            }
            tree.bufferMove(
                    temp,
                    PathUtils.concat(tree.getName(), Indexer.INDEX_CONTENT, getParentPath(), newName));
            parent = newParent;
            name = newName;
            tree.moveCache(oldPath);
            tree.modified(this);
        }
    }

    String getParentPath() {
        return parent == null ? "" : parent.getPath();
    }

    public String getPath() {
        return PathUtils.concat(getParentPath(), name);
    }

    int size() {
        return keys.length;
    }

    int find(String key, String value) {
        return tree.find(key, value, keys, values);
    }

}
