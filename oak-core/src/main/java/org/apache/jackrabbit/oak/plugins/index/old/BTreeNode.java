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
package org.apache.jackrabbit.oak.plugins.index.old;

import java.util.Arrays;

import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.oak.util.ArrayUtils;
import org.apache.jackrabbit.oak.commons.PathUtils;

/**
 * An index node page.
 */
public class BTreeNode extends BTreePage implements PropertyIndexConstants {

    private String[] children;

    public BTreeNode(BTree tree, BTreeNode parent, String name, String[] keys, String[] values, String[] children) {
        super(tree, parent, name, keys, values);
        this.children = children;
        verify();
    }

    String getNextChildPath() {
        int max = 0;
        for (String c : children) {
            int x = Integer.parseInt(c);
            if (x > max) {
                max = x;
            }
        }
        return Integer.toString(max + 1);
    }

    @Override
    BTreeLeaf firstLeaf() {
        return tree.getPage(this, children[0]).firstLeaf();
    }

    @Override
    void split(BTreeNode newParent, String newName, int pos, String siblingName) {
        setParent(newParent, newName, true);
        String[] k2 = Arrays.copyOfRange(keys, pos + 1, keys.length, String[].class);
        String[] v2 = Arrays.copyOfRange(values, pos + 1, values.length, String[].class);
        String[] c2 = Arrays.copyOfRange(children, pos + 1, children.length, String[].class);
        BTreeNode n2 = new BTreeNode(tree, parent, siblingName, k2, v2, c2);
        for (String c : c2) {
            BTreePage cp = tree.getPageIfCached(this, c);
            if (cp != null) {
                cp.setParent(n2, c, false);
            }
        }
        keys = Arrays.copyOfRange(keys, 0, pos, String[].class);
        values = Arrays.copyOfRange(values, 0, pos, String[].class);
        children = Arrays.copyOfRange(children, 0, pos + 1, String[].class);
        verify();
        n2.writeCreate();
        for (String c : n2.children) {
            tree.bufferMove(
                    PathUtils.concat(tree.getName(), INDEX_CONTENT, getPath(), c),
                    PathUtils.concat(tree.getName(), INDEX_CONTENT, getParentPath(), siblingName, c)
            );
        }
        tree.moveCache(getPath());
        writeData();
    }

    BTreeLeaf next(BTreePage child) {
        int i = 0;
        String childName = child.name;
        for (; i < children.length; i++) {
            if (children[i].equals(childName)) {
                break;
            }
        }
        if (i == children.length - 1) {
            return parent == null ? null : parent.next(this);
        }
        return tree.getPage(this, children[i + 1]).firstLeaf();
    }

    BTreePage getChild(int pos) {
        return tree.getPage(this, children[pos]);
    }

    void writeData() {
        verify();
        tree.modified(this);
        tree.bufferSetArray(getPath(), "keys", keys);
        tree.bufferSetArray(getPath(), "values", values);
        tree.bufferSetArray(getPath(), "children", children);
    }

    @Override
    void writeCreate() {
        verify();
        tree.modified(this);
        tree.buffer(getJsop());
    }

    void delete(int pos) {
        tree.modified(this);
        if (size() > 0) {
            // empty parent
            keys = ArrayUtils.arrayRemove(keys, Math.max(0, pos - 1));
            values = ArrayUtils.arrayRemove(values, Math.max(0, pos - 1));
        }
        children = ArrayUtils.arrayRemove(children, pos);
        verify();
    }

    void insert(int pos, String key, String value, String child) {
        tree.modified(this);
        keys = ArrayUtils.arrayInsert(keys, pos, key);
        values = ArrayUtils.arrayInsert(values, pos, value);
        children = ArrayUtils.arrayInsert(children, pos + 1, child);
        verify();
    }

    boolean isEmpty() {
        return children.length == 0;
    }

    private void verify() {
        if (values.length != keys.length) {
            throw new IllegalArgumentException(
                    "Number of values doesn't match number of keys: " +
                    Arrays.toString(values) + " " + Arrays.toString(keys) + " " + Arrays.toString(children));
        }
        if (children.length != keys.length + 1) {
            if (children.length != 0 || keys.length != 0) {
                throw new IllegalArgumentException(
                        "Number of children doesn't match number of keys + 1: " +
                        Arrays.toString(values) + " " + Arrays.toString(keys) + " " + Arrays.toString(children));
            }
        }
    }

    private String getJsop() {
        JsopBuilder jsop = new JsopBuilder();
        jsop.tag('+').key(PathUtils.concat(tree.getName(), INDEX_CONTENT, getPath())).object();
        jsop.key("keys").array();
        for (String k : keys) {
            jsop.value(k);
        }
        jsop.endArray();
        jsop.key("values").array();
        for (String v : values) {
            jsop.value(v);
        }
        jsop.endArray();
        // could just use child node list, but then
        // new children need to be ordered at the right position,
        // and we would need a way to distinguish empty lists
        // from a leaf
        jsop.key("children").array();
        for (String d : children) {
            jsop.value(d);
        }
        jsop.endArray();
        jsop.endObject();
        jsop.newline();
        return jsop.toString();
    }

    @Override
    public String toString() {
        return "node: " + getJsop();
    }

}