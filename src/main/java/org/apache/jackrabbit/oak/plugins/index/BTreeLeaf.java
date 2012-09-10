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

import java.util.Arrays;

import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.util.ArrayUtils;

/**
 * An index leaf page.
 */
public class BTreeLeaf extends BTreePage implements PropertyIndexConstants {

    public BTreeLeaf(BTree tree, BTreeNode parent, String name, String[] data, String[] paths) {
        super(tree, parent, name, data, paths);
        verify();
    }

    BTreeLeaf nextLeaf() {
        return parent == null ? null : parent.next(this);
    }

    @Override
    BTreeLeaf firstLeaf() {
        return this;
    }

    @Override
    void split(BTreeNode newParent, String newName, int pos, String siblingName) {
        setParent(newParent, newName, true);
        String[] k2 = Arrays.copyOfRange(keys, pos, keys.length, String[].class);
        String[] v2 = Arrays.copyOfRange(values, pos, values.length, String[].class);
        BTreeLeaf n2 = new BTreeLeaf(tree, parent, siblingName, k2, v2);
        keys = Arrays.copyOfRange(keys, 0, pos, String[].class);
        values = Arrays.copyOfRange(values, 0, pos, String[].class);
        writeData();
        n2.writeCreate();
    }

    void insert(int pos, String key, String value) {
        tree.modified(this);
        keys = ArrayUtils.arrayInsert(keys, pos, key);
        values = ArrayUtils.arrayInsert(values, pos, value);
        verify();
    }

    void delete(int pos) {
        tree.modified(this);
        keys = ArrayUtils.arrayRemove(keys, pos);
        values = ArrayUtils.arrayRemove(values, pos);
        verify();
    }

    void writeData() {
        verify();
        tree.modified(this);
        tree.bufferSetArray(getPath(), "children", null);
        tree.bufferSetArray(getPath(), "keys", keys);
        tree.bufferSetArray(getPath(), "values", values);
    }

    @Override
    void writeCreate() {
        verify();
        tree.modified(this);
        tree.buffer(getJsop());
    }

    private void verify() {
        if (values.length != keys.length) {
            throw new IllegalArgumentException(
                    "Number of values doesn't match number of keys: " +
                    Arrays.toString(values) + " " + Arrays.toString(keys));
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
        jsop.endObject();
        jsop.newline();
        return jsop.toString();
    }

    @Override
    public String toString() {
        return "leaf: " + getJsop();
    }

}