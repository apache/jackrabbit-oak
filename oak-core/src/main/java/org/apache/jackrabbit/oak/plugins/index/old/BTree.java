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

import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.oak.commons.PathUtils;

/**
 * A tree allows to query a value for a given key, similar to
 * {@code java.util.SortedMap}.
 */
public class BTree implements PropertyIndexConstants {

    private static final int DEFAULT_MIN_SIZE = 2;

    private BTreeHelper indexer;

    private String name;
    private boolean unique = true;
    private int minSize = DEFAULT_MIN_SIZE;

    public BTree(BTreeHelper indexer, String name, boolean unique) {
        this.indexer = indexer;
        this.name = name;
        this.unique = unique;
        indexer.createNodes(PathUtils.concat(name, INDEX_CONTENT));
    }

    public void setMinSize(int minSize) {
        if (minSize < 2) {
            throw new IllegalArgumentException("minSize: " + minSize);
        }
        this.minSize = minSize;
    }

    /**
     * Find the given key or key/value pair in the page.
     *
     * @param key the key (required)
     * @param value the value (optional)
     * @param keys the key list
     * @param values the value list
     * @return the position
     */
    int find(String key, String value, String[] keys, String[] values) {
        // modified binary search:
        // return the first elements for non-unique
        // indexes, if multiple elements were found
        int min = 0, max = keys.length - 1;
        while (min <= max) {
            int test = (min + max) >>> 1;
            int compare = key == null ? 1 : keys[test].compareTo(key);
            if (compare == 0) {
                if (unique) {
                    return test;
                }
                if (value != null) {
                    compare = values[test].compareTo(value);
                } else {
                    // consider equality as bigger, so
                    // that the first element is found
                    compare = 1;
                }
            }
            if (compare > 0) {
                max = test - 1;
            } else if (compare < 0) {
                min = test + 1;
            } else {
                return test;
            }
        }
        // not found: return negative insertion point
        return -(min + 1);
    }

    BTreePage getPageIfCached(BTreeNode parent, String name) {
        return indexer.getPageIfCached(this, parent, name);
    }

    BTreePage getPage(BTreeNode parent, String name) {
        return indexer.getPage(this, parent, name);
    }

    public Cursor findFirst(String key) {
        Cursor c = new Cursor();
        BTreePage node = getPage(null, "");
        while (true) {
            int pos = node.find(key, null);
            if (node instanceof BTreeLeaf) {
                if (pos < 0) {
                    pos = -pos - 1;
                }
                c.setCurrent((BTreeLeaf) node, pos);
                if (node.size() == 0 || pos >= node.size()) {
                    c.step();
                }
                break;
            }
            if (pos < 0) {
                pos = -pos - 1;
            } else {
                pos++;
            }
            node = ((BTreeNode) node).getChild(pos);
        }
        return c;
    }

    void bufferSetArray(String path, String propertyName, String[] data) {
        JsopBuilder jsop = new JsopBuilder();
        path = PathUtils.concat(name, INDEX_CONTENT, path);
        jsop.tag('^').key(PathUtils.concat(path, propertyName));
        if (data == null) {
            jsop.value(null);
        } else {
            jsop.array();
            for (String d : data) {
                jsop.value(d);
            }
            jsop.endArray();
        }
        jsop.newline();
        indexer.buffer(jsop.toString());
    }

    void bufferMove(String path, String newPath) {
        JsopBuilder jsop = new JsopBuilder();
        jsop.tag('>').key(path).value(newPath);
        jsop.newline();
        indexer.buffer(jsop.toString());
    }

    void bufferDelete(String path) {
        JsopBuilder jsop = new JsopBuilder();
        jsop.tag('-').value(PathUtils.concat(name, INDEX_CONTENT, path));
        jsop.newline();
        indexer.buffer(jsop.toString());
    }

    void buffer(String jsop) {
        indexer.buffer(jsop);
    }

    void modified(BTreePage page) {
        indexer.modified(this, page, false);
    }

    void moveCache(String oldPath) {
        indexer.moveCache(this, oldPath);
    }

    public boolean remove(String key, String value) {
        BTreeNode parent = null;
        int parentPos = 0;
        BTreePage n = getPage(null, "");
        while (true) {
            if (n instanceof BTreeNode) {
                BTreeNode page = (BTreeNode) n;
                int pos = page.find(key, value);
                if (pos < 0) {
                    pos = -pos - 1;
                } else {
                    pos++;
                }
                parent = page;
                parentPos = pos;
                n = page.getChild(pos);
            } else {
                BTreeLeaf page = (BTreeLeaf) n;
                int pos = page.find(key, value);
                if (pos < 0) {
                    return false;
                }
                page.delete(pos);
                if (n.size() == 0 && parent != null) {
                    bufferDelete(page.getPath());
                    indexer.modified(this, page, true);
                    // empty leaf with a parent
                    parent.delete(parentPos);
                    parent.writeData();
                    if (parent.isEmpty()) {
                        // empty node becomes a empty leaf
                        BTreeLeaf p = new BTreeLeaf(this, parent.parent, parent.name, new String[0], new String[0]);
                        modified(p);
                    }
                } else {
                    page.writeData();
                }
                break;
            }
        }
        return true;
    }

    public void add(String key, String value) {
        BTreeNode parent = null;
        int parentPos = 0;
        BTreePage n = getPage(null, "");
        while (true) {
            if (n.size() >= getMaxSize()) {
                // split
                int split = getMinSize();
                if (parent == null) {
                    // new root
                    BTreeNode root = new BTreeNode(this, null, "",
                            new String[] { n.keys[split] },
                            new String[] { n.values[split] },
                            new String[] { "0", "1" });
                    modified(root);
                    n.split(root, "0", split, "1");
                    n = root;
                } else {
                    String k = n.keys[split];
                    String v = n.values[split];
                    String path = parent.getNextChildPath();
                    n.split(null, null, split, path);
                    parent.insert(parentPos, k, v, PathUtils.getName(path));
                    parent.writeData();
                    // go back one step (the entry might be in the
                    // other node now)
                    n = parent;
                }
            }
            if (n instanceof BTreeNode) {
                BTreeNode page = (BTreeNode) n;
                int pos = page.find(key, value);
                if (pos < 0) {
                    pos = -pos - 1;
                } else {
                    pos++;
                }
                parent = page;
                parentPos = pos;
                n = page.getChild(pos);
            } else {
                BTreeLeaf page = (BTreeLeaf) n;
                int pos = page.find(key, value);
                if (pos < 0) {
                    pos = -pos - 1;
                } else {
                    if (unique) {
                        throw new UnsupportedOperationException("Unique key violation");
                    }
                }
                page.insert(pos, key, value);
                page.writeData();
                break;
            }
        }
    }

    public String getName() {
        return name;
    }

    private int getMinSize() {
        return minSize;
    }

    private int getMaxSize() {
        return minSize + minSize + 1;
    }

    boolean isUnique() {
        return unique;
    }

}
