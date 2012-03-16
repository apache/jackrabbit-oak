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

import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.mk.json.JsopReader;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.apache.jackrabbit.mk.simple.NodeImpl;
import org.apache.jackrabbit.mk.simple.NodeMap;
import org.apache.jackrabbit.mk.util.ExceptionFactory;
import org.apache.jackrabbit.mk.util.PathUtils;
import org.apache.jackrabbit.mk.util.SimpleLRUCache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

/**
 * A index mechanism. An index is bound to a certain repository, and supports
 * one or more indexes.
 */
public class Indexer {

    private static final boolean DISABLED = Boolean.getBoolean("mk.indexDisabled");

    private MicroKernel mk;
    private String revision;
    private String indexRootNode;
    private StringBuilder buffer;
    private HashMap<String, Index> indexes = new HashMap<String, Index>();
    private HashMap<String, BTreePage> modified = new HashMap<String, BTreePage>();
    private SimpleLRUCache<String, BTreePage> cache = SimpleLRUCache.newInstance(100);
    private String readRevision;

    public Indexer(MicroKernel mk, String indexRootNode) {
        this.mk = mk;
        if (!PathUtils.isAbsolute(indexRootNode)) {
            indexRootNode = "/" + indexRootNode;
        }
        this.indexRootNode = indexRootNode;
        revision = mk.getHeadRevision();
        readRevision = revision;
        if (!mk.nodeExists(indexRootNode, revision)) {
            JsopBuilder jsop = new JsopBuilder();
            jsop.tag('+').key(PathUtils.relativize("/", indexRootNode)).object().endObject();
            revision = mk.commit("/", jsop.toString(), revision, null);
        } else {
            String node = mk.getNodes(indexRootNode, revision, 0, 0, Integer.MAX_VALUE, null);
            JsopTokenizer t = new JsopTokenizer(node);
            t.read('{');
            HashMap<String, String> map = new HashMap<String, String>();
            do {
                String key = t.readString();
                t.read(':');
                t.read();
                String value = t.getToken();
                map.put(key, value);
            } while (t.matches(','));
            String rev = map.get("rev");
            if (rev != null) {
                readRevision = rev;
            }
            for (String k : map.keySet()) {
                Index p = PropertyIndex.fromNodeName(this, k);
                if (p == null) {
                    p = PrefixIndex.fromNodeName(this, k);
                }
                if (p != null) {
                    indexes.put(p.getName(), p);
                }
            }
        }
    }

    public Indexer(MicroKernel mk) {
        this(mk, "/index");
    }

    public PropertyIndex createPropertyIndex(String property, boolean unique) {
        PropertyIndex index = new PropertyIndex(this, property, unique);
        PropertyIndex existing = (PropertyIndex) indexes.get(index.getName());
        if (existing != null) {
            return existing;
        }
        buildAndAddIndex(index);
        return index;
    }

    public PrefixIndex createPrefixIndex(String prefix) {
        PrefixIndex index = new PrefixIndex(this, prefix);
        PrefixIndex existing = (PrefixIndex) indexes.get(index.getName());
        if (existing != null) {
            return existing;
        }
        buildAndAddIndex(index);
        return index;
    }

    boolean nodeExists(String name) {
        revision = mk.getHeadRevision();
        return mk.nodeExists(PathUtils.concat(indexRootNode, name), revision);
    }

    void commit(String jsop) {
        revision = mk.commit(indexRootNode, jsop, revision, null);
    }

    BTreePage getPageIfCached(BTree tree, BTreeNode parent, String name) {
        String p = getPath(tree, parent, name);
        return modified.get(p);
    }

    private String getPath(BTree tree, BTreeNode parent, String name) {
        String p = parent == null ? name : PathUtils.concat(parent.getPath(), name);
        String indexRoot = PathUtils.concat(indexRootNode, tree.getName());
        return PathUtils.concat(indexRoot, p);
    }

    BTreePage getPage(BTree tree, BTreeNode parent, String name) {
        String p = getPath(tree, parent, name);
        BTreePage page;
        page = modified.get(p);
        if (page != null) {
            return page;
        }
        String cacheId = p + "@" + revision;
        page = cache.get(cacheId);
        if (page != null) {
            return page;
        }
        String json = mk.getNodes(p, revision, 0, 0, 0, null);
        if (json == null) {
            page = new BTreeLeaf(tree, parent, name,
                    new String[0], new String[0]);
        } else {
            NodeImpl n = NodeImpl.parse(json);
            String keys = n.getProperty("keys");
            String values = n.getProperty("values");
            String children = n.getProperty("children");
            if (children != null) {
                BTreeNode node = new BTreeNode(tree, parent, name,
                        readArray(keys), readArray(values), readArray(children));
                page = node;
            } else {
                BTreeLeaf leaf = new BTreeLeaf(tree, parent, name,
                        readArray(keys), readArray(values));
                page = leaf;
            }
        }
        cache.put(cacheId, page);
        return page;
    }

    private static String[] readArray(String json) {
        if (json == null) {
            return new String[0];
        }
        ArrayList<String> dataList = new ArrayList<String>();
        JsopTokenizer t = new JsopTokenizer(json);
        t.read('[');
        if (!t.matches(']')) {
            do {
                dataList.add(t.readString());
            } while (t.matches(','));
            t.read(']');
        }
        String[] data = new String[dataList.size()];
        dataList.toArray(data);
        return data;
    }

    void buffer(String diff) {
        if (buffer == null) {
            buffer = new StringBuilder(diff.length());
        }
        buffer.append(diff);
    }

    void commit() {
        // TODO remove this method once MicroKernelImpl supports
        // move + add node
        if (mk instanceof MicroKernelImpl) {
            commitChanges();
        }
    }

    void modified(BTree tree, BTreePage page, boolean deleted) {
        String indexRoot = PathUtils.concat(indexRootNode, tree.getName());
        String p = PathUtils.concat(indexRoot, page.getPath());
        if (deleted) {
            modified.remove(p);
        } else {
            modified.put(p, page);
        }
    }

    public void moveCache(BTree tree, String oldPath) {
        String indexRoot = PathUtils.concat(indexRootNode, tree.getName());
        String o = PathUtils.concat(indexRoot, oldPath);
        HashMap<String, BTreePage> moved = new HashMap<String, BTreePage>();
        for (Entry<String, BTreePage> e : modified.entrySet()) {
            if (e.getKey().startsWith(o)) {
                moved.put(e.getKey(), e.getValue());
            }
        }
        for (String s : moved.keySet()) {
            modified.remove(s);
        }
        for (BTreePage p : moved.values()) {
            String n = PathUtils.concat(indexRoot, p.getPath());
            modified.put(n, p);
        }
    }

    void commitChanges() {
        if (buffer != null) {
            String jsop = buffer.toString();
            // System.out.println(jsop);
            revision = mk.commit(indexRootNode, jsop, revision, null);
            buffer = null;
            modified.clear();
        }
    }

    synchronized void updateUntil(String toRevision) {
        if (DISABLED) {
            return;
        }
        if (toRevision.equals(readRevision)) {
            return;
        } else {
            toRevision = mk.getHeadRevision();
        }
        String journal = mk.getJournal(readRevision, toRevision, null);
        JsopTokenizer t = new JsopTokenizer(journal);
        String lastRevision = readRevision;
        t.read('[');
        if (t.matches(']')) {
            readRevision = toRevision;
            // nothing to update
            return;
        }

        HashMap<String, String> map = new HashMap<String, String>();
        do {
            map.clear();
            t.read('{');
            do {
                String key = t.readString();
                t.read(':');
                t.read();
                String value = t.getToken();
                map.put(key, value);
            } while (t.matches(','));
            String rev = map.get("id");
            if (!rev.equals(readRevision)) {
                String jsop = map.get("changes");
                JsopTokenizer tokenizer = new JsopTokenizer(jsop);
                updateIndex("", tokenizer, lastRevision);
            }
            lastRevision = rev;
            t.read('}');
        } while (t.matches(','));
        updateEnd(toRevision);
    }

    /**
     * Finish updating the index.
     *
     * @param toRevision the new index revision
     * @return the new head revision
     */
    public String updateEnd(String toRevision) {
        readRevision = toRevision;
        JsopBuilder jsop = new JsopBuilder();
        jsop.tag('^').key("rev").value(readRevision);
        buffer(jsop.toString());
        commitChanges();
        return revision;
    }

    /**
     * Update the index with the given changes.
     *
     * @param t the changes
     * @param lastRevision
     */
    public void updateIndex(String rootPath, JsopReader t, String lastRevision) {
        while (true) {
            int r = t.read();
            if (r == JsopTokenizer.END) {
                break;
            }
            String path = PathUtils.concat(rootPath, t.readString());
            switch (r) {
            case '+': {
                t.read(':');
                NodeMap map = new NodeMap();
                if (t.matches('{')) {
                    NodeImpl n = NodeImpl.parse(map, t, 0, path);
                    addOrRemoveRecursive(n, false, true);
                } else {
                    String value = t.readRawValue().trim();
                    String nodePath = PathUtils.getParentPath(path);
                    NodeImpl node = new NodeImpl(map, 0);
                    node.setPath(nodePath);
                    String propertyName = PathUtils.getName(path);
                    node.cloneAndSetProperty(propertyName, value, 0);
                    addOrRemoveRecursive(node, true, true);
                }
                break;
            }
            case '-':
                moveNode(path, null, lastRevision);
                break;
            case '^': {
                removeProperty(path, lastRevision);
                t.read(':');
                if (t.matches(JsopTokenizer.NULL)) {
                    // ignore
                } else {
                    String value = t.readRawValue().trim();
                    addProperty(path, value);
                }
                break;
            }
            case '>':
                t.read(':');
                String name = PathUtils.getName(path);
                String target, position;
                if (t.matches('{')) {
                    position = t.readString();
                    t.read(':');
                    target = t.readString();
                    t.read('}');
                } else {
                    position = null;
                    target = t.readString();
                }
                if ("last".equals(position) || "first".equals(position)) {
                    target = PathUtils.concat(target, name);
                } else if ("before".equals(position) || "after".equals(position)) {
                    target = PathUtils.getParentPath(target);
                    target = PathUtils.concat(target, name);
                } else if (position == null) {
                    // move
                } else {
                    throw ExceptionFactory.get("position: " + position);
                }
                moveNode(path, target, lastRevision);
                break;
            default:
                throw new AssertionError("token: " + (char) t.getTokenType());
            }
        }
    }

    private void addOrRemoveRecursive(NodeImpl n, boolean remove, boolean add) {
        if (isInIndex(n.getPath())) {
            // don't index the index
            return;
        }
        for (Index index : indexes.values()) {
            if (remove) {
                index.addOrRemoveNode(n, false);
            }
            if (add) {
                index.addOrRemoveNode(n, true);
            }
        }
        for (Iterator<String> it = n.getChildNodeNames(Integer.MAX_VALUE); it.hasNext();) {
            addOrRemoveRecursive(n.getNode(it.next()), remove, add);
        }
    }

    private boolean isInIndex(String path) {
        return PathUtils.isAncestor(indexRootNode, path) || indexRootNode.equals(path);
    }

    private void removeProperty(String path, String lastRevision) {
        if (isInIndex(path)) {
            // don't index the index
            return;
        }
        String nodePath = PathUtils.getParentPath(path);
        String property = PathUtils.getName(path);
        if (!mk.nodeExists(nodePath, lastRevision)) {
            return;
        }
        // TODO remove: support large trees
        String node = mk.getNodes(nodePath, lastRevision, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, null);
        JsopTokenizer t = new JsopTokenizer(node);
        NodeMap map = new NodeMap();
        t.read('{');
        NodeImpl n = NodeImpl.parse(map, t, 0, path);
        if (n.hasProperty(property)) {
            n.setPath(nodePath);
            for (Index index : indexes.values()) {
                index.addOrRemoveProperty(nodePath, property, n.getProperty(property), false);
            }
        }
    }

    private void addProperty(String path, String value) {
        if (isInIndex(path)) {
            // don't index the index
            return;
        }
        String nodePath = PathUtils.getParentPath(path);
        String property = PathUtils.getName(path);
        for (Index index : indexes.values()) {
            index.addOrRemoveProperty(nodePath, property, value, true);
        }
    }

    private void moveNode(String sourcePath, String targetPath, String lastRevision) {
        if (isInIndex(sourcePath)) {
            // don't index the index
            return;
        }
        if (!mk.nodeExists(sourcePath, lastRevision)) {
            return;
        }
        // TODO move: support large trees
        String node = mk.getNodes(sourcePath, lastRevision, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, null);
        JsopTokenizer t = new JsopTokenizer(node);
        NodeMap map = new NodeMap();
        t.read('{');
        NodeImpl n = NodeImpl.parse(map, t, 0, sourcePath);
        addOrRemoveRecursive(n, true, false);
        if (targetPath != null) {
            t = new JsopTokenizer(node);
            map = new NodeMap();
            t.read('{');
            n = NodeImpl.parse(map, t, 0, targetPath);
            addOrRemoveRecursive(n, false, true);
        }
    }

    private void buildAndAddIndex(Index index) {
        addRecursive(index, "/");
        indexes.put(index.getName(), index);
    }

    private void addRecursive(Index index, String path) {
        if (isInIndex(path)) {
            return;
        }
        // TODO add: support large child node lists
        String node = mk.getNodes(path, readRevision, 0, 0, Integer.MAX_VALUE, null);
        JsopTokenizer t = new JsopTokenizer(node);
        NodeMap map = new NodeMap();
        t.read('{');
        NodeImpl n = NodeImpl.parse(map, t, 0, path);
        index.addOrRemoveNode(n, true);
        for (Iterator<String> it = n.getChildNodeNames(Integer.MAX_VALUE); it.hasNext();) {
            addRecursive(index, PathUtils.concat(path, it.next()));
        }
    }

}
