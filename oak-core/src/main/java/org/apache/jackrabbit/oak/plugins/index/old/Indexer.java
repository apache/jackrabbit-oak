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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.mk.json.JsopReader;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.apache.jackrabbit.mk.util.SimpleLRUCache;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.old.mk.ExceptionFactory;
import org.apache.jackrabbit.oak.plugins.index.old.mk.IndexWrapper;
import org.apache.jackrabbit.oak.plugins.index.old.mk.simple.NodeImpl;
import org.apache.jackrabbit.oak.plugins.index.old.mk.simple.NodeMap;

/**
 * A index mechanism. An index is bound to a certain repository, and supports
 * one or more indexes.
 * 
 * @deprecated use {@link PropertyIndexer} - see OAK-298
 */
public class Indexer implements PropertyIndexConstants, BTreeHelper {

    /**
     * The maximum length of the write buffer.
     */
    private static final int MAX_BUFFER_LENGTH = 100000;

    private static final boolean DISABLED = Boolean.getBoolean("mk.indexDisabled");

    private MicroKernel mk;
    private String revision;
    private final String indexRootNode;
    private final int indexRootNodeDepth;
    private StringBuilder buffer;
    private HashMap<String, BTreePage> modified = new HashMap<String, BTreePage>();
    private SimpleLRUCache<String, BTreePage> cache = SimpleLRUCache.newInstance(100);
    private String readRevision;
    private boolean init;

    /**
     * An index node name to index map.
     */
    private HashMap<String, PIndex> indexes = new HashMap<String, PIndex>();

    /**
     * A prefix to prefix index map.
     */
    private final HashMap<String, PrefixIndex> prefixIndexes = new HashMap<String, PrefixIndex>();

    /**
     * A property name to property index map.
     */
    private final HashMap<String, PropertyIndex> propertyIndexes = new HashMap<String, PropertyIndex>();

    public Indexer(MicroKernel mk, String indexConfigPath) {
        this.mk = mk;
        this.indexRootNode = indexConfigPath;
        this.indexRootNodeDepth = PathUtils.getDepth(indexRootNode);
    }

    /**
     * TODO test-only
     */
    public Indexer(MicroKernel mk) {
        this(mk, INDEX_CONFIG_PATH);
    }

    /**
     * Get the indexer for the given MicroKernel. This will either create a new instance,
     * or (if the MicroKernel is an IndexWrapper), return the existing indexer.
     *
     * @param mk the MicroKernel instance
     * @return an indexer
     */
    public static Indexer getInstance(MicroKernel mk) {
        if (mk instanceof IndexWrapper) {
            Indexer indexer = ((IndexWrapper) mk).getIndexer();
            if (indexer != null) {
                return indexer;
            }
        }
        return new Indexer(mk);
    }

    public String getIndexRootNode() {
        return indexRootNode;
    }

    public void init() {
        if (init) {
            return;
        }
        init = true;
        revision = mk.getHeadRevision();
        readRevision = revision;
        boolean exists = mk.nodeExists(indexRootNode, revision);
        createNodes(INDEX_CONTENT);
            if (exists) {
            String node = mk.getNodes(indexRootNode, revision, 1, 0, Integer.MAX_VALUE, null);
            JsopTokenizer t = new JsopTokenizer(node);
            NodeMap map = new NodeMap();
            t.read('{');
            NodeImpl n = NodeImpl.parse(map, t, 0);
            String rev = JsopTokenizer.decodeQuoted(n.getNode(INDEX_CONTENT).getProperty("rev"));
            if (rev != null) {
                readRevision = rev;
            }
            for (int i = 0; i < n.getChildNodeCount(); i++) {
                String k = n.getChildNodeName(i);
                PropertyIndex prop = PropertyIndex.fromNodeName(this, k);
                if (prop != null) {
                    indexes.put(prop.getIndexNodeName(), prop);
                    propertyIndexes.put(prop.getPropertyName(), prop);
                }
                PrefixIndex pref = PrefixIndex.fromNodeName(this, k);
                if (pref != null) {
                    indexes.put(pref.getIndexNodeName(), pref);
                    prefixIndexes.put(pref.getPrefix(), pref);
                }
            }
        }
    }

    private void removePropertyIndex(String property, boolean unique) {
        PropertyIndex index = propertyIndexes.remove(property);
        indexes.remove(index.getIndexNodeName());
    }

    public PropertyIndex createPropertyIndex(String property, boolean unique) {
        PropertyIndex existing = propertyIndexes.get(property);
        if (existing != null) {
            return existing;
        }
        PropertyIndex index = new PropertyIndex(this, property, unique);
        buildIndex(index);
        indexes.put(index.getIndexNodeName(), index);
        propertyIndexes.put(index.getPropertyName(), index);
        return index;
    }

    private void removePrefixIndex(String prefix) {
         PrefixIndex index = prefixIndexes.remove(prefix);
         indexes.remove(index.getIndexNodeName());
    }

    public PrefixIndex createPrefixIndex(String prefix) {
        PrefixIndex existing = prefixIndexes.get(prefix);
        if (existing != null) {
            return existing;
        }
        PrefixIndex index = new PrefixIndex(this, prefix);
        buildIndex(index);
        indexes.put(index.getIndexNodeName(), index);
        prefixIndexes.put(index.getPrefix(), index);
        return index;
    }

    boolean nodeExists(String name) {
        revision = mk.getHeadRevision();
        return mk.nodeExists(PathUtils.concat(indexRootNode, name), revision);
    }

    public void createNodes(String path) {
        String rev = mk.getHeadRevision();
        JsopBuilder jsop = new JsopBuilder();
        String p = "/";
        path = PathUtils.concat(indexRootNode, path);
        for (String e : PathUtils.elements(path)) {
            p = PathUtils.concat(p, e);
            if (!mk.nodeExists(p, rev)) {
                jsop.tag('+').key(PathUtils.relativize("/", p)).
                    object().endObject().newline();
            }
        }
        revision = mk.commit("/", jsop.toString(), rev, null);
    }

    void commit(String jsop) {
        revision = mk.commit(indexRootNode, jsop, revision, null);
    }

    public BTreePage getPageIfCached(BTree tree, BTreeNode parent, String name) {
        String p = getPath(tree, parent, name);
        return modified.get(p);
    }

    private String getPath(BTree tree, BTreeNode parent, String name) {
        String p = parent == null ? name : PathUtils.concat(parent.getPath(), name);
        String indexRoot = PathUtils.concat(indexRootNode, tree.getName(), INDEX_CONTENT);
        return PathUtils.concat(indexRoot, p);
    }

    public BTreePage getPage(BTree tree, BTreeNode parent, String name) {
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

    public void buffer(String diff) {
        if (buffer == null) {
            buffer = new StringBuilder(diff);
        } else {
            buffer.append(diff);
        }
    }

    public void modified(BTree tree, BTreePage page, boolean deleted) {
        String indexRoot = PathUtils.concat(indexRootNode, tree.getName());
        String p = PathUtils.concat(indexRoot, INDEX_CONTENT, page.getPath());
        if (deleted) {
            modified.remove(p);
        } else {
            modified.put(p, page);
        }
    }

    public void moveCache(BTree tree, String oldPath) {
        String indexRoot = PathUtils.concat(indexRootNode, tree.getName());
        String o = PathUtils.concat(indexRoot, INDEX_CONTENT, oldPath);
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
            String n = PathUtils.concat(indexRoot, INDEX_CONTENT, p.getPath());
            modified.put(n, p);
        }
    }

    private void commitChanges() {
        if (buffer != null) {
            String jsop = buffer.toString();
            // System.out.println(jsop);
            revision = mk.commit(indexRootNode, jsop, revision, null);
            buffer = null;
            modified.clear();
        }
    }

    public synchronized void updateUntil(String toRevision) {
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
            if (buffer != null && buffer.length() > MAX_BUFFER_LENGTH) {
                updateEnd(rev);
            }
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
        jsop.tag('^').key(PathUtils.concat(INDEX_CONTENT, "rev")).value(readRevision);
        buffer(jsop.toString());
        flushBuffer();
        return revision;
    }

    private void flushBuffer() {
        try {
            commitChanges();
        } catch (MicroKernelException e) {
            if (!mk.nodeExists(indexRootNode, revision)) {
                // the index node itself was removed, which is
                // unexpected but possible
                // this will cause all indexes to be removed, so
                // it can be ignored here
            } else {
                throw e;
            }
        }
    }

    /**
     * Update the index with the given changes.
     *
     * @param rootPath the root path
     * @param t the changes
     * @param lastRevision
     */
    public void updateIndex(String rootPath, JsopReader t, String lastRevision) {
        while (true) {
            int r = t.read();
            if (r == JsopReader.END) {
                break;
            }
            String path = PathUtils.concat(rootPath, t.readString());
            String target;
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
            case '*':
                // TODO support and test copy operation ("*"),
                // specially in combination with other operations
                // possibly split up the commit in this case
                t.read(':');
                target = t.readString();
                moveOrCopyNode(path, false, target, lastRevision);
                break;
            case '-':
                moveOrCopyNode(path, true, null, lastRevision);
                break;
            case '^': {
                removeProperty(path, lastRevision);
                t.read(':');
                if (t.matches(JsopReader.NULL)) {
                    // ignore
                } else {
                    String value = t.readRawValue().trim();
                    addProperty(path, value);
                }
                break;
            }
            case '>':
                // TODO does move work correctly
                // in combination with other operations?
                // possibly split up the commit in this case
                t.read(':');
                String name = PathUtils.getName(path);
                String position;
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
                moveOrCopyNode(path, true, target, lastRevision);
                break;
            default:
                throw new AssertionError("token: " + (char) t.getTokenType());
            }
        }
    }

    private void addOrRemoveRecursive(NodeImpl n, boolean remove, boolean add) {
        String path = n.getPath();
        if (isInIndex(path)) {
            addOrRemoveIndex(path, remove, add);
            // don't index the index data itself
            return;
        }
        for (PIndex index : indexes.values()) {
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

    private void addOrRemoveIndex(String path, boolean remove, boolean add) {
        // check the depth first for speed

        // TODO allow creating multiple indexes in one step
        // (buffer indexes to be created; traverse the repository only once)
        // TODO allow filters (only index a certain path; exclude a list of paths)

        if (PathUtils.getDepth(path) == indexRootNodeDepth + 1) {
            // actually not required, just to make sure
            if (PathUtils.getParentPath(path).equals(indexRootNode)) {
                String name = PathUtils.getName(path);
                if (name.startsWith(PropertyIndexConstants.TYPE_PREFIX)) {
                    String prefix = name.substring(PropertyIndexConstants.TYPE_PREFIX.length());
                    if (remove) {
                        removePrefixIndex(prefix);
                    }
                    if (add) {
                        createPrefixIndex(prefix);
                    }
                } else if (name.startsWith(PropertyIndexConstants.TYPE_PROPERTY)) {
                    String property = name.substring(PropertyIndexConstants.TYPE_PROPERTY.length());
                    boolean unique = false;
                    if (property.endsWith("," + PropertyIndexConstants.UNIQUE)) {
                        unique = true;
                        property = property.substring(0, property.length() - PropertyIndexConstants.UNIQUE.length() - 1);
                    }
                    if (remove) {
                        removePropertyIndex(property, unique);
                    }
                    if (add) {
                        createPropertyIndex(property, unique);
                    }
                }
            }
        }
    }

    private boolean isInIndex(String path) {
        if (PathUtils.isAncestor(indexRootNode, path) || indexRootNode.equals(path)) {
            return true;
        }
        return false;
    }

    private void removeProperty(String path, String lastRevision) {
        if (isInIndex(path)) {
            // don't index the index data itself
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
            for (PIndex index : indexes.values()) {
                index.addOrRemoveProperty(nodePath, property, n.getProperty(property), false);
            }
        }
    }

    private void addProperty(String path, String value) {
        if (isInIndex(path)) {
            // don't index the index data itself
            return;
        }
        String nodePath = PathUtils.getParentPath(path);
        String property = PathUtils.getName(path);
        for (PIndex index : indexes.values()) {
            index.addOrRemoveProperty(nodePath, property, value, true);
        }
    }

    private void moveOrCopyNode(String sourcePath, boolean remove, String targetPath, String lastRevision) {
        if (isInIndex(sourcePath)) {
            if (remove) {
                addOrRemoveIndex(sourcePath, true, false);
            }
            if (targetPath != null) {
                addOrRemoveIndex(targetPath, false, true);
            }
            // don't index the index data itself
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
        if (remove) {
            addOrRemoveRecursive(n, true, false);
        }
        if (targetPath != null) {
            t = new JsopTokenizer(node);
            map = new NodeMap();
            t.read('{');
            n = NodeImpl.parse(map, t, 0, targetPath);
            addOrRemoveRecursive(n, false, true);
        }
    }

    private void buildIndex(PIndex index) {
        // TODO index: add ability to start / stop / restart indexing; log the progress
        addRecursive(index, "/");
    }

    private void addRecursive(PIndex index, String path) {
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
        if (needFlush()) {
            flushBuffer();
        }
    }

    private boolean needFlush() {
        return buffer != null && buffer.length() > MAX_BUFFER_LENGTH;
    }

    public PrefixIndex getPrefixIndex(String prefix) {
        return prefixIndexes.get(prefix);
    }

    public PropertyIndex getPropertyIndex(String property) {
        return propertyIndexes.get(property);
    }

}
