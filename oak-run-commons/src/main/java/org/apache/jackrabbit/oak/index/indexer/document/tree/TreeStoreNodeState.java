/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.index.indexer.document.tree;

import static org.apache.jackrabbit.guava.common.collect.Iterators.transform;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;

public class TreeStoreNodeState implements NodeState {

    private final Logger LOG = LoggerFactory.getLogger(TreeStoreNodeState.class);

    private final NodeState delegate;
    private final String path;
    private final TreeStore treeStore;

    public TreeStoreNodeState(NodeState delegate, String path, TreeStore treeStore) {
        this.delegate = delegate;
        this.path = path;
        this.treeStore = treeStore;
    }

    @Override
    public boolean exists() {
        LOG.debug("exists {}", path);
        return delegate.exists();
    }

    @Override
    public boolean hasProperty(@NotNull String name) {
        LOG.debug("hasProperty {} {}", name, path);
        return delegate.hasProperty(name);
    }

    @Nullable
    @Override
    public PropertyState getProperty(@NotNull String name) {
        LOG.debug("getProperty {} {}", name, path);
        return delegate.getProperty(name);
    }

    @Override
    public boolean getBoolean(@NotNull String name) {
        LOG.debug("getBoolean {} {}", name, path);
        return delegate.getBoolean(name);
    }

    @Override
    public long getLong(String name) {
        LOG.debug("getLong {} {}", name, path);
        return delegate.getLong(name);
    }

    @Nullable
    @Override
    public String getString(String name) {
        LOG.debug("getString {} {}", name, path);
        return delegate.getString(name);
    }

    @NotNull
    @Override
    public Iterable<String> getStrings(@NotNull String name) {
        LOG.debug("getStrings {} {}", name, path);
        return delegate.getStrings(name);
    }

    @Nullable
    @Override
    public String getName(@NotNull String name) {
        LOG.debug("getName {} {}", name, path);
        return delegate.getName(name);
    }

    @NotNull
    @Override
    public Iterable<String> getNames(@NotNull String name) {
        LOG.debug("getNames {} {}", name, path);
        return delegate.getNames(name);
    }

    @Override
    public long getPropertyCount() {
        LOG.debug("getPropertyCount {}", path);
        return delegate.getPropertyCount();
    }

    @NotNull
    @Override
    public Iterable<? extends PropertyState> getProperties() {
        LOG.debug("getProperties {}", path);
        return delegate.getProperties();
    }

    @NotNull
    @Override
    public NodeBuilder builder() {
        return delegate.builder();
    }

    @Override
    public boolean compareAgainstBaseState(NodeState base, NodeStateDiff diff) {
        return AbstractNodeState.compareAgainstBaseState(this, base, diff);
    }

    // ~-------------------------------< child node access >

    @Override
    public boolean hasChildNode(@NotNull String name) {
        LOG.debug("hasChildNode {} {}", name, path);
        return treeStore.getNodeState(PathUtils.concat(path, name)).exists();
    }

    @NotNull
    @Override
    public NodeState getChildNode(@NotNull String name) throws IllegalArgumentException {
        LOG.debug("getChildNode {} {}", name, path);
        return treeStore.getNodeState(PathUtils.concat(path, name));
    }

    @Override
    public long getChildNodeCount(long max) {
        LOG.debug("getChildNodeCount {} {}", max, path);
        long result = 0;
        Iterator<String> it = getChildNodeNamesIterator();
        while (it.hasNext()) {
            result++;
            if (result > max) {
                return Long.MAX_VALUE;
            }
            it.next();
        }
        return result;
    }

    @Override
    public Iterable<String> getChildNodeNames() {
        LOG.debug("getChildNodeNames {}", path);
        return new Iterable<String>() {
            public Iterator<String> iterator() {
                return getChildNodeNamesIterator();
            }
        };
    }

    @NotNull
    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        LOG.debug("getChildNodeEntries {}", path);
        return () -> transform(getChildNodeIterator(),
                s -> new MemoryChildNodeEntry(PathUtils.getName(s.getPath()), s.getNodeState()));
    }

    private Iterator<NodeStateEntry> getChildNodeIterator() {
        if (TreeStore.CHILD_ENTRIES) {
              return transform(getChildNodeNamesIterator(),
                      s -> treeStore.getNodeStateEntry(PathUtils.concat(path, s)));
        } else {
            return transform(getChildEntryIterator(),
                    e -> treeStore.getNodeStateEntry(e.getKey(), e.getValue()));
        }
    }

    /**
     * Return an iterator over all child nodes
     *
     * @return an iterator over path, value
     */
    Iterator<Entry<String, String>> getChildEntryIterator() {
        if (!TreeStore.PREFERRED_CHILDREN) {
            return getChildEntryIteratorRaw();
        }
        Iterator<String> it = getChildNodeNamesIterator();
        if (!it.hasNext()) {
            return Collections.emptyListIterator();
        }
        return transform(it, k -> {
            String p = PathUtils.concat(path, k);
            return newEntry(p, treeStore.getSession().get(TreeStore.convertPathToKey(p)));
        });
    }

    Iterator<Entry<String, String>> getChildEntryIteratorRaw() {
        Entry<String, String> e = findFirstChild(path);
        if (e == null) {
            return Iterators.emptyIterator();
        }
        return new Iterator<Entry<String, String>>() {
            Entry<String, String> current = e;

            public boolean hasNext() {
                return current != null;
            }

            public Entry<String, String> next() {
                Entry<String, String> result = current;
                if (result == null) {
                    throw new IllegalStateException();
                }
                current = findNextSibling(current.getKey());
                return result;
            }
        };
    }

    private Entry<String, String> newEntry(String key, String value) {
        return new Entry<>() {

            @Override
            public String getKey() {
                return key;
            }

            @Override
            public String getValue() {
                return value;
            }

            @Override
            public String setValue(String value) {
                throw new UnsupportedOperationException();
            }
        };
    }

    private Entry<String, String> findFirstChild(String path) {
        String key = TreeStore.convertPathToKey(path);
        String search;
        if (key.equals("\t")) {
            // root node: search for the next
            search = "\t";
        } else {
            // not a root node, such as a-b:
            // search for the one after a-b-
            search = key + "\t";
        }
        Iterator<Entry<String, String>> it = treeStore.getSession().iterator(search);
        Entry<String, String> x = it.hasNext() ? it.next() : null;
        if (x == null || !x.getKey().startsWith(search)) {
            // found null or /b: no child
            return null;
        }
        return newEntry(TreeStore.convertKeyToPath(x.getKey()), x.getValue());
    }

    private Entry<String, String> findNextSibling(String path) {
        String key = TreeStore.convertPathToKey(path);
        // search for the next
        String prefix = key.substring(0, key.lastIndexOf('\t') + 1);
        String search = key + "\n";
        Iterator<Entry<String, String>> it = treeStore.getSession().iterator(search);
        Entry<String, String> x = it.hasNext() ? it.next() : null;
        if (x == null || !x.getKey().startsWith(prefix)) {
            return null;
        }
        return newEntry(TreeStore.convertKeyToPath(x.getKey()), x.getValue());
    }

    Iterator<String> getChildNodeNamesIterator() {
        if(!TreeStore.PREFERRED_CHILDREN) {
            return getChildNodeNamesIteratorRaw();
        }

        Iterator<String> it = getChildNodeNamesIteratorRaw();
        if (!it.hasNext()) {
            return Collections.emptyListIterator();
        }
        // collect the first few entries
        List<String> list = new ArrayList<>();
        int preferredChildCount = 0;
        for (int i = 0; it.hasNext() && i < TreeStore.PREFERRED_CHILDREN_SET.size(); i++) {
            String s = it.next();
            if (TreeStore.PREFERRED_CHILDREN_SET.contains(s)) {
                preferredChildCount++;
            }
            list.add(s);
        }
        if (!it.hasNext()) {
            // we have all children
            if (preferredChildCount == 0 || preferredChildCount == list.size()) {
                // no preferred children at all, or
                // all of the children are preferred
                return list.iterator();
            }
            list.sort(new Comparator<String>() {

                @Override
                public int compare(String o1, String o2) {
                    boolean p1 = TreeStore.PREFERRED_CHILDREN_SET.contains(o1);
                    boolean p2 = TreeStore.PREFERRED_CHILDREN_SET.contains(o2);
                    if (p1 != p2) {
                        // one is preferred, the other is not
                        return p1 ? -1 : 1;
                    }
                    // both or neither is preferred
                    return o1.compareTo(o2);
                }

            });
            return list.iterator();
        }

        // if there are many children, we need to use a slower algorithm:
        // first read all preferred children
        list.clear();
        for (String k : TreeStore.PREFERRED_CHILDREN_LIST) {
            String preferred = TreeStore.convertPathToKey(PathUtils.concat(path, k));
            String value = treeStore.getSession().get(preferred);
            if (value == null) {
                continue;
            }
            list.add(k);
        }
        Iterator<String> rest = Iterators.filter(getChildNodeNamesIteratorRaw(), new Predicate<String>() {
            @Override
            public boolean apply(String input) {
                return !TreeStore.PREFERRED_CHILDREN_SET.contains(input);
            }
        });
        return Iterators.concat(list.iterator(), rest);
    }

    Iterator<String> getChildNodeNamesIteratorRaw() {
        if (!TreeStore.CHILD_ENTRIES) {
            return transform(getChildEntryIteratorRaw(), e -> PathUtils.getName(e.getKey()));
        }
        String key = TreeStore.convertPathToKey(path);
        Iterator<Entry<String, String>> it = treeStore.getSession().iterator(key);
        return new Iterator<String>() {
            String current;
            {
                fetch();
            }

            private void fetch() {
                if (!it.hasNext()) {
                    current = null;
                } else {
                    Entry<String, String> e = it.next();
                    if (!e.getValue().isEmpty()) {
                        current = null;
                    } else {
                        String key = e.getKey();
                        int index = key.lastIndexOf('\t');
                        if (index < 0) {
                            throw new IllegalArgumentException(key);
                        }
                        current = key.substring(index + 1);
                    }
                }
            }

            public boolean hasNext() {
                return current != null;
            }

            public String next() {
                String result = current;
                if (result == null) {
                    throw new IllegalStateException();
                }
                fetch();
                return result;
            }
        };
    }

}