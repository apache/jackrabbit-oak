/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.mk.model;

import org.apache.jackrabbit.mk.store.Binding;
import org.apache.jackrabbit.mk.store.NotFoundException;
import org.apache.jackrabbit.mk.store.RevisionProvider;
import org.apache.jackrabbit.mk.util.AbstractRangeIterator;
import org.apache.jackrabbit.mk.util.EmptyIterator;
import org.apache.jackrabbit.mk.util.PathUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 *
 */
public abstract class AbstractNode implements Node {

    protected RevisionProvider provider;
    
    protected HashMap<String, String> properties;

    protected ChildNodeEntries childEntries;

    protected AbstractNode(RevisionProvider provider) {
        this.provider = provider;
        this.properties = new HashMap<String, String>();
        this.childEntries = new ChildNodeEntriesMap();
    }

    protected AbstractNode(Node other, RevisionProvider provider) {
        this.provider = provider;
        if (other instanceof AbstractNode) {
            AbstractNode srcNode = (AbstractNode) other;
            this.properties = (HashMap<String, String>) srcNode.properties.clone();
            this.childEntries = (ChildNodeEntries) srcNode.childEntries.clone();
        } else {
            this.properties = new HashMap<String, String>(other.getProperties());
            if (other.getChildNodeCount() <= ChildNodeEntries.CAPACITY_THRESHOLD) {
                this.childEntries = new ChildNodeEntriesMap();
            } else {
                this.childEntries = new ChildNodeEntriesTree(provider);
            }
            for (Iterator<ChildNodeEntry> it = other.getChildNodeEntries(0, -1); it.hasNext(); ) {
                ChildNodeEntry cne = it.next();
                this.childEntries.add(cne);
            }
        }
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public ChildNodeEntry getChildNodeEntry(String name) {
        return childEntries.get(name);
    }

    public Iterator<String> getChildNodeNames(int offset, int count) {
        return childEntries.getNames(offset, count);
    }

    public int getChildNodeCount() {
        return childEntries.getCount();
    }

    public Iterator<ChildNodeEntry> getChildNodeEntries(int offset, int count) {
        return childEntries.getEntries(offset, count);
    }

    public Iterator<ChildNode> getChildNodes(int offset, int count)
            throws Exception {
        if (offset < 0 || count < -1) {
            throw new IllegalArgumentException();
        }

        if (offset >= childEntries.getCount()) {
            return new EmptyIterator<ChildNode>();
        }

        // todo support embedded/in-lined sub-trees

        if (count == -1 || (offset + count) > childEntries.getCount()) {
            count = childEntries.getCount() - offset;
        }

        return new AbstractRangeIterator<ChildNode>(childEntries.getEntries(offset, count), 0, -1) {
            @Override
            protected ChildNode doNext() {
                ChildNodeEntry cne = (ChildNodeEntry) it.next();
                try {
                    return new ChildNode(cne.getName(), provider.getNode(cne.getId()));
                } catch (Exception e) {
                    throw new NoSuchElementException();
                }
            }
        };
    }

    public Node getNode(String relPath)
            throws NotFoundException, Exception {
        String[] names = PathUtils.split(relPath);

        Node node = this;
        for (String name : names) {
            ChildNodeEntry cne = node.getChildNodeEntry(name);
            if (cne == null) {
                throw new NotFoundException();
            }
            node = provider.getNode(cne.getId());
        }
        return node;
    }

    public void diff(Node other, NodeDiffHandler handler) {
        // compare properties
        Map<String, String> oldProps = getProperties();
        Map<String, String> newProps = other.getProperties();
        if (!oldProps.equals(newProps)) {
            Set<String> set = new HashSet<String>();
            set.addAll(oldProps.keySet());
            set.removeAll(newProps.keySet());
            for (String name : set) {
                handler.propDeleted(name, oldProps.get(name));
            }
            set.clear();
            set.addAll(newProps.keySet());
            set.removeAll(oldProps.keySet());
            for (String name : set) {
                handler.propAdded(name, newProps.get(name));
            }
            for (Map.Entry<String, String> entry : oldProps.entrySet()) {
                String val = newProps.get(entry.getKey());
                if (val != null && !entry.getValue().equals(val)) {
                    handler.propChanged(entry.getKey(), entry.getValue(), val);
                }
            }
        }

        // compare child node entries

        if (other instanceof AbstractNode) {
            ChildNodeEntries otherEntries = ((AbstractNode) other).childEntries;
            for (Iterator<ChildNodeEntry> it = childEntries.getAdded(otherEntries); it.hasNext(); ) {
                handler.childNodeAdded(it.next());
            }
            for (Iterator<ChildNodeEntry> it = childEntries.getRemoved(otherEntries); it.hasNext(); ) {
                handler.childNodeDeleted(it.next());
            }
            for (Iterator<ChildNodeEntry> it = childEntries.getModified(otherEntries); it.hasNext(); ) {
                ChildNodeEntry old = it.next();
                ChildNodeEntry modified = otherEntries.get(old.getName());
                handler.childNodeChanged(old, modified.getId());
            }
            return;
        }

        Map<String, ChildNodeEntry> oldEntries = new HashMap<String, ChildNodeEntry>(getChildNodeCount());
        for (Iterator<ChildNodeEntry> it = getChildNodeEntries(0, -1); it.hasNext(); ) {
            ChildNodeEntry cne = it.next();
            oldEntries.put(cne.getName(), cne);
        }
        Map<String, ChildNodeEntry> newEntries = new HashMap<String, ChildNodeEntry>(other.getChildNodeCount());
        for (Iterator<ChildNodeEntry> it = other.getChildNodeEntries(0, -1); it.hasNext(); ) {
            ChildNodeEntry cne = it.next();
            newEntries.put(cne.getName(), cne);
        }
        if (!oldEntries.equals(newEntries)) {
            Set<String> set = new HashSet<String>();
            set.addAll(oldEntries.keySet());
            set.removeAll(newEntries.keySet());
            for (String name : set) {
                handler.childNodeDeleted(oldEntries.get(name));
            }
            set.clear();
            set.addAll(newEntries.keySet());
            set.removeAll(oldEntries.keySet());
            for (String name : set) {
                handler.childNodeAdded(newEntries.get(name));
            }
            for (ChildNodeEntry cneOld : oldEntries.values()) {
                ChildNodeEntry cneNew = newEntries.get(cneOld.getName());
                if (cneNew != null && !cneNew.getId().equals(cneOld.getId())) {
                    handler.childNodeChanged(cneOld, cneNew.getId());
                }
            }
        }
    }

    public void serialize(Binding binding) throws Exception {
        final Iterator<Map.Entry<String, String>> iter = properties.entrySet().iterator();
        binding.writeMap(":props", properties.size(),
                new Binding.StringEntryIterator() {
                    @Override
                    public boolean hasNext() {
                        return iter.hasNext();
                    }
                    @Override
                    public Binding.StringEntry next() {
                        Map.Entry<String, String> entry = iter.next();
                        return new Binding.StringEntry(entry.getKey(), entry.getValue());
                    }
                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                });
        binding.write(":inlined", childEntries.inlined() ? 1 : 0);
        childEntries.serialize(binding);
    }
}