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
import org.apache.jackrabbit.mk.util.AbstractFilteringIterator;
import org.apache.jackrabbit.mk.util.RangeIterator;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class ChildNodeEntriesMap implements ChildNodeEntries {

    protected static final List<ChildNode> EMPTY = Collections.emptyList();
    
    protected HashMap<String, ChildNode> entries = new HashMap<String, ChildNode>();

    ChildNodeEntriesMap() {
    }

    ChildNodeEntriesMap(ChildNodeEntriesMap other) {
        entries = (HashMap<String, ChildNode>) other.entries.clone();
    }

    //------------------------------------------------------------< overrides >

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ChildNodeEntriesMap) {
            return entries.equals(((ChildNodeEntriesMap) obj).entries);
        }
        return false;
    }

    @Override
    public Object clone()  {
        ChildNodeEntriesMap clone = null;
        try {
            clone = (ChildNodeEntriesMap) super.clone();
        } catch (CloneNotSupportedException e) {
            // can't possibly get here
        }
        clone.entries = (HashMap<String, ChildNode>) entries.clone();
        return clone;
    }

    @Override
    public boolean inlined() {
        return true;
    }

    //-------------------------------------------------------------< read ops >

    @Override
    public int getCount() {
        return entries.size();
    }

    @Override
    public ChildNode get(String name) {
        return entries.get(name);
    }

    @Override
    public Iterator<String> getNames(int offset, int count) {
        if (offset < 0 || count < -1) {
            throw new IllegalArgumentException();
        }

        if (offset == 0 && count == -1) {
            return entries.keySet().iterator();
        } else {
            if (offset >= entries.size() || count == 0) {
                List<String> empty = Collections.emptyList();
                return empty.iterator();
            }
            if (count == -1 || (offset + count) > entries.size()) {
                count = entries.size() - offset;
            }
            return new RangeIterator<String>(entries.keySet().iterator(), offset, count);
        }
    }

    @Override
    public Iterator<ChildNode> getEntries(int offset, int count) {
        if (offset < 0 || count < -1) {
            throw new IllegalArgumentException();
        }
        if (offset == 0 && count == -1) {
            return entries.values().iterator();
        } else {
            if (offset >= entries.size() || count == 0) {
                return EMPTY.iterator();
            }
            if (count == -1 || (offset + count) > entries.size()) {
                count = entries.size() - offset;
            }
            return new RangeIterator<ChildNode>(entries.values().iterator(), offset, count);
        }
    }

    //------------------------------------------------------------< write ops >

    @Override
    public ChildNode add(ChildNode entry) {
        return entries.put(entry.getName(), entry);
    }

    @Override
    public ChildNode remove(String name) {
        return entries.remove(name);
    }

    @Override
    public ChildNode rename(String oldName, String newName) {
        if (oldName.equals(newName)) {
            return entries.get(oldName);
        }
        if (entries.get(oldName) == null) {
            return null;
        }
        HashMap<String, ChildNode> clone =
                (HashMap<String, ChildNode>) entries.clone();
        entries.clear();
        ChildNode oldCNE = null;
        for (Map.Entry<String, ChildNode> entry : clone.entrySet()) {
            if (entry.getKey().equals(oldName)) {
                oldCNE = entry.getValue();
                entries.put(newName, new ChildNode(newName, oldCNE.getId()));
            } else {
                entries.put(entry.getKey(), entry.getValue());
            }
        }
        return oldCNE;
    }

    //-------------------------------------------------------------< diff ops >

    @Override
    public Iterator<ChildNode> getAdded(final ChildNodeEntries other) {
        return new AbstractFilteringIterator<ChildNode>(other.getEntries(0, -1)) {
            @Override
            protected boolean include(ChildNode entry) {
                return !entries.containsKey(entry.getName());
            }
        };
    }

    @Override
    public Iterator<ChildNode> getRemoved(final ChildNodeEntries other) {
        return new AbstractFilteringIterator<ChildNode>(entries.values().iterator()) {
            @Override
            protected boolean include(ChildNode entry) {
                return other.get(entry.getName()) == null;
            }
        };
    }

    @Override
    public Iterator<ChildNode> getModified(final ChildNodeEntries other) {
        return new AbstractFilteringIterator<ChildNode>(getEntries(0, -1)) {
            @Override
            protected boolean include(ChildNode entry) {
                ChildNode namesake = other.get(entry.getName());
                return (namesake != null && !namesake.getId().equals(entry.getId()));
            }
        };
    }

    //------------------------------------------------< serialization support >

    @Override
    public void serialize(Binding binding) throws Exception {
        final Iterator<ChildNode> iter = getEntries(0, -1);
        binding.writeMap(":children", getCount(),
                new Binding.BytesEntryIterator() {
                    @Override
                    public boolean hasNext() {
                        return iter.hasNext();
                    }
                    @Override
                    public Binding.BytesEntry next() {
                        ChildNode cne = iter.next();
                        return new Binding.BytesEntry(cne.getName(), cne.getId().getBytes());
                    }
                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                });
    }

    public static ChildNodeEntriesMap deserialize(Binding binding) throws Exception {
        ChildNodeEntriesMap newInstance = new ChildNodeEntriesMap();
        Binding.BytesEntryIterator iter = binding.readBytesMap(":children");
        while (iter.hasNext()) {
            Binding.BytesEntry entry = iter.next();
            newInstance.add(new ChildNode(entry.getKey(), new Id(entry.getValue())));
        }
        return newInstance;
    }
}
