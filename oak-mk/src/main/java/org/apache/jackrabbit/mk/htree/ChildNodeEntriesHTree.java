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
package org.apache.jackrabbit.mk.htree;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.jackrabbit.mk.model.ChildNodeEntries;
import org.apache.jackrabbit.mk.model.ChildNodeEntry;
import org.apache.jackrabbit.mk.store.Binding;
import org.apache.jackrabbit.mk.store.PersistHook;
import org.apache.jackrabbit.mk.store.RevisionProvider;
import org.apache.jackrabbit.mk.store.RevisionStore;
import org.apache.jackrabbit.mk.store.RevisionStore.PutToken;
import org.apache.jackrabbit.mk.util.AbstractFilteringIterator;
import org.apache.jackrabbit.mk.util.AbstractRangeIterator;

/**
 * <code>HTree</code> based implementation to manage child node entries. 
 */
public class ChildNodeEntriesHTree implements ChildNodeEntries, PersistHook {

    private HashDirectory top;
    
    public ChildNodeEntriesHTree(RevisionProvider provider) {
        top = new HashDirectory(provider, 0);
    }
    
    public Object clone() {
        ChildNodeEntriesHTree clone = null;
        try {
            clone = (ChildNodeEntriesHTree) super.clone();
        } catch (CloneNotSupportedException e) {
            // can't possibly get here
        }
        // shallow clone of array of immutable IndexEntry objects
        clone.top = (HashDirectory) top.clone();
        return clone;
    }

    @Override
    public boolean inlined() {
        return false;
    }

    @Override
    public int getCount() {
        return top.getCount();
    }

    @Override
    public ChildNodeEntry get(String name) {
        return top.get(name);
    }

    @Override
    public Iterator<String> getNames(int offset, int count) {
        if (offset < 0 || count < -1) {
            throw new IllegalArgumentException();
        }

        if (offset >= getCount() || count == 0) {
            List<String> empty = Collections.emptyList();
            return empty.iterator();
        }

        if (count == -1 || (offset + count) > getCount()) {
            count = getCount() - offset;
        }

        return new AbstractRangeIterator<String>(getEntries(offset, count), 0, -1) {
            @Override
            protected String doNext() {
                ChildNodeEntry cne = (ChildNodeEntry) it.next();
                return cne.getName();
            }
        };
    }

    @Override
    public Iterator<ChildNodeEntry> getEntries(int offset, int count) {
        return top.getEntries(offset, count);
    }

    @Override
    public ChildNodeEntry add(ChildNodeEntry entry) {
        return top.add(entry);
    }

    @Override
    public ChildNodeEntry remove(String name) {
        return top.remove(name);
    }

    @Override
    public ChildNodeEntry rename(String oldName, String newName) {
        if (oldName.equals(newName)) {
            return get(oldName);
        }
        ChildNodeEntry old = remove(oldName);
        if (old == null) {
            return null;
        }
        add(new ChildNodeEntry(newName, old.getId()));
        return old;
    }

    @Override
    public Iterator<ChildNodeEntry> getAdded(ChildNodeEntries other) {
        if (other instanceof ChildNodeEntriesHTree) {
            return top.getAdded(((ChildNodeEntriesHTree) other).top);
        }
        // todo optimize
        return new AbstractFilteringIterator<ChildNodeEntry>(other.getEntries(0, -1)) {
            @Override
            protected boolean include(ChildNodeEntry entry) {
                return get(entry.getName()) == null;
            }
        };
    }

    @Override
    public Iterator<ChildNodeEntry> getRemoved(ChildNodeEntries other) {
        return other.getAdded(this);
    }

    @Override
    public Iterator<ChildNodeEntry> getModified(final ChildNodeEntries other) {
        if (other instanceof ChildNodeEntriesHTree) {
            return top.getModified(((ChildNodeEntriesHTree) other).top);
        }
        // todo optimize
        return new AbstractFilteringIterator<ChildNodeEntry>(getEntries(0, -1)) {
            @Override
            protected boolean include(ChildNodeEntry entry) {
                ChildNodeEntry namesake = other.get(entry.getName());
                return (namesake != null && !namesake.getId().equals(entry.getId()));
            }
        };
    }

    @Override
    public void serialize(Binding binding) throws Exception {
        top.serialize(binding);
    }

    public void deserialize(Binding binding) throws Exception {
        top.deserialize(binding);
    }
    
    @Override
    public void prePersist(RevisionStore store, PutToken token)
            throws Exception {
        
        top.prePersist(store, token);
    }
    
    @Override
    public void postPersist(RevisionStore store, PutToken token)
            throws Exception {

        // nothing to be done here
    }

    @Override
    public int getMemory() {
        return top.getMemory();
    }

}
