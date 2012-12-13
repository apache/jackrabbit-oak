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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.jackrabbit.mk.model.ChildNodeEntries;
import org.apache.jackrabbit.mk.model.ChildNodeEntry;
import org.apache.jackrabbit.mk.model.Id;
import org.apache.jackrabbit.mk.store.Binding;
import org.apache.jackrabbit.mk.store.RevisionProvider;
import org.apache.jackrabbit.mk.store.RevisionStore;
import org.apache.jackrabbit.mk.store.RevisionStore.PutToken;

/**
 * Directory structure in an <code>HTree</code>. 
 */
class HashDirectory implements ChildNodeEntries {
    
    private static final List<ChildNodeEntry> EMPTY = Collections.emptyList();
    private static final int MAX_CHILDREN = 256;
    private static final int BIT_SIZE = 8;
    private static final int MAX_DEPTH = 3;
    
    private final RevisionProvider provider;
    private final int depth;
    private int count;
    private IndexEntry[] index = new IndexEntry[MAX_CHILDREN];
    
    public HashDirectory(RevisionProvider provider, int depth) {
        this.provider = provider;
        this.depth = depth;
    }

    public HashDirectory(HashDirectory other) {
        provider = other.provider;
        depth = other.depth;
        count = other.count;
        index = other.index.clone();
    }
    
    @Override
    public Object clone() {
        HashDirectory clone = null;
        try {
            clone = (HashDirectory) super.clone();
        } catch (CloneNotSupportedException e) {
            // can't possibly get here
        }
        // shallow clone of array of immutable IndexEntry objects
        clone.index = index.clone();
        return clone;
    }
    
    public int getCount() {
        return count;
    }
    
    public ChildNodeEntry get(String name) {
        int hash = hashCode(name);
        
        IndexEntry ie = index[hash];
        if (ie == null) {
            return null;
        }
        if (ie instanceof ChildNodeEntry) {
            ChildNodeEntry cne = (ChildNodeEntry) ie;
            return cne.getName().equals(name) ? cne : null;
        }
        ChildNodeEntries container = ((ContainerEntry) ie).getContainer();
        if (container != null) {
            return container.get(name);
        }
        return null;
    }

    public Iterator<ChildNodeEntry> getEntries(int offset, int count) {
        if (offset < 0 || count < -1) {
            throw new IllegalArgumentException();
        }

        if (offset >= this.count || count == 0) {
            return EMPTY.iterator();
        }

        int skipped = 0;
        if (count == -1 || (offset + count) > this.count) {
            count = this.count - offset;
        }
        ArrayList<ChildNodeEntry> list = new ArrayList<ChildNodeEntry>(count);
        for (IndexEntry ie : index) {
            if (ie == null) {
                continue;
            }
            if (skipped + ie.getSize() <= offset) {
                skipped += ie.getSize();
                continue;
            }
            if (ie instanceof ChildNodeEntry) {
                list.add((ChildNodeEntry) ie);
            } else {
                ChildNodeEntries container = ((ContainerEntry) ie).getContainer();
                if (container != null) {
                    for (Iterator<ChildNodeEntry> it = container.getEntries(offset - skipped, count - list.size());
                            it.hasNext(); ) {
                        list.add(it.next());
                    }
                   skipped = offset;
                }
                if (list.size() == count) {
                    break;
                }
            }
        }
        return list.iterator();
    }
    
    public ChildNodeEntry add(ChildNodeEntry entry) {
        int hash = hashCode(entry.getName());
        IndexEntry ie = index[hash];
        if (ie == null) {
            index[hash] = new NodeEntry(entry.getName(), entry.getId());
            count++;
            return null;
        }
        if (ie instanceof ChildNodeEntry) {
            ChildNodeEntry existing = (ChildNodeEntry) ie;
            if (existing.getName().equals(entry.getName())) {
                index[hash] = new NodeEntry(entry.getName(), entry.getId());
                return existing;
            } else {
                ContainerEntry ce = createContainerEntry();
                ce.getContainer().add(existing);
                ce.getContainer().add(entry);
                index[hash] = ce;
                count++;
                return null;
            }
        } 
        ContainerEntry ce = (ContainerEntry) ie;
        ChildNodeEntries container = ce.getContainer();
        ChildNodeEntry existing = container.add(entry);
        if (entry.equals(existing)) {
            // no-op
            return existing;
        }
        ce.setDirty(container);
        if (existing == null) {
            // new entry
            count++;
        }
        return existing;
    }

    public ChildNodeEntry remove(String name) {
        int hash = hashCode(name);
        IndexEntry ie = index[hash];
        if (ie == null) {
            return null;
        }
        if (ie instanceof ChildNodeEntry) {
            ChildNodeEntry existing = (ChildNodeEntry) ie;
            if (existing.getName().equals(name)) {
                index[hash] = null;
                count--;
                return existing;
            } else {
                return null;
            }
        }
        ContainerEntry ce = (ContainerEntry) ie;
        ChildNodeEntries container = ce.getContainer();
        ChildNodeEntry existing = container.remove(name);
        if (existing == null) {
            return null;
        }
        if (container.getCount() == 0) {
            index[hash] = null;
        } else if (container.getCount() == 1) {
            // inline single remaining entry
            ChildNodeEntry remaining = container.getEntries(0, 1).next();
            index[hash] = new NodeEntry(remaining.getName(), remaining.getId());
        } else {
            ce.setDirty(container);
        }
        count--;
        return existing;
    }

    public Iterator<ChildNodeEntry> getAdded(ChildNodeEntries otherContainer) {
        if (!(otherContainer instanceof HashDirectory)) {
            // needs no implementation
            return null;
        }
        
        HashDirectory other = (HashDirectory) otherContainer;
        List<ChildNodeEntry> added = new ArrayList<ChildNodeEntry>();
        for (int i = 0; i < index.length; i++) {
            IndexEntry ie1 = index[i];
            IndexEntry ie2 = other.index[i];

            if (ie1 == null && ie2 == null || (ie1 != null && ie1.equals(ie2))) {
                continue;
            }

            // index entries aren't equal
            if (ie1 == null) {
                // this index entry in null => other must be non-null, add all its entries
                if (ie2 instanceof ChildNodeEntry) {
                    added.add((ChildNodeEntry) ie2);
                } else {
                    ChildNodeEntries container = ((ContainerEntry) ie2).getContainer();
                    for (Iterator<ChildNodeEntry> it = container.getEntries(0, -1); it.hasNext(); ) {
                        added.add(it.next());
                    }
                }
                continue;
            }
            
            // optimization for simple child node entries
            if (ie1 instanceof ChildNodeEntry && ie2 instanceof ChildNodeEntry) {
                ChildNodeEntry cne1 = (ChildNodeEntry) ie1;
                ChildNodeEntry cne2 = (ChildNodeEntry) ie2;
                
                if (cne2.getName().equals(cne1.getName())) {
                    added.add(cne2);
                }
                continue;
            }
            
            // all other cases
            for (Iterator<ChildNodeEntry> it = ie1.getAdded(ie2); it.hasNext(); ) {
                added.add(it.next());
            }
        }            
        return added.iterator();
    }

    public Iterator<ChildNodeEntry> getModified(ChildNodeEntries otherContainer) {
        if (!(otherContainer instanceof HashDirectory)) {
            // needs no implementation
            return null;
        }
        
        HashDirectory other = (HashDirectory) otherContainer;
        List<ChildNodeEntry> modified = new ArrayList<ChildNodeEntry>();
        for (int i = 0; i < index.length; i++) {
            IndexEntry ie1 = index[i];
            IndexEntry ie2 = other.index[i];
            
            if (ie1 == null || ie2 == null || ie1.equals(ie2)) {
                continue;
            }

            // optimization for simple child node entries
            if (ie1 instanceof ChildNodeEntry && ie2 instanceof ChildNodeEntry) {
                ChildNodeEntry cne1 = (ChildNodeEntry) ie1;
                ChildNodeEntry cne2 = (ChildNodeEntry) ie2;
                
                if (cne1.getName().equals(cne2.getName()) && !cne1.getId().equals(cne2.getId())) {
                    modified.add(cne1);
                    continue;
                }
            }
                
            // all other cases
            for (Iterator<ChildNodeEntry> it = ie1.getModified(ie2); it.hasNext();) {
                modified.add(it.next());
            }
        }
        return modified.iterator();
    }
    
    private ContainerEntry createContainerEntry() {
        return depth < MAX_DEPTH - 1 ? new DirectoryEntry(depth + 1) : new BucketEntry(); 
    }
    
    private int hashCode(String name) {
        int hashMask = hashMask();
        int hash = name.hashCode();
        hash = hash & hashMask;
        hash = hash >>> ((MAX_DEPTH - depth) * BIT_SIZE);
        hash = hash % MAX_CHILDREN;
        return hash;
    }

    int hashMask() {
        int bits = MAX_CHILDREN-1;
        int hashMask = bits << ((MAX_DEPTH - depth) * BIT_SIZE);
        return hashMask;
    }
    
    public void deserialize(Binding binding) throws Exception {
        count = binding.readIntValue(":count");
        
        Binding.StringEntryIterator iter = binding.readStringMap(":index");
        int pos = -1;
        while (iter.hasNext()) {
            Binding.StringEntry entry = iter.next();
            ++pos;
            // deserialize index array entry
            assert(pos == Integer.parseInt(entry.getKey()));
            if (entry.getValue().length() == 0) {
                // ""
                index[pos] = null;
            } else {
                switch (entry.getValue().charAt(0)) {
                case 'n':
                    String value = entry.getValue().substring(1);
                    int i = value.indexOf(':');
                    String id = value.substring(0, i);
                    String name = value.substring(i + 1);
                    index[pos] = new NodeEntry(name, Id.fromString(id));
                    break;
                case 'b':
                    value = entry.getValue().substring(1);
                    i = value.indexOf(':');
                    id = value.substring(0, i);
                    int count = Integer.parseInt(value.substring(i + 1));
                    index[pos] = new BucketEntry(provider, Id.fromString(id), count);
                    break;
                case 'd':
                    value = entry.getValue().substring(1);
                    i = value.indexOf(':');
                    id = value.substring(0, i);
                    count = Integer.parseInt(value.substring(i + 1));
                    index[pos] = new DirectoryEntry(provider, Id.fromString(id), count, depth + 1);
                    break;
                }
            }
        }
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof HashDirectory) {
            HashDirectory other = (HashDirectory) obj;
            return Arrays.equals(index, other.index);
        }
        return false;
    }
    
    public void prePersist(RevisionStore store, PutToken token)
            throws Exception {
        
        for (int i = 0; i < index.length; i++) {
            if (index[i] instanceof ContainerEntry) {
                ContainerEntry ce  = (ContainerEntry) index[i];
                if (ce.isDirty()) {
                    ce.store(store, token);
                }
            }
        }
    }
    
    public void serialize(Binding binding) throws Exception {
        final IndexEntry[] index = this.index;
        binding.write(":count", count);
        binding.writeMap(":index", index.length, new Binding.StringEntryIterator() {
            int pos = -1;

            @Override
            public boolean hasNext() {
                return pos < index.length - 1;
            }

            @Override
            public Binding.StringEntry next() {
                pos++;
                if (pos >= index.length) {
                    throw new NoSuchElementException();
                }
                // serialize index array entry
                IndexEntry entry = index[pos];
                if (entry == null) {
                    // null entry: ""
                    return new Binding.StringEntry(Integer.toString(pos), "");
                } else {
                    return new Binding.StringEntry(Integer.toString(pos), entry.toString());
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        });
    }
    
    /**
     * Entry inside this a directory's index.
     */
    static interface IndexEntry {
        
        public int getSize();

        public Iterator<ChildNodeEntry> getAdded(IndexEntry other);

        public Iterator<ChildNodeEntry> getModified(IndexEntry other);
    }
    
    /**
     * Direct entry inside this a directory's index, pointing to a child node.
     */
    static class NodeEntry extends ChildNodeEntry implements IndexEntry {

        public NodeEntry(String name, Id id) {
            super(name, id);
        }

        public int getSize() {
            return 1;
        }
        
        public Iterator<ChildNodeEntry> getAdded(IndexEntry other) {
            if (other == null) {
                return null;
            }
            ChildNodeEntries container = ((ContainerEntry) other).createCompatibleContainer();
            container.add(this);
            return container.getAdded(((ContainerEntry) other).getContainer());
        }

        public Iterator<ChildNodeEntry> getModified(IndexEntry other) {
            if (other == null) {
                return null;
            }
            ChildNodeEntries container = ((ContainerEntry) other).createCompatibleContainer();
            container.add(this);
            return container.getModified(((ContainerEntry) other).getContainer());
        }
        
        @Override
        public String toString() {
            return "n" + getId() + ":" + getName();
        }
    }
    
    /**
     * Container entry inside this a directory's index, pointing to either a
     * directory or a bucket.
     */
    static abstract class ContainerEntry implements IndexEntry {
        
        protected RevisionProvider provider;
        protected Id id;
        protected int count;
        
        protected ChildNodeEntries container;

        public ContainerEntry(RevisionProvider provider, Id id, int count) {
            this.provider = provider;
            this.id = id;
            this.count = count;
        }
        
        public ContainerEntry() {
        }
        
        public abstract ChildNodeEntries getContainer();
        
        public abstract ChildNodeEntries createCompatibleContainer();
        
        public boolean isDirty() {
            return container != null;
        }
        
        public void setDirty(ChildNodeEntries container) {
            this.container = container;
        }
        
        public Id getId() {
            return id;
        }
        
        public int getSize() {
            if (container != null) {
                return container.getCount();
            }
            return count;
        }
        
        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other instanceof ContainerEntry) {
                ContainerEntry ce = (ContainerEntry) other;
                if (container != null && ce.container != null) {
                    return container.equals(ce.container);
                }
                if (container == null && ce.container == null) {
                    return (count == ce.count && id == null ? ce.id == null : id.equals(ce.id));                    
                }
            }
            return false;
        }

        public Iterator<ChildNodeEntry> getAdded(IndexEntry other) {
            if (other == null) {
                return null;
            }
            if (other instanceof ChildNodeEntry) {
                ChildNodeEntries container  = ((ContainerEntry) other).createCompatibleContainer();
                container.add((ChildNodeEntry) other);
                return getContainer().getAdded(container);
            }
            return getContainer().getAdded(((ContainerEntry) other).getContainer());
        }

        public Iterator<ChildNodeEntry> getModified(IndexEntry other) {
            if (other == null) {
                return null;
            }
            if (other instanceof ChildNodeEntry) {
                ChildNodeEntries container = ((ContainerEntry) other).createCompatibleContainer();
                container.add((ChildNodeEntry) other);
                return getContainer().getModified(container);
            }
            return getContainer().getModified(((ContainerEntry) other).getContainer());
        }

        public void store(RevisionStore store, PutToken token) throws Exception {
            store.putCNEMap(token, container);
        }
    }
    
    /**
     * Directory entry inside this a directory's index, pointing to a directory on the
     * next level.
     */
    static class DirectoryEntry extends ContainerEntry {
        
        private final int depth;
        
        public DirectoryEntry(RevisionProvider provider, Id id, int count, int depth) {
            super(provider, id, count);
            
            this.depth = depth;
        }

        public DirectoryEntry(int depth) {
            this.depth = depth;
        }

        public ChildNodeEntries getContainer() {
            if (container != null) {
                return container;
            }
            
            try {
                // TODO return provider.getCNEMap(id);
                return new HashDirectory(provider, depth);
            } catch (Exception e) {
                // todo log error and gracefully handle exception
                return null;
            }
        }

        public ChildNodeEntries createCompatibleContainer() {
            return new HashDirectory(provider, depth);
        }
        
        @Override
        public String toString() {
            return "d" + getId() + ":" + getSize();
        }

        public void store(RevisionStore store, PutToken token) throws Exception {
            ((HashDirectory) container).prePersist(store, token);
            super.store(store, token);
        }
    }

    /**
     * Bucket entry inside this a directory's index, pointing to a bucket or leaf node.
     */
    static class BucketEntry extends ContainerEntry {
        
        public BucketEntry(RevisionProvider provider, Id id, int count) {
            super(provider, id, count);
        }

        public BucketEntry() {
        }

        public HashBucket getContainer() {
            if (container != null) {
                return (HashBucket) container;
            }

            try {
                return new HashBucket(provider.getCNEMap(id));
            } catch (Exception e) {
                // todo log error and gracefully handle exception
                return null;
            }
        }

        public ChildNodeEntries createCompatibleContainer() {
            return new HashBucket();
        }
    
        @Override
        public String toString() {
            return "b" + getId() + ":" + getSize();
        }
    }

    // ------------------------------------------------------------------------------------------- unimplemented methods

    @Override
    public boolean inlined() {
        throw new NoSuchMethodError();
    }

    @Override
    public Iterator<String> getNames(int offset, int count) {
        throw new NoSuchMethodError();
    }

    @Override
    public ChildNodeEntry rename(String oldName, String newName) {
        throw new NoSuchMethodError();
    }

    @Override
    public Iterator<ChildNodeEntry> getRemoved(ChildNodeEntries other) {
        throw new NoSuchMethodError();
    }
    
    @Override
    public int getMemory() {
        // assuming a fixed size of 1000 entries, each with 100 bytes, plus 100
        // bytes overhead
        int memory = 100 + 1000 * 100;
        return memory;
    }
    
}
