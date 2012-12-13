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
import org.apache.jackrabbit.mk.store.CacheObject;
import org.apache.jackrabbit.mk.store.RevisionProvider;
import org.apache.jackrabbit.mk.store.RevisionStore;
import org.apache.jackrabbit.mk.util.AbstractFilteringIterator;
import org.apache.jackrabbit.mk.util.AbstractRangeIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 *
 */
public class ChildNodeEntriesTree implements ChildNodeEntries, CacheObject {

    protected static final List<ChildNodeEntry> EMPTY = Collections.emptyList();
    
    protected int count;
    
    protected RevisionProvider revProvider;

    // array of *immutable* IndexEntry objects
    protected IndexEntry[] index = new IndexEntry[1024];  // 2^10

    ChildNodeEntriesTree(RevisionProvider revProvider) {
        this.revProvider = revProvider;
    }

    @Override
    public boolean inlined() {
        return false;
    }

    //------------------------------------------------------------< overrides >

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof ChildNodeEntriesTree) {
            ChildNodeEntriesTree other = (ChildNodeEntriesTree) obj;
            return Arrays.equals(index, other.index);
        }
        return false;
    }

    @Override
    public Object clone() {
        ChildNodeEntriesTree clone = null;
        try {
            clone = (ChildNodeEntriesTree) super.clone();
        } catch (CloneNotSupportedException e) {
            // can't possibly get here
        }
        // shallow clone of array of immutable IndexEntry objects
        clone.index = index.clone();
        return clone;
    }

    //-------------------------------------------------------------< read ops >

    @Override
    public int getCount() {
        return count;
    }

    @Override
    public ChildNodeEntry get(String name) {
        IndexEntry entry = index[keyToIndex(name)];
        if (entry == null) {
            return null;
        }
        if (entry instanceof ChildNodeEntry) {
            ChildNodeEntry cne = (ChildNodeEntry) entry;
            return cne.getName().equals(name) ? cne : null;
        } else if (entry instanceof BucketInfo) {
            BucketInfo bi = (BucketInfo) entry;
            ChildNodeEntries entries = retrieveBucket(bi.getId());
            return entries == null ? null : entries.get(name);
        } else {
            // dirty bucket
            Bucket bucket = (Bucket) entry;
            return bucket.get(name);
        }
    }

    @Override
    public Iterator<String> getNames(int offset, int cnt) {
        if (offset < 0 || cnt < -1) {
            throw new IllegalArgumentException();
        }

        if (offset >= count || cnt == 0) {
            List<String> empty = Collections.emptyList();
            return empty.iterator();
        }

        if (cnt == -1 || (offset + cnt) > count) {
            cnt = count - offset;
        }

        return new AbstractRangeIterator<String>(getEntries(offset, cnt), 0, -1) {
            @Override
            protected String doNext() {
                ChildNodeEntry cne = (ChildNodeEntry) it.next();
                return cne.getName();
            }
        };
    }

    @Override
    public Iterator<ChildNodeEntry> getEntries(int offset, int cnt) {
        if (offset < 0 || cnt < -1) {
            throw new IllegalArgumentException();
        }

        if (offset >= count || cnt == 0) {
            return EMPTY.iterator();
        }

        int skipped = 0;
        if (cnt == -1 || (offset + cnt) > count) {
            cnt = count - offset;
        }
        ArrayList<ChildNodeEntry> list = new ArrayList<ChildNodeEntry>(cnt);
        for (IndexEntry e : index) {
            if (e != null) {
                if (skipped + e.getSize() <= offset) {
                    skipped += e.getSize();
                } else {
                    if (e instanceof NodeInfo) {
                        list.add((NodeInfo) e);
                    } else if (e instanceof BucketInfo) {
                        BucketInfo bi = (BucketInfo) e;
                        ChildNodeEntries bucket = retrieveBucket(bi.getId());
                        for (Iterator<ChildNodeEntry> it =
                                     bucket.getEntries(offset - skipped, cnt - list.size());
                             it.hasNext(); ) {
                            list.add(it.next());
                        }
                        skipped = offset;
                    } else {
                        // dirty bucket
                        Bucket bucket = (Bucket) e;
                        for (Iterator<ChildNodeEntry> it =
                                     bucket.getEntries(offset - skipped, cnt - list.size());
                             it.hasNext(); ) {
                            list.add(it.next());
                        }
                        skipped = offset;
                    }
                    if (list.size() == cnt) {
                        break;
                    }
                }
            }
        }

        return list.iterator();
    }

    //------------------------------------------------------------< write ops >

    @Override
    public ChildNodeEntry add(ChildNodeEntry entry) {
        int idx = keyToIndex(entry.getName());
        IndexEntry ie = index[idx];
        if (ie == null) {
            index[idx] = new NodeInfo(entry.getName(), entry.getId());
            count++;
            return null;
        }
        if (ie instanceof ChildNodeEntry) {
            ChildNodeEntry existing = (ChildNodeEntry) ie;
            if (existing.getName().equals(entry.getName())) {
                index[idx] = new NodeInfo(entry.getName(), entry.getId());
                return existing;
            } else {
                Bucket bucket = new Bucket();
                bucket.add(existing);
                bucket.add(entry);
                index[idx] = bucket;
                count++;
                return null;
            }
        } 
        
        Bucket bucket;
        if (ie instanceof BucketInfo) {
            BucketInfo bi = (BucketInfo) ie;
            bucket = new Bucket(retrieveBucket(bi.getId()));
        } else {
            // dirty bucket
            bucket = (Bucket) ie;
        }

        ChildNodeEntry existing = bucket.add(entry);
        if (entry.equals(existing)) {
            // no-op
            return existing;
        }
        index[idx] = bucket;
        if (existing == null) {
            // new entry
            count++;
        }
        return existing;
    }

    @Override
    public ChildNodeEntry remove(String name) {
        int idx = keyToIndex(name);
        IndexEntry ie = index[idx];
        if (ie == null) {
            return null;
        }
        if (ie instanceof ChildNodeEntry) {
            ChildNodeEntry existing = (ChildNodeEntry) ie;
            if (existing.getName().equals(name)) {
                index[idx] = null;
                count--;
                return existing;
            } else {
                return null;
            }
        }
        
        Bucket bucket;
        if (ie instanceof BucketInfo) {
            BucketInfo bi = (BucketInfo) ie;
            bucket = new Bucket(retrieveBucket(bi.getId()));
        } else {
            // dirty bucket
            bucket = (Bucket) ie;
        }

        ChildNodeEntry existing = bucket.remove(name);
        if (existing == null) {
            return null;
        }
        if (bucket.getCount() == 0) {
            index[idx] = null;
        } else if (bucket.getCount() == 1) {
            // inline single remaining entry
            ChildNodeEntry remaining = bucket.getEntries(0, 1).next();
            index[idx] = new NodeInfo(remaining.getName(), remaining.getId());
        } else {
            index[idx] = bucket;
        }
        count--;
        return existing;
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

    //-------------------------------------------------------------< diff ops >

    @Override
    public Iterator<ChildNodeEntry> getAdded(final ChildNodeEntries other) {
        if (other instanceof ChildNodeEntriesTree) {
            List<ChildNodeEntry> added = new ArrayList<ChildNodeEntry>();
            ChildNodeEntriesTree otherEntries = (ChildNodeEntriesTree) other;
            for (int i = 0; i < index.length; i++) {
                IndexEntry ie1 = index[i];
                IndexEntry ie2 = otherEntries.index[i];
                if (! (ie1 == null ? ie2 == null : ie1.equals(ie2))) {
                    // index entries aren't equal
                    if (ie1 == null) {
                        // this index entry in null => other must be non-null
                        if (ie2 instanceof NodeInfo) {
                            added.add((ChildNodeEntry) ie2);
                        } else if (ie2 instanceof BucketInfo) {
                            BucketInfo bi = (BucketInfo) ie2;
                            ChildNodeEntries bucket = retrieveBucket(bi.getId());
                            for (Iterator<ChildNodeEntry> it = bucket.getEntries(0, -1);
                                 it.hasNext(); ) {
                                added.add(it.next());
                            }
                        } else {
                            // dirty bucket
                            Bucket bucket = (Bucket) ie2;
                            for (Iterator<ChildNodeEntry> it = bucket.getEntries(0, -1);
                                 it.hasNext(); ) {
                                added.add(it.next());
                            }
                        }
                    } else if (ie2 != null) {
                        // both this and other index entry are non-null
                        ChildNodeEntriesMap bucket1;
                        if (ie1 instanceof NodeInfo) {
                            bucket1 = new ChildNodeEntriesMap();
                            bucket1.add((ChildNodeEntry) ie1);
                        } else if (ie1 instanceof BucketInfo) {
                            BucketInfo bi = (BucketInfo) ie1;
                            bucket1 = retrieveBucket(bi.getId());
                        } else {
                            // dirty bucket
                            bucket1 = (Bucket) ie1;
                        }
                        ChildNodeEntriesMap bucket2;
                        if (ie2 instanceof NodeInfo) {
                            bucket2 = new ChildNodeEntriesMap();
                            bucket2.add((ChildNodeEntry) ie2);
                        } else if (ie2 instanceof BucketInfo) {
                            BucketInfo bi = (BucketInfo) ie2;
                            bucket2 = retrieveBucket(bi.getId());
                        } else {
                            // dirty bucket
                            bucket2 = (Bucket) ie2;
                        }

                        for (Iterator<ChildNodeEntry> it = bucket1.getAdded(bucket2);
                             it.hasNext(); ) {
                            added.add(it.next());
                        }
                    }
                }
            }
            return added.iterator();
        } else {
            // todo optimize
            return new AbstractFilteringIterator<ChildNodeEntry>(other.getEntries(0, -1)) {
                @Override
                protected boolean include(ChildNodeEntry entry) {
                    return get(entry.getName()) == null;
                }
            };
        }
    }

    @Override
    public Iterator<ChildNodeEntry> getRemoved(final ChildNodeEntries other) {
        if (other instanceof ChildNodeEntriesTree) {
            List<ChildNodeEntry> removed = new ArrayList<ChildNodeEntry>();
            ChildNodeEntriesTree otherEntries = (ChildNodeEntriesTree) other;
            for (int i = 0; i < index.length; i++) {
                IndexEntry ie1 = index[i];
                IndexEntry ie2 = otherEntries.index[i];
                if (! (ie1 == null ? ie2 == null : ie1.equals(ie2))) {
                    // index entries aren't equal
                    if (ie2 == null) {
                        // other index entry is null => this must be non-null
                        if (ie1 instanceof NodeInfo) {
                            removed.add((ChildNodeEntry) ie1);
                        } else if (ie1 instanceof BucketInfo) {
                            BucketInfo bi = (BucketInfo) ie1;
                            ChildNodeEntries bucket = retrieveBucket(bi.getId());
                            for (Iterator<ChildNodeEntry> it = bucket.getEntries(0, -1);
                                 it.hasNext(); ) {
                                removed.add(it.next());
                            }
                        } else {
                            // dirty bucket
                            Bucket bucket = (Bucket) ie1;
                            for (Iterator<ChildNodeEntry> it = bucket.getEntries(0, -1);
                                 it.hasNext(); ) {
                                removed.add(it.next());
                            }
                        }
                    } else if (ie1 != null) {
                        // both this and other index entry are non-null
                        ChildNodeEntriesMap bucket1;
                        if (ie1 instanceof NodeInfo) {
                            bucket1 = new ChildNodeEntriesMap();
                            bucket1.add((ChildNodeEntry) ie1);
                        } else if (ie1 instanceof BucketInfo) {
                            BucketInfo bi = (BucketInfo) ie1;
                            bucket1 = retrieveBucket(bi.getId());
                        } else {
                            // dirty bucket
                            bucket1 = (Bucket) ie1;
                        }
                        ChildNodeEntriesMap bucket2;
                        if (ie2 instanceof NodeInfo) {
                            bucket2 = new ChildNodeEntriesMap();
                            bucket2.add((ChildNodeEntry) ie2);
                        } else if (ie2 instanceof BucketInfo) {
                            BucketInfo bi = (BucketInfo) ie2;
                            bucket2 = retrieveBucket(bi.getId());
                        } else {
                            // dirty bucket
                            bucket2 = (Bucket) ie2;
                        }

                        for (Iterator<ChildNodeEntry> it = bucket1.getRemoved(bucket2);
                             it.hasNext(); ) {
                            removed.add(it.next());
                        }
                    }
                }
            }
            return removed.iterator();
        } else {
            // todo optimize
            return new AbstractFilteringIterator<ChildNodeEntry>(getEntries(0, -1)) {
                @Override
                protected boolean include(ChildNodeEntry entry) {
                    return other.get(entry.getName()) == null;
                }
            };
        }
    }

    @Override
    public Iterator<ChildNodeEntry> getModified(final ChildNodeEntries other) {
        if (other instanceof ChildNodeEntriesTree) {
            List<ChildNodeEntry> modified = new ArrayList<ChildNodeEntry>();
            ChildNodeEntriesTree otherEntries = (ChildNodeEntriesTree) other;
            for (int i = 0; i < index.length; i++) {
                IndexEntry ie1 = index[i];
                IndexEntry ie2 = otherEntries.index[i];
                if (ie1 != null && ie2 != null && !ie1.equals(ie2)) {
                    // index entries are non-null and not equal
                    if (ie1 instanceof NodeInfo
                            && ie2 instanceof NodeInfo) {
                        NodeInfo ni1 = (NodeInfo) ie1;
                        NodeInfo ni2 = (NodeInfo) ie2;
                        if (ni1.getName().equals(ni2.getName())
                                && !ni1.getId().equals(ni2.getId())) {
                            modified.add(ni1);
                            continue;
                        }
                    }

                    ChildNodeEntriesMap bucket1;
                    if (ie1 instanceof NodeInfo) {
                        bucket1 = new ChildNodeEntriesMap();
                        bucket1.add((ChildNodeEntry) ie1);
                    } else if (ie1 instanceof BucketInfo) {
                        BucketInfo bi = (BucketInfo) ie1;
                        bucket1 = retrieveBucket(bi.getId());
                    } else {
                        // dirty bucket
                        bucket1 = (Bucket) ie1;
                    }
                    ChildNodeEntriesMap bucket2;
                    if (ie2 instanceof NodeInfo) {
                        bucket2 = new ChildNodeEntriesMap();
                        bucket2.add((ChildNodeEntry) ie2);
                    } else if (ie2 instanceof BucketInfo) {
                        BucketInfo bi = (BucketInfo) ie2;
                        bucket2 = retrieveBucket(bi.getId());
                    } else {
                        // dirty bucket
                        bucket2 = (Bucket) ie2;
                    }

                    for (Iterator<ChildNodeEntry> it = bucket1.getModified(bucket2);
                         it.hasNext(); ) {
                        modified.add(it.next());
                    }
                }
            }

            return modified.iterator();
        } else {
            return new AbstractFilteringIterator<ChildNodeEntry>(getEntries(0, -1)) {
                @Override
                protected boolean include(ChildNodeEntry entry) {
                    ChildNodeEntry namesake = other.get(entry.getName());
                    return (namesake != null && !namesake.getId().equals(entry.getId()));
                }
            };
        }
    }

    //-------------------------------------------------------< implementation >
    
    protected void persistDirtyBuckets(RevisionStore store, RevisionStore.PutToken token) throws Exception {
        for (int i = 0; i < index.length; i++) {
            if (index[i] instanceof Bucket) {
                // dirty bucket
                Bucket bucket = (Bucket) index[i];
                Id id = store.putCNEMap(token, bucket);
                index[i] = new BucketInfo(id, bucket.getSize());
            }
        }
    }

    protected int keyToIndex(String key) {
        int hash = key.hashCode();
        // todo rehash? ensure optimal distribution of hash WRT to index.length
        return (hash & 0x7FFFFFFF) % index.length;
    }

    protected ChildNodeEntriesMap retrieveBucket(Id id) {
        try {
            return revProvider.getCNEMap(id);
        } catch (Exception e) {
            // todo log error and gracefully handle exception
            return new ChildNodeEntriesMap();
        }
    }

    //------------------------------------------------< serialization support >

    public void serialize(Binding binding) throws Exception {
        // TODO use binary instead of string serialization
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
                } else if (entry instanceof NodeInfo) {
                    NodeInfo ni = (NodeInfo) entry;
                    // "n<id>:<name>"
                    return new Binding.StringEntry(Integer.toString(pos), "n" + ni.getId() + ":" + ni.getName());
                } else {
                    BucketInfo bi = (BucketInfo) entry;
                    // "b<id>:<count>"
                    return new Binding.StringEntry(Integer.toString(pos), "b" + bi.getId() + ":" + bi.getSize());
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        });
    }

    static ChildNodeEntriesTree deserialize(RevisionProvider provider, Binding binding) throws Exception {
        // TODO use binary instead of string serialization
        ChildNodeEntriesTree newInstance = new ChildNodeEntriesTree(provider);
        newInstance.count = binding.readIntValue(":count");
        Binding.StringEntryIterator iter = binding.readStringMap(":index");
        int pos = -1;
        while (iter.hasNext()) {
            Binding.StringEntry entry = iter.next();
            ++pos;
            // deserialize index array entry
            assert(pos == Integer.parseInt(entry.getKey()));
            if (entry.getValue().length() == 0) {
                // ""
                newInstance.index[pos] = null;
            } else if (entry.getValue().charAt(0) == 'n') {
                // "n<id>:<name>"
                String value = entry.getValue().substring(1);
                int i = value.indexOf(':');
                String id = value.substring(0, i);
                String name = value.substring(i + 1);
                newInstance.index[pos] = new NodeInfo(name, Id.fromString(id));
            } else {
                // "b<id>:<count>"
                String value = entry.getValue().substring(1);
                int i = value.indexOf(':');
                String id = value.substring(0, i);
                int count = Integer.parseInt(value.substring(i + 1));
                newInstance.index[pos] = new BucketInfo(Id.fromString(id), count);
            }
        }
        return newInstance;
    }

    //--------------------------------------------------------< inner classes >

    protected static interface IndexEntry {
        // number of entries
        int getSize();
    }

    protected static class BucketInfo implements IndexEntry {

        // bucket id
        private final Id id;

        // number of bucket entries
        private final int size;

        protected BucketInfo(Id id, int size) {
            this.id = id;
            this.size = size;
        }

        public Id getId() {
            return id;
        }

        public int getSize() {
            return size;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj instanceof BucketInfo) {
                BucketInfo other = (BucketInfo) obj;
                return (size == other.size && id == null ? other.id == null : id.equals(other.id));
            }
            return false;
        }
    }

    protected static class Bucket extends ChildNodeEntriesMap implements IndexEntry {

        protected Bucket() {
        }

        protected Bucket(ChildNodeEntriesMap other) {
            super(other);
        }

        @Override
        public int getSize() {
            return getCount();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj instanceof Bucket) {
                return super.equals(obj);
            }
            return false;
        }
    }

    protected static class NodeInfo extends ChildNodeEntry implements IndexEntry {

        public NodeInfo(String name, Id id) {
            super(name, id);
        }

        public int getSize() {
            return 1;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj instanceof NodeInfo) {
                return super.equals(obj);
            }
            return false;
        }
    }
    
    @Override
    public int getMemory() {
        // assuming a fixed size of 1000 entries, each with 100 bytes, plus 100 bytes overhead
        int memory = 100 + 1000 * 100;
        return memory;
    }

}
