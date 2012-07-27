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

import org.apache.jackrabbit.mk.store.PersistHook;
import org.apache.jackrabbit.mk.store.RevisionProvider;
import org.apache.jackrabbit.mk.store.RevisionStore;

import java.util.Iterator;

/**
 *
 */
public class MutableNode extends AbstractNode implements PersistHook {

    public MutableNode(RevisionProvider provider) {
        super(provider);
    }

    public MutableNode(Node other, RevisionProvider provider) {
        super(other, provider);
    }

    public ChildNodeEntry add(ChildNodeEntry newEntry) {
        ChildNodeEntry existing = childEntries.add(newEntry);
        if (childEntries.getCount() > ChildNodeEntries.CAPACITY_THRESHOLD
                && childEntries.inlined()) {
            ChildNodeEntries entries = new ChildNodeEntriesTree(provider);
            Iterator<ChildNodeEntry> iter = childEntries.getEntries(0, -1);
            while (iter.hasNext()) {
                entries.add(iter.next());
            }
            childEntries = entries;
        }
        return existing;
    }
    
    public ChildNodeEntry remove(String name) {
        return childEntries.remove(name);
    }

    public ChildNodeEntry rename(String oldName, String newName) {
        return childEntries.rename(oldName, newName);
    }

    //----------------------------------------------------------< PersistHook >

    @Override
    public void prePersist(RevisionStore store, RevisionStore.PutToken token) throws Exception {
        if (!childEntries.inlined()) {
            // persist dirty buckets
            ((ChildNodeEntriesTree) childEntries).persistDirtyBuckets(store, token);
        }
    }

    @Override
    public void postPersist(RevisionStore store, RevisionStore.PutToken token) throws Exception {
        // there's nothing to do
    }
}
