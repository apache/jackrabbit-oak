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
import org.apache.jackrabbit.mk.store.RevisionProvider;
import org.apache.jackrabbit.mk.util.UnmodifiableIterator;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 *
 */
public class StoredNode extends AbstractNode {

    private final Id id;

    public StoredNode(Id id, RevisionProvider provider) {
        super(provider);
        this.id = id;
    }
    
    public StoredNode(Id id, Node node, RevisionProvider provider) {
        super(node, provider);
        this.id = id;
    }

    public Id getId() {
        return id;
    }

    public Map<String, String> getProperties() {
        return Collections.unmodifiableMap(properties);
    }

    public Iterator<ChildNodeEntry> getChildNodeEntries(int offset, int count) {
        return new UnmodifiableIterator<ChildNodeEntry>(super.getChildNodeEntries(offset, count));
    }

    public Iterator<String> getChildNodeNames(int offset, int count) {
        return new UnmodifiableIterator<String>(super.getChildNodeNames(offset, count));
    }

    public void deserialize(Binding binding) throws Exception {
        Binding.StringEntryIterator iter = binding.readStringMap(":props");
        while (iter.hasNext()) {
            Binding.StringEntry entry = iter.next();
            properties.put(entry.getKey(), entry.getValue());
        }
        boolean inlined = binding.readIntValue(":inlined") != 0;
        if (inlined) {
            childEntries = ChildNodeEntriesMap.deserialize(binding);
        } else {
            childEntries = ChildNodeEntriesTree.deserialize(provider, binding);
        }
    }
}
