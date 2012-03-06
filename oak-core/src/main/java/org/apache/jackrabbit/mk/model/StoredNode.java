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
import org.apache.jackrabbit.mk.store.NotFoundException;
import org.apache.jackrabbit.mk.util.UnmodifiableIterator;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 *
 */
public class StoredNode extends AbstractNode {

    private final String id;

    public static StoredNode deserialize(String id, RevisionProvider provider, Binding binding) throws Exception {
        StoredNode newInstance = new StoredNode(id, provider);
        Binding.StringEntryIterator iter = binding.readStringMap(":props");
        while (iter.hasNext()) {
            Binding.StringEntry entry = iter.next();
            newInstance.properties.put(entry.getKey(), entry.getValue());
        }
        boolean inlined = binding.readIntValue(":inlined") != 0;
        if (inlined) {
            newInstance.childEntries = ChildNodeEntriesMap.deserialize(binding);
        } else {
            newInstance.childEntries = ChildNodeEntriesTree.deserialize(provider, binding);
        }
        return newInstance;
    }

    private StoredNode(String id, RevisionProvider provider) {
        super(provider);
        this.id = id;
    }
    
    public StoredNode(String id, RevisionProvider provider, Map<String, String> properties, Iterator<ChildNodeEntry> cneIt) {
        super(provider);
        this.id = id;
        this.properties.putAll(properties);
        while (cneIt.hasNext()) {
            childEntries.add(cneIt.next());
        }
    }

    public StoredNode(String id, Node node, RevisionProvider provider) {
        super(node, provider);
        this.id = id;
    }

    public String getId() {
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

    public Iterator<ChildNode> getChildNodes(int offset, int count)
            throws Exception {
        return new UnmodifiableIterator<ChildNode>(super.getChildNodes(offset, count));
    }

    public Node getNode(String relPath) throws NotFoundException, Exception {
        Node result = super.getNode(relPath);
        if (!(result instanceof StoredNode)) {
            // todo return a StoredNode instance instead?
        }
        return result;
    }
}
