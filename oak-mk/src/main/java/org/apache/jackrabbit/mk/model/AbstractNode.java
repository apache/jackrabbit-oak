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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
            for (Iterator<ChildNode> it = other.getChildNodeEntries(0, -1); it.hasNext(); ) {
                ChildNode cne = it.next();
                this.childEntries.add(cne);
            }
        }
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public ChildNode getChildNodeEntry(String name) {
        return childEntries.get(name);
    }

    public Iterator<String> getChildNodeNames(int offset, int count) {
        return childEntries.getNames(offset, count);
    }

    public int getChildNodeCount() {
        return childEntries.getCount();
    }

    public Iterator<ChildNode> getChildNodeEntries(int offset, int count) {
        return childEntries.getEntries(offset, count);
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
