/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.property.strategy;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ENTRY_COUNT_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.MultiStringPropertyState;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.ApproximateCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An IndexStoreStrategy implementation that saves the unique node in a single property.<br>
 * This should reduce the number of nodes in the repository, and speed up access.<br>
 * <br>
 * For example for a node that is under {@code /test/node}, the index
 * structure will be {@code /oak:index/index/@key}:
 */
public class UniqueEntryStoreStrategy implements IndexStoreStrategy {

    static final Logger LOG = LoggerFactory.getLogger(UniqueEntryStoreStrategy.class);

    private final String indexName;

    public UniqueEntryStoreStrategy() {
        this(INDEX_CONTENT_NODE_NAME);
    }

    public UniqueEntryStoreStrategy(String indexName) {
        this.indexName = indexName;
    }

    @Override
    public void update(
            NodeBuilder index, String path,
            @Nullable final String indexName,
            @Nullable final NodeBuilder indexMeta,
            Set<String> beforeKeys, Set<String> afterKeys) {
        for (String key : beforeKeys) {
            remove(index, key, path);
        }
        for (String key : afterKeys) {
            insert(index, key, path);
        }
    }

    private static void remove(NodeBuilder index, String key, String value) {
        ApproximateCounter.adjustCountSync(index, -1);
        NodeBuilder builder = index.getChildNode(key);
        if (builder.exists()) {
            // there could be (temporarily) multiple entries
            // we need to remove the right one
            PropertyState s = builder.getProperty("entry");
            if (s.count() == 1) {
                builder.remove();
            } else {
                ArrayList<String> list = new ArrayList<String>();
                for (int i = 0; i < s.count(); i++) {
                    String r = s.getValue(Type.STRING, i);
                    if (!r.equals(value)) {
                        list.add(r);
                    }
                }
                PropertyState s2 = MultiStringPropertyState.stringProperty("entry", list);
                builder.setProperty(s2);
            }
        }
    }
    
    private static void insert(NodeBuilder index, String key, String value) {
        ApproximateCounter.adjustCountSync(index, 1);
        NodeBuilder k = index.child(key);
        ArrayList<String> list = new ArrayList<String>();
        list.add(value);
        if (k.hasProperty("entry")) {
            // duplicate key (to detect duplicate entries)
            // this is just set temporarily,
            // while trying to add a duplicate entry
            PropertyState s = k.getProperty("entry");
            for (int i = 0; i < s.count(); i++) {
                String r = s.getValue(Type.STRING, i);
                if (!list.contains(r)) {
                    list.add(r);
                }
            }
        }
        PropertyState s2 = MultiStringPropertyState.stringProperty("entry", list);
        k.setProperty(s2);
    }

    @Override
    public Iterable<String> query(final Filter filter, final String indexName, 
            final NodeState indexMeta, final Iterable<String> values) {
        final NodeState index = indexMeta.getChildNode(getIndexNodeName());
        return new Iterable<String>() {
            @Override
            public Iterator<String> iterator() {
                if (values == null) {
                    return new Iterator<String>() {
                        
                        Iterator<? extends ChildNodeEntry> it = index.getChildNodeEntries().iterator();

                        @Override
                        public boolean hasNext() {
                            return it.hasNext();
                        }

                        @Override
                        public String next() {
                            PropertyState s = it.next().getNodeState().getProperty("entry");
                            return s.getValue(Type.STRING, 0);
                        }

                        @Override
                        public void remove() {
                            it.remove();
                        }
                        
                    };
                }
                ArrayList<String> list = new ArrayList<String>();
                for (String p : values) {
                    NodeState key = index.getChildNode(p);
                    if (key.exists()) {
                        // we have an entry for this value, so use it
                        PropertyState s = key.getProperty("entry");
                        String v = s.getValue(Type.STRING, 0);
                        list.add(v);
                    }
                }
                return list.iterator();
            }
        };
    }

    @Override
    public boolean exists(NodeBuilder index, String key) {
        return index.hasChildNode(key);
    }

    @Override
    public long count(NodeState root, NodeState indexMeta, Set<String> values, int max) {
        NodeState index = indexMeta.getChildNode(getIndexNodeName());
        long count = 0;
        if (values == null) {
            PropertyState ec = indexMeta.getProperty(ENTRY_COUNT_PROPERTY_NAME);
            if (ec != null) {
                count = ec.getValue(Type.LONG);
                if (count >= 0) {
                    return count;
                }
            }
            if (count == 0) {
                long approxCount = ApproximateCounter.getCountSync(index);
                if (approxCount != -1) {
                    return approxCount;
                }
            }
            count = 1 + index.getChildNodeCount(max);
            // "is not null" queries typically read more data
            count *= 10;
        } else if (values.size() == 1) {
            NodeState k = index.getChildNode(values.iterator().next());
            if (k.exists()) {
                count = k.getProperty("entry").count();
            } else {
                count = 0;
            }
        } else {
            count = values.size();
        }
        return count;
    }

    @Override
    public long count(final Filter filter, NodeState root, NodeState indexMeta, Set<String> values, int max) {
        return count(root, indexMeta, values, max);
    }

    @Override
    public String getIndexNodeName() {
        return indexName;
    }
}
