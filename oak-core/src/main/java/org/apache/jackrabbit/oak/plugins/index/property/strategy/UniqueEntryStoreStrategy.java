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
import java.util.function.Consumer;

import javax.annotation.Nullable;

import com.google.common.base.Supplier;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.MultiStringPropertyState;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.plugins.index.counter.ApproximateCounter;
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

    private static final Consumer<NodeBuilder> NOOP = (nb) -> {};

    private final String indexName;

    private final Consumer<NodeBuilder> insertCallback;

    public UniqueEntryStoreStrategy() {
        this(INDEX_CONTENT_NODE_NAME);
    }

    public UniqueEntryStoreStrategy(String indexName) {
        this(indexName, NOOP);
    }

    public UniqueEntryStoreStrategy(String indexName, Consumer<NodeBuilder> insertCallback) {
        this.indexName = indexName;
        this.insertCallback = insertCallback;
    }

    @Override
    public void update(
            Supplier<NodeBuilder> index, String path,
            @Nullable final String indexName,
            @Nullable final NodeBuilder indexMeta,
            Set<String> beforeKeys, Set<String> afterKeys) {
        for (String key : beforeKeys) {
            remove(index.get(), key, path);
        }
        for (String key : afterKeys) {
            insert(index.get(), key, path);
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
    
    private void insert(NodeBuilder index, String key, String value) {
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

        insertCallback.accept(k);
    }

    @Override
    public Iterable<String> query(final Filter filter, final String indexName, 
            final NodeState indexMeta, final Iterable<String> values) {
        return query0(filter, indexName, indexMeta, values, new HitProducer<String>() {
            @Override
            public String produce(NodeState indexHit, String pathName) {
                PropertyState s = indexHit.getProperty("entry");
                return s.getValue(Type.STRING, 0);
            }
        });        
    }

    
    
    /**
     * Search for a given set of values, returning <tt>IndexEntry</tt> results
     * 
     * @param filter the filter (can optionally be used for optimized query execution)
     * @param indexName the name of the index (for logging)
     * @param indexMeta the index metadata node (may not be null)
     * @param values values to look for (null to check for property existence)
     * @return an iterator of index entries
     * 
     * @throws UnsupportedOperationException if the operation is not supported
     */
    public Iterable<IndexEntry> queryEntries(Filter filter, String indexName, NodeState indexMeta,
            Iterable<String> values) {
        return query0(filter, indexName, indexMeta, values, new HitProducer<IndexEntry>() {
            @Override
            public IndexEntry produce(NodeState indexHit, String pathName) {
                PropertyState s = indexHit.getProperty("entry");
                return new IndexEntry(s.getValue(Type.STRING, 0), pathName);
            }
        });
    }

    private <T> Iterable<T> query0(Filter filter, String indexName, NodeState indexMeta,
            Iterable<String> values, HitProducer<T> prod) {
        final NodeState index = indexMeta.getChildNode(getIndexNodeName());
        return new Iterable<T>() {
            @Override
            public Iterator<T> iterator() {
                if (values == null) {
                    return new Iterator<T>() {
                        
                        Iterator<? extends ChildNodeEntry> it = index.getChildNodeEntries().iterator();
                        
                        @Override
                        public boolean hasNext() {
                            return it.hasNext();
                        }
                        
                        @Override
                        public T next() {
                            ChildNodeEntry indexEntry = it.next();
                            
                            return prod.produce(indexEntry.getNodeState(), indexEntry.getName());
                        }
                        
                        @Override
                        public void remove() {
                            it.remove();
                        }
                        
                    };
                }
                ArrayList<T> list = new ArrayList<>();
                for (String p : values) {
                    NodeState key = index.getChildNode(p);
                    if (key.exists()) {
                        // we have an entry for this value, so use it
                        list.add(prod.produce(key, p));
                    }
                }
                return list.iterator();
            }
        };
    }
    
    @Override
    public boolean exists(Supplier<NodeBuilder> index, String key) {
        return index.get().hasChildNode(key);
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

    /**
     * Creates a specific type of "hit" to return from the query methods
     * 
     * <p>Use primarily to reduce duplication when the query algorithms execute mostly the same steps but return different objects.</p>
     * 
     * @param <T> The type of Hit to produce
     */
    private interface HitProducer<T> {
        
        /**
         * Invoked when a matching index entry is found 
         * 
         * @param indexHit the index node
         * @param propertyValue the value of the property
         * @return the value produced for the specific "hit" 
         */
        T produce(NodeState indexHit, String propertyValue);
    }
    
}
