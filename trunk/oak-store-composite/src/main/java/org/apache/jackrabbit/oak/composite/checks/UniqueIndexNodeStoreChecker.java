/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.composite.checks;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.UNIQUE_PROPERTY_NAME;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.composite.MountedNodeStore;
import org.apache.jackrabbit.oak.plugins.index.property.Multiplexers;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.IndexEntry;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.UniqueEntryStoreStrategy;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

/**
 * Checker that ensures the consistency of unique entries in the various mounts
 * 
 * <p>For all unique indexes, it checks that the uniqueness constraint holds when
 * taking into account the combined index from all the mounts, including the global one.</p>
 * 
 * <p>Being a one-off check, it does not strictly implement the {@link #check(MountedNodeStore, Tree, ErrorHolder, Context)}
 * contract in terms of navigating the specified tree, but instead accesses the index definitions node directly
 * on first access and skips all subsequent executions.</p>
 *
 */
@Component
@Service(MountedNodeStoreChecker.class)
public class UniqueIndexNodeStoreChecker implements MountedNodeStoreChecker<UniqueIndexNodeStoreChecker.Context> {
    
    private static final Logger LOG = LoggerFactory.getLogger(UniqueIndexNodeStoreChecker.class);
    
    @Override
    public Context createContext(NodeStore globalStore, MountInfoProvider mip) {

        Context ctx = new Context(mip);
        
        // read definitions from oak:index, and pick all unique indexes
        NodeState indexDefs = globalStore.getRoot().getChildNode(INDEX_DEFINITIONS_NAME);
        for ( ChildNodeEntry indexDef : indexDefs.getChildNodeEntries() ) {
            if ( indexDef.getNodeState().hasProperty(UNIQUE_PROPERTY_NAME) &&
                    indexDef.getNodeState().getBoolean(UNIQUE_PROPERTY_NAME) ) {
                ctx.add(indexDef, mip.getDefaultMount(), indexDefs);
                ctx.track(new MountedNodeStore(mip.getDefaultMount() , globalStore));
            }
        }
        
        return ctx;
    }
    
    @Override
    public boolean check(MountedNodeStore mountedStore, Tree tree, ErrorHolder errorHolder, Context context) {
        context.track(mountedStore);
        
        // gather index definitions owned by this mount
        NodeState indexDefs = mountedStore.getNodeStore().getRoot().getChildNode(INDEX_DEFINITIONS_NAME);
        
        for ( ChildNodeEntry indexDef : indexDefs.getChildNodeEntries() ) {
            if ( indexDef.getNodeState().hasProperty(UNIQUE_PROPERTY_NAME) &&
                    indexDef.getNodeState().getBoolean(UNIQUE_PROPERTY_NAME) ) {
                
                String mountIndexDefName = Multiplexers.getNodeForMount(mountedStore.getMount(), INDEX_CONTENT_NODE_NAME);
                
                NodeState mountIndexDef = indexDef.getNodeState().getChildNode(mountIndexDefName);
                
                if ( mountIndexDef.exists() ) {
                    context.add(indexDef, mountedStore.getMount(), indexDefs);
                }
            }
        }

        // execute checks
        context.runChecks(context, errorHolder);
        
        return false;
    }
    
    static class Context {
        private final MountInfoProvider mip;
        private final Map<String, IndexCombination> combinations = new HashMap<>();
        private final Map<String, MountedNodeStore> mountedNodeStoresByName = Maps.newHashMap();
        
        Context(MountInfoProvider mip) {
            this.mip = mip;
        }

        public void track(MountedNodeStore mountedNodeStore) {
            mountedNodeStoresByName.put(mountedNodeStore.getMount().getName(), mountedNodeStore);
        }

        public void add(ChildNodeEntry rootIndexDef, Mount mount, NodeState indexDef) {

            IndexCombination combination = combinations.get(rootIndexDef.getName());
            if ( combination == null ) {
                combination = new IndexCombination(rootIndexDef);
                combinations.put(rootIndexDef.getName(), combination);
            }
            
            combination.addEntry(mount, indexDef);
        }
        
        public MountInfoProvider getMountInfoProvider() {
            
            return mip;
        }
        
        public void runChecks(Context context, ErrorHolder errorHolder) {
            for ( IndexCombination combination: combinations.values() ) {
                combination.runCheck(context, errorHolder);
            }
        }
    }
    
    static class IndexCombination {
        private final ChildNodeEntry rootIndexDef;
        private final Map<Mount, NodeState> indexEntries = Maps.newHashMap();
        private final List<Mount[]> checked = new ArrayList<>();
        // bounded as the ErrorHolder has a reasonable limit of entries before it fails immediately
        private final Set<String> reportedConflictingValues = new HashSet<>();
        
        IndexCombination(ChildNodeEntry rootIndexDef) {
            this.rootIndexDef = rootIndexDef;
        }
        
        public void addEntry(Mount mount, NodeState indexDef) {
            
            if ( !indexEntries.containsKey(mount) )
                indexEntries.put(mount, indexDef);
        }
        
        public void runCheck(Context context, ErrorHolder errorHolder) {
            
            for ( Map.Entry<Mount, NodeState> indexEntry : indexEntries.entrySet() ) {
                for ( Map.Entry<Mount, NodeState> indexEntry2 : indexEntries.entrySet() ) {
                    
                    // same entry, skip
                    if ( indexEntry.getKey().equals(indexEntry2.getKey()) ) {
                        continue;
                    }
                    
                    // ensure we don't check twice
                    if ( wasChecked(indexEntry.getKey(), indexEntry2.getKey() )) {
                        continue;
                    }
                    
                    check(indexEntry, indexEntry2, context, errorHolder);
                    
                    recordChecked(indexEntry.getKey(), indexEntry2.getKey());
                }
            }
        }

        private boolean wasChecked(Mount first, Mount second) {

            for ( Mount[] checkedEntry : checked ) {
                if ( ( checkedEntry[0].equals(first) && checkedEntry[1].equals(second) ) ||
                        ( checkedEntry[1].equals(first) && checkedEntry[0].equals(second))) {
                    return true;
                }
            }
            
            return false;
        }
        
        private void recordChecked(Mount first, Mount second) {
            
            checked.add(new Mount[] { first, second });
        }
        
        private void check(Entry<Mount, NodeState> indexEntry, Entry<Mount, NodeState> indexEntry2, Context ctx, ErrorHolder errorHolder) {

            String indexName = rootIndexDef.getName();
            
            // optimisation: sort strategies to ensuer that we query all the entries from the mount and check them
            // against the default. this way we'll run a significantly smaller number of checks. The assumption is
            // that the non-default mount will always hold a larger number of entries
            TreeSet<StrategyWrapper> wrappers = new TreeSet<>();
            wrappers.add(getWrapper(indexEntry, indexName, ctx));
            wrappers.add(getWrapper(indexEntry2, indexName, ctx));
            
            StrategyWrapper wrapper = wrappers.first();
            StrategyWrapper wrapper2 = wrappers.last();
            
            LOG.info("Checking index definitions for {} between mounts {} and {}", indexName, wrapper.mount.getName(), wrapper2.mount.getName());
            
            for ( IndexEntry hit : wrapper.queryAll() ) {
                Optional<IndexEntry> result = wrapper2.queryOne(hit.getPropertyValue());
                if ( result.isPresent() ) {
                    IndexEntry hit2 = result.get();
                    if ( reportedConflictingValues.add(hit.getPropertyValue())) {
                        errorHolder.report(wrapper.nodeStore, hit.getPath(), wrapper2.nodeStore, hit2.getPath(), 
                                hit.getPropertyValue(), "duplicate unique index entry");
                    }
                }
            }
        }

        private StrategyWrapper getWrapper(Entry<Mount, NodeState> indexEntry, String indexName, Context ctx) {

            NodeState indexNode = indexEntry.getValue();
            Mount mount = indexEntry.getKey();
            MountedNodeStore mountedNodeStore = ctx.mountedNodeStoresByName.get(mount.getName());
            
            UniqueEntryStoreStrategy strategy = mount.isDefault() ? new UniqueEntryStoreStrategy() : 
                new UniqueEntryStoreStrategy(Multiplexers.getNodeForMount(mount, INDEX_CONTENT_NODE_NAME));
            
            return new StrategyWrapper(strategy, indexNode.getChildNode(indexName), indexName, mountedNodeStore);
        }
    }
    
    private static class StrategyWrapper implements Comparable<StrategyWrapper> {
        
        private static final int PRIORITY_DEFAULT = 0;
        private static final int PRIORITY_MOUNT = 100;
        
        private final UniqueEntryStoreStrategy strategy;
        private final int priority;
        private final NodeState indexNode;
        private final Mount mount;
        private final MountedNodeStore nodeStore;
        private final String indexName;
        
        private StrategyWrapper(UniqueEntryStoreStrategy strategy, NodeState indexNode, String indexName, MountedNodeStore nodeStore) {
            this.strategy = strategy;
            this.mount = nodeStore.getMount();
            this.indexNode = indexNode;
            this.nodeStore = nodeStore;
            this.indexName = indexName;
            this.priority = mount.isDefault() ? PRIORITY_MOUNT : PRIORITY_DEFAULT;            
        }

        @Override
        public int compareTo(StrategyWrapper o) {
            int prioCmp = Integer.compare(priority, o.priority);
            if ( prioCmp != 0 ) {
                return prioCmp;
            }
            return mount.getName().compareTo(o.mount.getName());
        }
        
        public Iterable<IndexEntry> queryAll() {
            return strategy.queryEntries(Filter.EMPTY_FILTER, indexName, indexNode, null);
        }
        
        public Optional<IndexEntry> queryOne(String value) {
            
            Iterable<IndexEntry> results = strategy.queryEntries(Filter.EMPTY_FILTER, indexName, indexNode, Collections.singleton(value));
            if ( !results.iterator().hasNext() ) {
                return Optional.empty();
            }
            
            return Optional.of(results.iterator().next());
        }
    }
}
