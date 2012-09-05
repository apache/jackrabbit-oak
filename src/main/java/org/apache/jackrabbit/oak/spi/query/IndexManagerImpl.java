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
package org.apache.jackrabbit.oak.spi.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexManagerImpl implements IndexManager, CommitHook {

    private static final Logger LOG = LoggerFactory
            .getLogger(IndexManagerImpl.class);

    private final String indexConfigPath;

    private final MicroKernel mk;

    private final Map<IndexFactory, String[]> indexFactories = new ConcurrentHashMap<IndexFactory, String[]>();

    public IndexManagerImpl(String indexConfigPath, MicroKernel mk,
            IndexFactory... factories) {
        this.indexConfigPath = indexConfigPath;
        this.mk = mk;
        internalRegisterIndexFactory(factories);
    }

    @Override
    public void registerIndexFactory(IndexFactory... factories) {
        internalRegisterIndexFactory(factories);
    }

    @Override
    public void unregisterIndexFactory(IndexFactory factory) {
        indexFactories.remove(factory);
    }

    private void internalRegisterIndexFactory(IndexFactory... factories) {
        if (factories == null) {
            return;
        }
        for (IndexFactory factory : factories) {
            if (indexFactories.remove(factory) != null) {
                // TODO is override allowed?
            }
            factory.init(mk);
            indexFactories.put(factory, factory.getTypes());
            LOG.debug("Registered index factory {}.", factory);
        }
    }

    /**
     * Builds a list of the existing index definitions from the repository
     * 
     */
    private static List<IndexDefinition> buildIndexDefinitions(boolean log,
            NodeState nodeState, String indexConfigPath) {

        NodeState definitions = IndexUtils.getNode(nodeState, indexConfigPath);
        if (definitions == null) {
            return Collections.emptyList();
        }

        List<IndexDefinition> defs = new ArrayList<IndexDefinition>();
        for (ChildNodeEntry c : definitions.getChildNodeEntries()) {
            IndexDefinition def = IndexUtils.getDefinition(indexConfigPath, c);
            if (def == null) {
                if (log) {
                    LOG.warn("Skipping illegal index definition '{}' @ {}",
                            c.getName(), indexConfigPath);
                }
                continue;
            }
            defs.add(def);
        }
        return defs;
    }

    @Override
    public Index getIndex(String name, NodeState nodeState) {
        if (name == null) {
            return null;
        }
        IndexDefinition id = null;
        for (IndexDefinition def : getIndexDefinitions(nodeState)) {
            if (name.equals(def.getName())) {
                id = def;
                break;
            }
        }
        return getIndex(id);
    }

    @Override
    public Index getIndex(IndexDefinition def) {
        if (def == null) {
            return null;
        }
        Iterator<IndexFactory> iterator = indexFactories.keySet().iterator();
        while (iterator.hasNext()) {
            IndexFactory factory = iterator.next();
            for (String type : factory.getTypes()) {
                if (type != null && type.equals(def.getType())) {
                    return factory.getIndex(def);
                }
            }
        }
        LOG.debug("Index definition {} doesn't have a known factory.", def);
        return null;
    }

    @Override
    public List<IndexDefinition> getIndexDefinitions(NodeState nodeState) {
        return buildIndexDefinitions(true, nodeState, indexConfigPath);
    }

    @Override
    public synchronized void close() throws IOException {
        Iterator<IndexFactory> iterator = indexFactories.keySet().iterator();
        while (iterator.hasNext()) {
            IndexFactory factory = iterator.next();
            try {
                factory.close();
            } catch (IOException e) {
                LOG.error("error closing index factory {}", factory, e);
            }
            iterator.remove();
        }
    }

    @Override
    public NodeState processCommit(NodeStore store, NodeState before,
            NodeState after) throws CommitFailedException {

        NodeState newState = after;
        for (IndexDefinition def : buildIndexDefinitions(true, after,
                indexConfigPath)) {
            Index index = getIndex(def);
            if (index == null) {
                continue;
            }
            newState = index.processCommit(store, before, newState);
        }
        return newState;
    }
}
