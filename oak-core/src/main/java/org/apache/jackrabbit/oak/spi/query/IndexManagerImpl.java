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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Tree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;

public class IndexManagerImpl implements IndexManager {

    // TODO implement an observation listener so that the {@link
    // IndexManagerImpl} automatically creates new indexes based on new nodes
    // added under {@link #indexConfigPath}

    private static final Logger LOG = LoggerFactory
            .getLogger(IndexManagerImpl.class);

    private final String indexConfigPath;

    private final ContentSession session;

    private final MicroKernel mk;

    private final Map<String, IndexFactory> indexFactories = new ConcurrentHashMap<String, IndexFactory>();

    private final Map<IndexDefinition, Index> indexes = new ConcurrentHashMap<IndexDefinition, Index>();

    public IndexManagerImpl(String indexConfigPath, ContentSession session,
            MicroKernel mk) {
        this.indexConfigPath = indexConfigPath;
        this.session = session;
        this.mk = mk;
    }

    @Override
    public void registerIndexFactory(IndexFactory factory) {
        factory.init(mk);
        for (String type : factory.getTypes()) {
            if (indexFactories.remove(type) != null) {
                // TODO is override allowed?
            }
            indexFactories.put(type, factory);
        }
    }

    @Override
    public void init() {
        //
        // TODO hardwire default property indexes first ?
        // registerIndexFactory(type, factory);
        Tree definitions = session.getCurrentRoot().getTree(indexConfigPath);
        if (definitions == null) {
            return;
        }

        List<IndexDefinition> defs = new ArrayList<IndexDefinition>();
        for (Tree c : definitions.getChildren()) {
            IndexDefinition def = IndexUtils.getDefs(indexConfigPath, c);
            if (def == null) {
                LOG.warn("Skipping illegal index definition name {} @ {}",
                        c.getName(), indexConfigPath);
                continue;
            }
            if (indexes.get(def.getName()) != null) {
                LOG.warn("Skipping existing index definition name {} @ {}",
                        c.getName(), indexConfigPath);
                continue;
            }
            defs.add(def);
        }
        registerIndex(defs.toArray(new IndexDefinition[defs.size()]));
    }

    @Override
    public void registerIndex(IndexDefinition... indexDefinition) {
        for (IndexDefinition def : indexDefinition) {
            if (def == null) {
                continue;
            }
            IndexFactory f = indexFactories.get(def.getType());
            if (f == null) {
                LOG.warn(
                        "Skipping unknown index definition type {}, name {} @ {}",
                        new String[] { def.getType(), indexConfigPath,
                                def.getName() });
                continue;
            }
            Index index = f.createIndex(def);
            if (index != null) {
                indexes.put(def, index);
            }
        }
    }

    @Override
    public Set<IndexDefinition> getIndexes() {
        return ImmutableSet.copyOf(indexes.keySet());
    }

    @Override
    public synchronized void close() throws IOException {
        Iterator<IndexDefinition> iterator = indexes.keySet().iterator();
        while (iterator.hasNext()) {
            IndexDefinition id = iterator.next();
            try {
                indexes.get(id).close();
            } catch (IOException e) {
                LOG.error("error closing index {}", id.getName(), e);
            }
            iterator.remove();
        }
    }
}
