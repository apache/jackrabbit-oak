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
package org.apache.jackrabbit.oak.plugins.lucene;

import static org.apache.jackrabbit.oak.plugins.lucene.LuceneIndexUtils.getIndexInfos;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore;
import org.apache.jackrabbit.oak.spi.QueryIndex;
import org.apache.jackrabbit.oak.spi.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.query.IndexDefinition;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A provider for Lucene indexes. There is exactly one Lucene index instance per
 * MicroKernel.
 */
public class LuceneIndexProvider implements QueryIndexProvider {

    private static final Logger LOG = LoggerFactory
            .getLogger(LuceneIndexProvider.class);

    private final String indexPath;

    private boolean init;

    /**
     * The indexes list
     * 
     * lazy init
     */
    private List<QueryIndex> indexes = null;

    public LuceneIndexProvider(String indexPath) {
        this.indexPath = indexPath;
    }

    private void init(MicroKernel mk) {
        if (init) {
            return;
        }
        LOG.debug("initializing indexes");

        if (!PathUtils.isValid(indexPath)) {
            LOG.warn("index path is not valid {}", indexPath);
            indexes = Collections.<QueryIndex> emptyList();
            init = true;
            return;
        }

        NodeStore store = new KernelNodeStore(mk);
        NodeState index = store.getRoot();
        for (String e : PathUtils.elements(indexPath)) {
            if (PathUtils.denotesRoot(e)) {
                continue;
            }
            index = index.getChildNode(e);
            if (index == null) {
                break;
            }
        }

        if (index == null) {
            // TODO what should happen when the index node doesn't exist?
            indexes = Collections.<QueryIndex> emptyList();
            init = true;
            return;
        }

        List<QueryIndex> tempIndexes = new ArrayList<QueryIndex>();
        for (IndexDefinition childIndex : getIndexInfos(index, indexPath)) {
            LOG.debug("adding a new lucene index instance @ {}", childIndex);
            tempIndexes.add(new LuceneIndex(store, childIndex));
        }
        indexes = new ArrayList<QueryIndex>(tempIndexes);
        init = true;
    }

    @Override
    public List<QueryIndex> getQueryIndexes(MicroKernel mk) {
        init(mk);
        return indexes;
    }
}
