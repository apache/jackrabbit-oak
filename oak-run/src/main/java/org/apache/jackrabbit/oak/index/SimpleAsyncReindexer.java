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

package org.apache.jackrabbit.oak.index;

import java.util.List;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.index.NodeStoreUtils.childBuilder;
import static org.apache.jackrabbit.oak.index.NodeStoreUtils.mergeWithConcurrentCheck;

public class SimpleAsyncReindexer {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final IndexHelper indexHelper;
    private final List<String> indexPaths;
    private final IndexEditorProvider indexEditorProvider;

    public SimpleAsyncReindexer(IndexHelper indexHelper, List<String> indexPaths, IndexEditorProvider indexEditorProvider) {
        this.indexHelper = indexHelper;
        this.indexPaths = indexPaths;
        this.indexEditorProvider = indexEditorProvider;
    }

    public void reindex() throws CommitFailedException {
        setReindexFlag();
        Multimap<String, String> laneMapping = getIndexesPerLane();
        for (String laneName : laneMapping.keySet()) {
            reindex(laneName);
        }
    }

    private void setReindexFlag() throws CommitFailedException {
        NodeBuilder builder = getNodeStore().getRoot().builder();
        for (String indexPath : indexPaths) {
            NodeBuilder idx = childBuilder(builder, indexPath);
            idx.setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true);
        }
        mergeWithConcurrentCheck(getNodeStore(), builder);
    }

    private void reindex(String laneName) {
        AsyncIndexUpdate async = new AsyncIndexUpdate(
                laneName,
                getNodeStore(),
                indexEditorProvider,
                indexHelper.getStatisticsProvider(),
                false);

        //TODO Expose the JMX
        boolean done = false;
        while (!done) {
            //TODO Check for timeout
            async.run();
            done = async.isFinished();
        }
    }

    private Multimap<String, String> getIndexesPerLane(){
        NodeState root = getNodeStore().getRoot();
        Multimap<String, String> map = HashMultimap.create();
        for (String indexPath : indexPaths) {
            NodeState idxState = NodeStateUtils.getNode(root, indexPath);
            String asyncName = IndexUtils.getAsyncLaneName(idxState, indexPath);
            if (asyncName != null) {
                map.put(asyncName, indexPath);
            } else {
                log.warn("No async value for indexPath {}. Ignoring it", indexPath);
            }
        }
        return map;
    }

    private NodeStore getNodeStore() {
        return indexHelper.getNodeStore();
    }
}
