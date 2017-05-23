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

package org.apache.jackrabbit.oak.plugins.index;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.felix.scr.annotations.ReferencePolicyOption;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.notNull;

@Component
@Service
public class IndexInfoServiceImpl implements IndexInfoService{
    private final Logger log = LoggerFactory.getLogger(getClass());

    @Reference
    private IndexPathService indexPathService;

    @Reference(policy = ReferencePolicy.DYNAMIC,
            cardinality = ReferenceCardinality.OPTIONAL_MULTIPLE,
            policyOption = ReferencePolicyOption.GREEDY,
            referenceInterface = IndexInfoProvider.class
    )
    private final Map<String, IndexInfoProvider> infoProviders = new ConcurrentHashMap<>();

    @Reference
    private NodeStore nodeStore;

    public IndexInfoServiceImpl() {
        //For DS
    }

    public IndexInfoServiceImpl(NodeStore nodeStore, IndexPathService indexPathService) {
        this.indexPathService = indexPathService;
        this.nodeStore = nodeStore;
    }

    @Override
    public Iterable<IndexInfo> getAllIndexInfo() {
        return Iterables.filter(Iterables.transform(indexPathService.getIndexPaths(), new Function<String, IndexInfo>() {
            @Override
            public IndexInfo apply(String indexPath) {
                try {
                    return getInfo(indexPath);
                } catch (Exception e) {
                    log.warn("Error occurred while capturing IndexInfo for path {}", indexPath, e);
                    return null;
                }
            }
        }), notNull());
    }

    @Override
    public IndexInfo getInfo(String indexPath) throws IOException {
        String type = getIndexType(indexPath);
        if (type == null) return null;
        IndexInfoProvider infoProvider = infoProviders.get(type);

        if (infoProvider == null) {
            return new SimpleIndexInfo(indexPath, type);
        }

        return infoProvider.getInfo(indexPath);
    }

    @Override
    public boolean isValid(String indexPath) throws IOException {
        String type = getIndexType(indexPath);
        if (type == null){
            log.warn("No type property defined for index definition at path {}", indexPath);
            return false;
        }
        IndexInfoProvider infoProvider = infoProviders.get(type);
        if (infoProvider == null){
            log.warn("No IndexInfoProvider for for index definition at path {} of type {}", indexPath, type);
            return true; //TODO Reconsider this scenario
        }
        return infoProvider.isValid(indexPath);
    }

    public void bindInfoProviders(IndexInfoProvider infoProvider){
        infoProviders.put(checkNotNull(infoProvider.getType()), infoProvider);
    }

    public void unbindInfoProviders(IndexInfoProvider infoProvider){
        infoProviders.remove(infoProvider.getType());
    }

    private String getIndexType(String indexPath) {
        NodeState idxState = NodeStateUtils.getNode(nodeStore.getRoot(), indexPath);
        String type = idxState.getString(IndexConstants.TYPE_PROPERTY_NAME);
        if (type == null || "disabled".equals(type)){
            return null;
        }
        return type;
    }

    private static class SimpleIndexInfo implements IndexInfo {
        private final String indexPath;
        private final String type;

        private SimpleIndexInfo(String indexPath, String type) {
            this.indexPath = indexPath;
            this.type = type;
        }

        @Override
        public String getIndexPath() {
            return indexPath;
        }

        @Override
        public String getType() {
            return type;
        }

        @Override
        public String getAsyncLaneName() {
            return null;
        }

        @Override
        public long getLastUpdatedTime() {
            return -1;        }

        @Override
        public long getIndexedUpToTime() {
            return -1;
        }

        @Override
        public long getEstimatedEntryCount() {
            return -1;
        }

        @Override
        public long getSizeInBytes() {
            return -1;
        }

        @Override
        public boolean hasIndexDefinitionChangedWithoutReindexing() {
            return false;
        }

        @Override
        public String getIndexDefinitionDiff() {
            return null;
        }
    }
}
