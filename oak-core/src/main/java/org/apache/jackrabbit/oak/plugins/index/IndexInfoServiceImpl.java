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
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.component.annotations.ReferencePolicyOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

@Component
public class IndexInfoServiceImpl implements IndexInfoService{
    private final Logger log = LoggerFactory.getLogger(getClass());

    @Reference
    private IndexPathService indexPathService;

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
        HashSet<String> allIndexes = new HashSet<>();
        indexPathService.getIndexPaths().forEach(allIndexes::add);
        HashSet<String> activeIndexes = new HashSet<>();
        if (indexPathService.getMountInfoProvider().hasNonDefaultMounts()) {
            activeIndexes.addAll(IndexName.filterReplacedIndexes(allIndexes, nodeStore.getRoot(), true));
        } else {
            activeIndexes.addAll(allIndexes);
        }
        return Iterables.filter(Iterables.transform(indexPathService.getIndexPaths(), indexPath -> {
            try {
                IndexInfo info = getInfo(indexPath);
                if (info != null) {
                    info.setActive(activeIndexes.contains(indexPath));
                }
                return info;
            } catch (Exception e) {
                log.warn("Error occurred while capturing IndexInfo for path {}", indexPath, e);
                return null;
            }
        }), x -> x != null);
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

    @Reference(name = "infoProviders",
            policy = ReferencePolicy.DYNAMIC,
            cardinality = ReferenceCardinality.MULTIPLE,
            policyOption = ReferencePolicyOption.GREEDY,
            service = IndexInfoProvider.class
    )
    public void bindInfoProviders(IndexInfoProvider infoProvider){
        infoProviders.put(requireNonNull(infoProvider.getType()), infoProvider);
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
        private boolean isActive;

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

        @Override
        public boolean hasHiddenOakLibsMount() {
            return false;
        }

        @Override
        public boolean hasPropertyIndexNode() {
            return false;
        }

        @Override
        public long getSuggestSizeInBytes() {
            return -1;
        }

        @Override
        public long getCreationTimestamp() {
            return -1;
        }

        @Override
        public long getReindexCompletionTimestamp() {
            return -1;
        }

        @Override
        public void setActive(boolean value) {
            isActive = value;
        }

        @Override
        public boolean isActive() {
            return isActive;
        }
    }
}
