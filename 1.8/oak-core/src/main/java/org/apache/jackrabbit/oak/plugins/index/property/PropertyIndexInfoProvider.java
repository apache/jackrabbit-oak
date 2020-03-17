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

package org.apache.jackrabbit.oak.plugins.index.property;

import java.io.IOException;

import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexInfo;
import org.apache.jackrabbit.oak.plugins.index.IndexInfoProvider;
import org.apache.jackrabbit.oak.plugins.index.counter.ApproximateCounter;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;

import static com.google.common.base.Preconditions.checkArgument;

@Component
public class PropertyIndexInfoProvider implements IndexInfoProvider {

    @Reference
    private NodeStore nodeStore;

    @SuppressWarnings("unused")
    public PropertyIndexInfoProvider() {
        //For DS
    }

    public PropertyIndexInfoProvider(NodeStore nodeStore) {
        this.nodeStore = nodeStore;
    }

    @Override
    public String getType() {
        return PropertyIndexEditorProvider.TYPE;
    }

    @Override
    public IndexInfo getInfo(String indexPath) throws IOException {
        NodeState idxState = NodeStateUtils.getNode(nodeStore.getRoot(), indexPath);

        checkArgument(PropertyIndexEditorProvider.TYPE.equals(idxState.getString(IndexConstants.TYPE_PROPERTY_NAME)),
                "Index definition at [%s] is not of type 'property'", indexPath);
        PropertyIndexInfo info = new PropertyIndexInfo(indexPath);
        computeCountEstimate(info, idxState);
        return info;
    }

    private void computeCountEstimate(PropertyIndexInfo info, NodeState idxState) {
        long count = -1;

        for (ChildNodeEntry cne : idxState.getChildNodeEntries()) {
            //In multiplexing setups there can be multiple index nodes
            if (NodeStateUtils.isHidden(cne.getName())) {
                NodeState indexData = cne.getNodeState();
                long estimate = ApproximateCounter.getCountSync(indexData);
                if (estimate > 0) {
                    if (count < 0) {
                        count = 0;
                    }
                    count += estimate;

                } else if (count < 0 && indexData.getChildNodeCount(1) == 0) {
                    //If we cannot get estimate then at least try to see if any index data is there or not
                    count = 0;
                }
            }
        }
        info.estimatedCount = count;
    }

    @Override
    public boolean isValid(String indexPath) throws IOException {
        return true;
    }

    private static class PropertyIndexInfo implements IndexInfo {
        private final String indexPath;
        long estimatedCount = -1;

        public PropertyIndexInfo(String indexPath) {
            this.indexPath = indexPath;
        }

        @Override
        public String getIndexPath() {
            return indexPath;
        }

        @Override
        public String getType() {
            return PropertyIndexEditorProvider.TYPE;
        }

        @Override
        public String getAsyncLaneName() {
            return null;
        }

        @Override
        public long getLastUpdatedTime() {
            return -2;
        }

        @Override
        public long getIndexedUpToTime() {
            return 0;
        }

        @Override
        public long getEstimatedEntryCount() {
            return estimatedCount;
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
