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

package org.apache.jackrabbit.oak.segment;

import javax.annotation.Nonnull;
import javax.jcr.Value;

import org.apache.jackrabbit.commons.SimpleValueFactory;
import org.apache.jackrabbit.oak.api.Descriptors;
import org.apache.jackrabbit.oak.spi.cluster.ClusterRepositoryInfo;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * This provides the 'clusterView' repository descriptors
 **/
class SegmentDiscoveryLiteDescriptors implements Descriptors {

    /**
     * Name of the repository descriptor via which the clusterView is published - which is the reason d'etre of the
     * DocumentDiscoveryLiteService TODO: move this constant to a generic place for both segment and document
     **/
    private static final String OAK_DISCOVERYLITE_CLUSTERVIEW = "oak.discoverylite.clusterview";

    private final SimpleValueFactory factory = new SimpleValueFactory();

    private final NodeStore store;

    SegmentDiscoveryLiteDescriptors(NodeStore store) {
        this.store = store;
    }

    @Nonnull
    @Override
    public String[] getKeys() {
        return new String[] {OAK_DISCOVERYLITE_CLUSTERVIEW};
    }

    @Override
    public boolean isStandardDescriptor(@Nonnull String key) {
        return OAK_DISCOVERYLITE_CLUSTERVIEW.equals(key);
    }

    @Override
    public boolean isSingleValueDescriptor(@Nonnull String key) {
        return OAK_DISCOVERYLITE_CLUSTERVIEW.equals(key);
    }

    @Override
    public Value getValue(@Nonnull String key) {
        if (!OAK_DISCOVERYLITE_CLUSTERVIEW.equals(key)) {
            return null;
        }
        return factory.createValue(getClusterViewAsDescriptorValue());
    }

    @Override
    public Value[] getValues(@Nonnull String key) {
        if (!OAK_DISCOVERYLITE_CLUSTERVIEW.equals(key)) {
            return null;
        }
        return new Value[] {getValue(key)};
    }

    private String getClusterViewAsDescriptorValue() {
        // since currently segment node store is not running in a cluster
        // we can hard-code a single-vm descriptor here:
        // {"seq":4,"final":true,"me":1,"active":[1],"deactivating":[],"inactive":[2]}
        // OAK-3672 : 'id' is now allowed to be null (supported by upper layers),
        //            and for tarMk we're doing exactly that (id==null) - indicating
        //            to upper layers that we're not really in a cluster and that
        //            this low level descriptor doesn't manage the 'cluster id' 
        //            in such a case.
        // OAK-4006: but ClusterRepositoryInfo now provides a persistent clusterId,
        //           so that is now used also for discovery-lite via exactly below 'id'
        String clusterId = ClusterRepositoryInfo.getOrCreateId(store);
        return "{\"seq\":1,\"final\":true,\"me\":1,\"id\":\"" + clusterId + "\",\"active\":[1],\"deactivating\":[],\"inactive\":[]}";
    }

}
