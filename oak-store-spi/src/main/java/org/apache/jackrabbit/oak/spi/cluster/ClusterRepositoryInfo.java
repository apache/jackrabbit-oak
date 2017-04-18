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
package org.apache.jackrabbit.oak.spi.cluster;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.UUID;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to manage a unique cluster/repository id for the cluster.
 */
public class ClusterRepositoryInfo {
    private static final Logger log = LoggerFactory.getLogger(ClusterRepositoryInfo.class);
    public static final String OAK_CLUSTERID_REPOSITORY_DESCRIPTOR_KEY = "oak.clusterid";
    public static final String CLUSTER_CONFIG_NODE = ":clusterConfig";
    public static final String CLUSTER_ID_PROP = ":clusterId";

    private ClusterRepositoryInfo() {
    }

    /**
     * Gets the {# CLUSTER_ID_PROP} if available, if it doesn't it 
     * creates it and returns the newly created one (or if that
     * happened concurrently and another cluster instance won,
     * return that one)
     * <p>
     * Note that this method doesn't require synchronization as
     * concurrent execution within the VM would be covered
     * within NodeStore's merge and result in a conflict for
     * one of the two threads - in which case the looser would
     * re-read and find the clusterId set.
     * 
     * @param store the NodeStore instance
     * @return the persistent clusterId
     */
    @CheckForNull
    public static String getOrCreateId(@Nonnull NodeStore store) {
        checkNotNull(store, "store is null");

        // first try to read an existing clusterId
        NodeState root = store.getRoot();
        NodeState node = root.getChildNode(CLUSTER_CONFIG_NODE);
        if (node.exists() && node.hasProperty(CLUSTER_ID_PROP)) {
            // clusterId is set - this is the normal case
            return node.getProperty(CLUSTER_ID_PROP).getValue(Type.STRING);
        }
        
        // otherwise either the config node or the property doesn't exist.
        // then try to create it - but since this could be executed concurrently
        // in a cluster, it might result in a conflict. in that case, re-read
        // the node
        NodeBuilder builder = root.builder();
        
        // choose a new random clusterId
        String newRandomClusterId = UUID.randomUUID().toString();
        builder.child(CLUSTER_CONFIG_NODE).setProperty(CLUSTER_ID_PROP, newRandomClusterId);
        try {
            store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            // great, we were able to create it, all good.
            log.info("getOrCreateId: created a new clusterId=" + newRandomClusterId);
            return newRandomClusterId;
        } catch (CommitFailedException e) {
            // if we can't commit, then another instance in the cluster
            // set the clusterId concurrently. in which case we should now
            // see that one
            root = store.getRoot();
            node = root.getChildNode(CLUSTER_CONFIG_NODE);
            if (node.exists() && node.hasProperty(CLUSTER_ID_PROP)) {
                // clusterId is set - this is the normal case
                return node.getProperty(CLUSTER_ID_PROP).getValue(Type.STRING);
            }
            
            // this should not happen
            String path = "/" + CLUSTER_CONFIG_NODE + "/" + CLUSTER_ID_PROP;
            log.error("getOrCreateId: both setting and then reading of " + path + "failed");
            throw new IllegalStateException("Both setting and then reading of " + path + " failed");
        }
    }

}

