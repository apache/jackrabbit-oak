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
package org.apache.jackrabbit.oak.plugins.identifier;

import java.util.UUID;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * Utility class to manage a unique cluster/repository id for the cluster.
 */
public class ClusterRepositoryInfo {
    public static final String CLUSTER_CONFIG_NODE = ":clusterConfig";
    public static final String CLUSTER_ID_PROP = ":clusterId";

    /**
     * Adds a new uuid for the repository in the property /:clusterConfig/:clusterId with preoperty.
     *
     * @param store the NodeStore instance
     * @return the repository id
     * @throws CommitFailedException
     */
    public static String createId(NodeStore store) throws CommitFailedException {
        NodeBuilder root = store.getRoot().builder();
        if (!root.hasChildNode(CLUSTER_CONFIG_NODE)) {
            String id = UUID.randomUUID().toString();
            root.child(CLUSTER_CONFIG_NODE).setProperty(CLUSTER_ID_PROP, id);
            store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            return id;
        } else {
            return root.getChildNode(CLUSTER_CONFIG_NODE).getProperty(CLUSTER_ID_PROP).getValue(Type.STRING);
        }
    }

    /**
     * Retrieves the {# CLUSTER_ID_PROP}
     *
     * @param store the NodeStore instance
     * @return the repository id
     */
    public static String getId(NodeStore store) {
        return store.getRoot().getChildNode(CLUSTER_CONFIG_NODE).getProperty(CLUSTER_ID_PROP).getValue(Type.STRING);
    }
}

