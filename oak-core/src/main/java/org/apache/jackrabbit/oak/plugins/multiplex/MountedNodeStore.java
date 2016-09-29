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
package org.apache.jackrabbit.oak.plugins.multiplex;

import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

class MountedNodeStore {

    private final Mount mount;

    private final NodeStore nodeStore;

    public MountedNodeStore(Mount mount, NodeStore nodeStore) {
        this.mount = mount;
        this.nodeStore = nodeStore;
    }

    public Mount getMount() {
        return mount;
    }

    public NodeStore getNodeStore() {
        return nodeStore;
    }

    boolean hasChildren(Iterable<String> children) {
        // since we can't possibly know if a node matching the
        // 'oak:mount-*' pattern exists below a given path
        // we are forced to iterate for each node store
        for (String childNodeName : children) {
            if (childNodeName.startsWith(getMount().getPathFragmentName())) {
                return true;
            }
        }
        return false;
    }
}