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
package org.apache.jackrabbit.oak.composite;

import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

public class MountedNodeStore {

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

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder(super.toString());
        result.append('[');
        if (mount.isDefault()) {
            result.append("default");
        } else {
            result.append(mount.getName());
        }
        result.append(']');
        return result.toString();
    }
}