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

package org.apache.jackrabbit.oak.index.indexer.document;

import java.util.Objects;

import org.apache.jackrabbit.oak.spi.state.NodeState;

public class NodeStateEntry {
    private final NodeState nodeState;
    private final String path;

    public NodeStateEntry(NodeState nodeState, String path) {
        this.nodeState = nodeState;
        this.path = path;
    }

    public NodeState getNodeState() {
        return nodeState;
    }

    public String getPath() {
        return path;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeStateEntry that = (NodeStateEntry) o;
        return Objects.equals(nodeState, that.nodeState) &&
                Objects.equals(path, that.path);
    }

    @Override
    public int hashCode() {
        //AbstractNodeState#hashCode
        return 0;
    }

    @Override
    public String toString() {
        return path;
    }
}
