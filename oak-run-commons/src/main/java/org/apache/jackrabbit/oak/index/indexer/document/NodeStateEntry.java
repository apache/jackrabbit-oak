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
    private final long memUsage;
    private final long lastModified;

    private NodeStateEntry(NodeState nodeState, String path, long memUsage, long lastModified) {
        this.nodeState = nodeState;
        this.path = path;
        this.memUsage = memUsage;
        this.lastModified = lastModified;
    }

    public NodeState getNodeState() {
        return nodeState;
    }

    public String getPath() {
        return path;
    }

    public long estimatedMemUsage() {
        return memUsage;
    }

    public long getLastModified() {
        return lastModified;
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

    public static class NodeStateEntryBuilder {

        private final NodeState nodeState;
        private final String path;
        private long memUsage;
        private long lastModified;

        public NodeStateEntryBuilder(NodeState nodeState, String path) {
            this.nodeState = nodeState;
            this.path = path;
        }

        public NodeStateEntryBuilder withMemUsage(long memUsage) {
            this.memUsage = memUsage;
            return this;
        }

        public NodeStateEntryBuilder withLastModified(long lastModified) {
            this.lastModified = lastModified;
            return this;
        }

        public NodeStateEntry build() {
            return new NodeStateEntry(nodeState, path, memUsage, lastModified);
        }
    }
}
