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
package org.apache.jackrabbit.oak.plugins.index.old.mk.simple;

/**
 * A node id.
 */
public class NodeId {

    public static final NodeId[] EMPTY_ARRAY = new NodeId[0];

    private final long x;
    private byte[] hash;

    protected NodeId(long x) {
        this.x = x;
    }

    public static NodeId get(long x) {
        return new NodeId(x);
    }

    public static NodeId getInline(NodeImpl n) {
        return new NodeIdInline(n);
    }

    public NodeImpl getNode(NodeMap map) {
        return map.getNode(x);
    }

    @Override
    public String toString() {
        return Long.toString(x);
    }

    public long getLong() {
        return x;
    }

    public boolean isInline() {
        return false;
    }

    public byte[] getHash() {
        return hash;
    }

    public void setHash(byte[] hash) {
        this.hash = hash;
    }

    /**
     * An node id of an inline node (a node that is stored within the parent).
     */
    private static class NodeIdInline extends NodeId {

        private final NodeImpl node;

        protected NodeIdInline(NodeImpl node) {
            super(0);
            this.node = node;
        }

        @Override
        public NodeImpl getNode(NodeMap map) {
            return node;
        }

        @Override
        public String toString() {
            return node.toString();
        }

        @Override
        public boolean isInline() {
            return true;
        }

    }

}
