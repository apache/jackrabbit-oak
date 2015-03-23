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
package org.apache.jackrabbit.oak.upgrade;

import javax.jcr.RepositoryException;

import com.google.common.base.Charsets;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeBuilder;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RepositorySidegrade {

    /**
     * Logger instance
     */
    private static final Logger logger =
        LoggerFactory.getLogger(RepositorySidegrade.class);

    /**
     * Target node store.
     */
    private final NodeStore target;

    private final NodeStore source;

    private boolean copyBinariesByReference = false;

    /**
     * Creates a tool for copying the full contents of the source repository
     * to the given target repository. Any existing content in the target
     * repository will be overwritten.
     *
     * @param source source node store
     * @param target target node store
     */
    public RepositorySidegrade(NodeStore source, NodeStore target) {
        this.source = source;
        this.target = target;
    }

    public boolean isCopyBinariesByReference() {
        return copyBinariesByReference;
    }

    public void setCopyBinariesByReference(boolean copyBinariesByReference) {
        this.copyBinariesByReference = copyBinariesByReference;
    }

    /**
     * Copies the full content from the source to the target repository.
     * <p>
     * The source repository <strong>must not be modified</strong> while
     * the copy operation is running to avoid an inconsistent copy.
     * <p>
     * Note that both the source and the target repository must be closed
     * during the copy operation as this method requires exclusive access
     * to the repositories.
     *
     * @throws RepositoryException if the copy operation fails
     */
    public void copy() throws RepositoryException {
        try {
            NodeState root = source.getRoot();
            NodeBuilder builder = target.getRoot().builder();

            new InitialContent().initialize(builder);

            copyState(builder, root);

            target.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        } catch (Exception e) {
            throw new RepositoryException("Failed to copy content", e);
        }
    }

    private void copyState(NodeBuilder parent, NodeState state) throws CommitFailedException {
        boolean isSegmentNodeBuilder = parent instanceof SegmentNodeBuilder;
        for (PropertyState property : state.getProperties()) {
            parent.setProperty(property);
        }
        for (ChildNodeEntry entry : state.getChildNodeEntries()) {
            if (isSegmentNodeBuilder) {
                parent.setChildNode(entry.getName(), entry.getNodeState());
            } else {
                setChildNode(parent, entry.getName(), entry.getNodeState());
            }
        }
        target.merge(parent, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    /**
     * NodeState are copied by value by recursing down the complete tree
     * This is a temporary approach for OAK-1760 for 1.0 branch.
     */
    private void setChildNode(NodeBuilder parent, String name, NodeState state) {
        // OAK-1589: maximum supported length of name for DocumentNodeStore
        // is 150 bytes. Skip the sub tree if the the name is too long
        if (name.length() > 37 && name.getBytes(Charsets.UTF_8).length > 150) {
            logger.warn("Node name too long. Skipping {}", state);
            return;
        }
        NodeBuilder builder = parent.setChildNode(name);
        for (PropertyState property : state.getProperties()) {
            builder.setProperty(property);
        }
        for (ChildNodeEntry child : state.getChildNodeEntries()) {
            setChildNode(builder, child.getName(), child.getNodeState());
        }
    }
}
