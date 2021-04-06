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
package org.apache.jackrabbit.oak.plugins.version;

import javax.jcr.RepositoryException;
import javax.jcr.nodetype.NoSuchNodeTypeException;
import javax.jcr.nodetype.NodeType;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.JcrConstants.MIX_REFERENCEABLE;
import static org.apache.jackrabbit.JcrConstants.NT_FROZENNODE;
import static org.apache.jackrabbit.oak.api.CommitFailedException.CONSTRAINT;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.tree.factories.RootFactory;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code Utils} provide some utility methods.
 */
public final class Utils {

    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

    private Utils() {
    }

    /**
     * Returns the jcr:uuid value of given {@code node}.
     *
     * @param node a referenceable node.
     * @return the value of the jcr:uuid property.
     * @throws IllegalArgumentException if the node is not referenceable.
     */
    @NotNull
    static String uuidFromNode(@NotNull NodeBuilder node)
            throws IllegalArgumentException {
        return uuidFromNode(node.getNodeState());
    }

    @NotNull
    static String uuidFromNode(@NotNull NodeState node) {
        PropertyState p = checkNotNull(node).getProperty(JCR_UUID);
        if (p == null) {
            throw new IllegalArgumentException("Not referenceable");
        }
        return p.getValue(Type.STRING);
    }

    /**
     * Returns the {@code jcr:primaryType} value of the given
     * {@code node}.
     *
     * @param node a node.
     * @return the {@code jcr:primaryType} value.
     * @throws IllegalStateException if the node does not have a {@code jcr:primaryType}
     *                               property.
     */
    @NotNull
    static String primaryTypeOf(@NotNull NodeBuilder node)
            throws IllegalStateException {
        String primaryType = checkNotNull(node).getName(JCR_PRIMARYTYPE);
        if (primaryType == null) {
            throw new IllegalStateException("Node does not have a jcr:primaryType");
        }
        return primaryType;
    }

    /**
     * Returns {@code true} iff there is a {@code nt:frozenNode} definition and
     * the definition has a {@code mix:referenceable} supertype.
     *
     * @param root the root of a repository from where to read the node type
     *      information.
     * @return {@code true} if frozen nodes are referenceable, {@code false}
     *      otherwise.
     */
    public static boolean isFrozenNodeReferenceable(@NotNull NodeState root) {
        return isFrozenNodeReferenceable(
                ReadOnlyNodeTypeManager.getInstance(
                        RootFactory.createReadOnlyRoot(root),
                        NamePathMapper.DEFAULT));
    }

    /**
     * Returns {@code true} iff there is a {@code nt:frozenNode} definition and
     * the definition has a {@code mix:referenceable} supertype.
     *
     * @param ntMgr a node type manager to access the node types.
     * @return {@code true} if frozen nodes are referenceable, {@code false}
     *      otherwise.
     */
    public static boolean isFrozenNodeReferenceable(@NotNull ReadOnlyNodeTypeManager ntMgr) {
        try {
            NodeType[] superTypes = ntMgr.getNodeType(NT_FROZENNODE).getSupertypes();
            for (NodeType superType : superTypes) {
                if (superType.isNodeType(MIX_REFERENCEABLE)) {
                    // OAK-9134: add uuid in older repositories with mix:referenceable in nt:frozenNode
                    return true;
                }
            }
        } catch (NoSuchNodeTypeException e) {
            LOG.info("Repository does not define nt:frozenNode. Assuming frozen nodes are not referenceable.");
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    static <T> T throwProtected(String path) throws CommitFailedException {
        throw new CommitFailedException(CONSTRAINT, 100,
                "Item is protected: " + path);
    }
}
