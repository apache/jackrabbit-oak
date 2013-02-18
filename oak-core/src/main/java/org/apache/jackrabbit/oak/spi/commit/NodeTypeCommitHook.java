/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.spi.commit;

import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;

import java.security.AccessController;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import javax.jcr.Credentials;
import javax.jcr.RepositoryException;
import javax.jcr.SimpleCredentials;
import javax.jcr.nodetype.NodeTypeIterator;
import javax.security.auth.Subject;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.spi.security.authentication.ImpersonationCredentials;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This abstract class provides node-type extensible commit hooks. One can extend this class to create
 * {@link CommitHook}s for specific node types or mixin types. For more details check this class' constructor.
 */
public abstract class NodeTypeCommitHook implements CommitHook {

    protected String nodeType;
    protected Set<String> nodeTypes;
    private static final Logger LOG = LoggerFactory.getLogger(NodeTypeCommitHook.class);

    /**
     * Creates a {@link NodeTypeCommitHook} for a specified node type or mixin type.
     * 
     * @param nodeType
     *            the type of the node / mixin
     */
    protected NodeTypeCommitHook(String nodeType) {
        this.nodeType = nodeType;
        nodeTypes = new HashSet<String>();
    }

    /**
     * Will be called on all sub-classes of the {@link NodeTypeCommitHook} class when a change is detected on a node
     * with the specified primary type or mixin type.
     * 
     * @param before
     *            the state of node before the change
     * @param after
     *            the state of the node after the change
     * @param nodeBuilder
     *            a node builder to be used in case one wants to create new states
     * @param userID
     *            the user who produced the change
     * @param lastModifiedDate
     *            the date when the change has been made
     */
    protected abstract void processChangedNode(NodeState before, NodeState after, NodeBuilder nodeBuilder,
            String userID, Date lastModifiedDate);

    @Override
    public NodeState processCommit(NodeState before, NodeState after) throws CommitFailedException {
        NodeBuilder node = after.builder();
        try {
            if (nodeTypes.size() == 0) {
                NodeTypeIterator nti = ReadOnlyNodeTypeManager.getInstance(after).getNodeType(nodeType).getSubtypes();
                while (nti.hasNext()) {
                    nodeTypes.add(nti.nextNodeType().getName());
                }
                nodeTypes.add(nodeType);
            }
            after.compareAgainstBaseState(before, new ModifiedNodeStateDiff(node, getUserID(), new Date()));
        } catch (RepositoryException e) {
            LOG.error("Unable to determine node types", e);
        }
        return node.getNodeState();
    }

    private class ModifiedNodeStateDiff extends DefaultNodeStateDiff {

        private final NodeBuilder nodeBuilder;
        private final String userID;
        private final Date lastModifiedDate;

        public ModifiedNodeStateDiff(NodeBuilder nodeBuilder, String userID, Date lastModifiedDate) {
            this.nodeBuilder = nodeBuilder;
            this.userID = userID;
            this.lastModifiedDate = lastModifiedDate;
        }

        @Override
        public void childNodeAdded(String name, NodeState after) {
            childNodeChanged(name, MemoryNodeState.EMPTY_NODE, after);
        }

        @Override
        public void childNodeChanged(String name, NodeState before, NodeState after) {
            after.compareAgainstBaseState(before, new ModifiedNodeStateDiff(nodeBuilder.child(name), userID,
                    lastModifiedDate));
            boolean matchesNodeType = nodeTypes.contains(after.getProperty(JcrConstants.JCR_PRIMARYTYPE).getValue(
                    STRING));
            boolean matchesMixin = false;
            PropertyState mixTypes = after.getProperty(JcrConstants.JCR_MIXINTYPES);
            if (mixTypes != null) {
                Iterable<String> mixins = mixTypes.getValue(STRINGS);
                for (String m : mixins) {
                    if (nodeTypes.contains(m)) {
                        matchesMixin = true;
                        break;
                    }
                }
            }
            if (matchesNodeType || matchesMixin) {
                processChangedNode(before, after, nodeBuilder.child(name), getUserID(), new Date());
            }
        }

        @Override
        public void childNodeDeleted(String name, NodeState before) {
            NodeState after = MemoryNodeState.EMPTY_NODE;
            after.compareAgainstBaseState(before, new ModifiedNodeStateDiff(after.builder(), userID, lastModifiedDate));
        }

    }

    private String getUserID() {
        Subject subject = Subject.getSubject(AccessController.getContext());
        Set<Credentials> credentials = subject.getPublicCredentials(Credentials.class);
        String userID = null;
        for (Credentials c : credentials) {
            if (c instanceof SimpleCredentials) {
                userID = ((SimpleCredentials) c).getUserID();
            } else if (c instanceof ImpersonationCredentials) {
                userID = ((ImpersonationCredentials) c).getImpersonatorInfo().getUserID();
            }
        }
        return userID;
    }
}
