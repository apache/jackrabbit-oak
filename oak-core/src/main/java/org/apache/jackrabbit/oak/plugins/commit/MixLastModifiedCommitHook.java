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
package org.apache.jackrabbit.oak.plugins.commit;

import java.security.AccessController;
import java.util.Set;

import javax.jcr.Credentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.Subject;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.LongPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState;
import org.apache.jackrabbit.oak.plugins.memory.StringPropertyState;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.commit.NodeTypeCommitHook;
import org.apache.jackrabbit.oak.spi.security.authentication.ImpersonationCredentials;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

/**
 * This class provides a commit hook for nodes of type {@code mix:lastModified} that properly implements section
 * 3.7.11.8 of JSR283:
 * 
 * <pre>
 * [mix:lastModified] mixin
 * - jcr:lastModified (DATE) autocreated protected? OPV?
 * - jcr:lastModifiedBy (STRING) autocreated protected? OPV?
 * </pre>
 * 
 */
public class MixLastModifiedCommitHook extends NodeTypeCommitHook {

    private Long lastModified;
    private String userID;

    public MixLastModifiedCommitHook() {
        super(NodeTypeConstants.MIX_LASTMODIFIED);
    }

    @Override
    public void processChangedNode(NodeState before, NodeState after, NodeBuilder nodeBuilder) {
        after.compareAgainstBaseState(before, new AllChangesNodeStateDiff(nodeBuilder));
        if (lastModified != null) {
            PropertyState jcrLastModifiedDate = LongPropertyState.createDateProperty(JcrConstants.JCR_LASTMODIFIED,
                    lastModified);
            nodeBuilder.setProperty(jcrLastModifiedDate);
        }
        if (userID != null) {
            PropertyState jcrLastModifiedBy = StringPropertyState.stringProperty(NodeTypeConstants.JCR_LASTMODIFIEDBY,
                    userID);
            nodeBuilder.setProperty(jcrLastModifiedBy);
        }
    }

    private final class AllChangesNodeStateDiff implements NodeStateDiff {

        private NodeBuilder node;

        public AllChangesNodeStateDiff(NodeBuilder node) {
            this.node = node;
        }

        @Override
        public void propertyAdded(PropertyState after) {
            if (after.getName().equals(JcrConstants.JCR_LASTMODIFIED)) {
                return;
            }
            updateLastModifiedDate();
            updateLastModifiedBy();
        }

        @Override
        public void propertyChanged(PropertyState before, PropertyState after) {
            if (after.getName().equals(JcrConstants.JCR_LASTMODIFIED)) {
                return;
            }
            updateLastModifiedDate();
            updateLastModifiedBy();
        }

        @Override
        public void propertyDeleted(PropertyState before) {
            updateLastModifiedDate();
            updateLastModifiedBy();
        }

        @Override
        public void childNodeAdded(String name, NodeState after) {
            childNodeChanged(name, MemoryNodeState.EMPTY_NODE, after);
        }

        @Override
        public void childNodeChanged(String name, NodeState before, NodeState after) {
            after.compareAgainstBaseState(before, new AllChangesNodeStateDiff(node.child(name)));
            updateLastModifiedDate();
            updateLastModifiedBy();
        }

        @Override
        public void childNodeDeleted(String name, NodeState before) {
            NodeState after = MemoryNodeState.EMPTY_NODE;
            after.compareAgainstBaseState(before, new AllChangesNodeStateDiff(after.builder()));
            updateLastModifiedDate();
            updateLastModifiedBy();
        }

        private void updateLastModifiedDate() {
            lastModified = System.currentTimeMillis();
        }
        
        private void updateLastModifiedBy() {
            Subject subject = Subject.getSubject(AccessController.getContext());
            Set<Credentials> credentials = subject.getPublicCredentials(Credentials.class);
            userID = null;
            for (Credentials c : credentials) {
                if (c instanceof SimpleCredentials) {
                    userID = ((SimpleCredentials) c).getUserID();
                } else if (c instanceof ImpersonationCredentials) {
                    userID = ((ImpersonationCredentials) c).getImpersonatorInfo().getUserID();
                }
            }
        }
    }
}
