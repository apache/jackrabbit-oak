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
package org.apache.jackrabbit.oak.security.privilege;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.MemoryPropertyBuilder;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.state.EmptyNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.PropertyBuilder;
import org.apache.jackrabbit.util.Text;

/**
 * JcrAllCommitHook is responsible for updating the jcr:all privilege definition
 * upon successful registration of a new privilege.
 */
public class JcrAllCommitHook implements CommitHook, PrivilegeConstants {

    @Nonnull
    @Override
    public NodeState processCommit(NodeState before, NodeState after) throws CommitFailedException {
        NodeBuilder builder = after.builder();
        after.compareAgainstBaseState(before, new PrivilegeDiff(null, null, builder));

        return builder.getNodeState();
    }

    private class PrivilegeDiff extends EmptyNodeStateDiff {

        private static final String ROOT_PATH = "";

        private final PrivilegeDiff parentDiff;
        private final String path;
        private final NodeBuilder nodeBuilder;

        private PrivilegeDiff(PrivilegeDiff parentDiff, String nodeName, NodeBuilder nodeBuilder) {
            this.parentDiff = parentDiff;
            this.path = (nodeName == null) ? ROOT_PATH : parentDiff.path + '/' + nodeName;
            this.nodeBuilder = nodeBuilder;
        }

        @Override
        public void childNodeAdded(String name, NodeState after) {
            if (PRIVILEGES_PATH.equals(path) && !JCR_ALL.equals(name)) {
                // a new privilege was registered -> update the jcr:all privilege
                NodeBuilder jcrAll = nodeBuilder.child(JCR_ALL);
                PropertyState aggregates = jcrAll.getProperty(REP_AGGREGATES);

                // FIXME: remove usage of MemoryPropertyBuilder (OAK-372)
                PropertyBuilder<String> propertyBuilder;
                if (aggregates == null) {
                    propertyBuilder = MemoryPropertyBuilder.array(Type.NAME, REP_AGGREGATES);
                } else {
                    propertyBuilder = MemoryPropertyBuilder.copy(Type.NAME, aggregates);
                }
                if (!propertyBuilder.hasValue(name)) {
                    propertyBuilder.addValue(name);
                    jcrAll.setProperty(propertyBuilder.getPropertyState());
                }
            }
        }

        @Override
        public void childNodeChanged(String name, NodeState before, NodeState after) {
            if (ROOT_PATH.equals(path) || Text.isDescendant(path, PRIVILEGES_PATH)) {
                after.compareAgainstBaseState(before, new PrivilegeDiff(this, name, nodeBuilder.child(name)));
            }
        }
    }
}
