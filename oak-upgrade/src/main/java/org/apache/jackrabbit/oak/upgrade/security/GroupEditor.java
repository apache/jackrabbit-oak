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
package org.apache.jackrabbit.oak.upgrade.security;

import java.util.Set;
import java.util.TreeSet;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyBuilder;
import org.apache.jackrabbit.oak.security.user.MembershipWriter;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.api.CommitFailedException.CONSTRAINT;
import static org.apache.jackrabbit.oak.api.Type.NAME;

class GroupEditor extends DefaultEditor {

    private static final Logger log = LoggerFactory.getLogger(GroupEditor.class);

    private final String[] groupsRoot;

    private State state;

    private EditorGroup currentGroup;

    private final UpgradeMembershipWriter writer = new UpgradeMembershipWriter();

    GroupEditor(@Nonnull NodeBuilder builder, @Nonnull String groupsPath) {
        this.state = new State(builder);
        this.groupsRoot = Text.explode(groupsPath, '/', false);
        // writer.setMembershipSizeThreshold(10); // uncomment to test different split sizes
    }

    private boolean descend(String name) {
        if (state.depth < groupsRoot.length && !name.equals(groupsRoot[state.depth])) {
            return false;
        }
        state = state.push(name);
        return true;
    }

    private void ascend() {
        state = state.pop();
    }

    @Override
    public void leave(NodeState before, NodeState after) throws CommitFailedException {
        if (currentGroup != null && currentGroup.path.equals(state.path)) {
            log.info("scanned group {}, {} members", currentGroup.path, currentGroup.members.size());
            writer.setMembers(state.builder, currentGroup.members);
            currentGroup = null;
        }
        ascend();
    }

    @SuppressWarnings("deprecation")
    @Override
    public Editor childNodeAdded(String name, NodeState after) throws CommitFailedException {
        if (!descend(name)) {
            return null;
        }

        String nt = after.getName(JCR_PRIMARYTYPE);
        if (nt == null) {
            throw new CommitFailedException(
                    CONSTRAINT, 34, JCR_PRIMARYTYPE + " missing at " + state.path);
        }

        if (UserConstants.NT_REP_GROUP.equals(nt)) {
            if (currentGroup != null) {
                log.error("rep:Group within rep:Group not supported during upgrade. current group: {}, overwriting group: {}",
                        currentGroup.path, state.path);
            }
            currentGroup = new EditorGroup(state.path);
            currentGroup.addMembers(after.getProperty(UserConstants.REP_MEMBERS));
        } else if (UserConstants.NT_REP_MEMBERS.equals(nt)) {
            if (currentGroup == null) {
                log.warn("rep:Members detected outside of a rep:Group. ignoring {}", state.path);
            } else {
                currentGroup.addMembers(after);
            }
        }
        return this;
    }

    @Override
    public Editor childNodeChanged(String name, NodeState before, NodeState after) {
        return null;
    }

    @Override
    public Editor childNodeDeleted(String name, NodeState before) {
        throw new IllegalStateException("deleted node during upgrade copy not expected: " + state.path + "/" + name);
    }

    private static class State {

        private final State parent;

        private final String path;

        private final NodeBuilder builder;

        private final int depth;

        private State(State parent, String name) {
            this.parent = parent;
            this.path = parent.path + "/" + name;
            this.builder = parent.builder.child(name);
            this.depth = parent.depth + 1;
            log.debug("{} --> {}", depth, path);
        }

        private State(NodeBuilder builder) {
            this.parent = null;
            this.path = "";
            this.builder = builder;
            this.depth = 0;
        }

        private State push(String name) {
            return new State(this, name);
        }

        private State pop() {
            log.debug("{} <-- {}", depth, path);
            return parent;
        }
    }

    private static class EditorGroup {

        private final String path;

        private final Set<String> members = new TreeSet<String>();

        private EditorGroup(String path) {
            this.path = path;
        }

        private void addMembers(PropertyState repMembers) {
            if (repMembers != null) {
                for (String ref: repMembers.getValue(Type.WEAKREFERENCES)) {
                    members.add(ref);
                }
            }
        }

        private void addMembers(NodeState node) {
            for (PropertyState prop: node.getProperties()) {
                if (prop.getType() == Type.WEAKREFERENCE) {
                    members.add(prop.getValue(Type.WEAKREFERENCE));
                }
            }
        }
    }

    private static class UpgradeMembershipWriter {

        /**
         * Sets the given set of members to the specified group. this method is only used by the migration code.
         *
         * @param group node builder of group
         * @param members set of content ids to set
         */
        public void setMembers(@Nonnull NodeBuilder group, @Nonnull Set<String> members) {
            group.removeProperty(UserConstants.REP_MEMBERS);
            if (group.hasChildNode(UserConstants.REP_MEMBERS)) {
                group.getChildNode(UserConstants.REP_MEMBERS).remove();
            }

            PropertyBuilder<String> prop = null;
            NodeBuilder refList = null;
            NodeBuilder node = group;

            int count = 0;
            int numNodes = 0;
            for (String ref : members) {
                if (prop == null) {
                    prop = PropertyBuilder.array(Type.WEAKREFERENCE, UserConstants.REP_MEMBERS);
                }
                prop.addValue(ref);
                count++;
                if (count > MembershipWriter.DEFAULT_MEMBERSHIP_THRESHOLD) {
                    node.setProperty(prop.getPropertyState());
                    prop = null;
                    if (refList == null) {
                        // create intermediate structure
                        refList = group.child(UserConstants.REP_MEMBERS_LIST);
                        refList.setProperty(JCR_PRIMARYTYPE, UserConstants.NT_REP_MEMBER_REFERENCES_LIST, NAME);
                    }
                    node = refList.child(String.valueOf(numNodes++));
                    node.setProperty(JCR_PRIMARYTYPE, UserConstants.NT_REP_MEMBER_REFERENCES, NAME);
                }
            }
            if (prop != null) {
                node.setProperty(prop.getPropertyState());
            }
        }
    }
}
