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
package org.apache.jackrabbit.oak.security.user;

import java.util.Iterator;
import java.util.Set;

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.plugins.memory.PropertyBuilder;

import com.google.common.collect.Iterators;

import static org.apache.jackrabbit.oak.api.Type.NAME;

/**
 * @see MembershipProvider to more details.
 */
public class MembershipWriter {

    /**
     * size of the membership threshold after which a new overflow node is created.
     */
    private int membershipSizeThreshold = 100;

    public int getMembershipSizeThreshold() {
        return membershipSizeThreshold;
    }

    public void setMembershipSizeThreshold(int membershipSizeThreshold) {
        this.membershipSizeThreshold = membershipSizeThreshold;
    }

    /**
     * Adds a new member to the given {@code groupTree}.
     * @param groupTree the group to add the member to
     * @param memberContentId the id of the new member
     * @return {@code true} if the member was added
     * @throws RepositoryException if an error occurs
     */
    boolean addMember(Tree groupTree, String memberContentId) throws RepositoryException {
        // check all possible rep:members properties for the new member and also find the one with the least values
        Tree membersList = groupTree.getChild(UserConstants.REP_MEMBERS_LIST);
        Iterator<Tree> trees = Iterators.concat(
                Iterators.singletonIterator(groupTree),
                membersList.getChildren().iterator()
        );
        int bestCount = membershipSizeThreshold;
        PropertyState bestProperty = null;
        Tree bestTree = null;
        while (trees.hasNext()) {
            Tree t = trees.next();
            PropertyState refs = t.getProperty(UserConstants.REP_MEMBERS);
            if (refs != null) {
                int numRefs = 0;
                for (String ref: refs.getValue(Type.WEAKREFERENCES)) {
                    if (ref.equals(memberContentId)) {
                        return false;
                    }
                    numRefs++;
                }
                if (numRefs < bestCount) {
                    bestCount = numRefs;
                    bestProperty = refs;
                    bestTree = t;
                }
            }
        }

        PropertyBuilder<String> propertyBuilder;
        if (bestProperty == null) {
            // we don't have a good candidate to store the new member.
            // so there are no members at all or all are full
            if (!groupTree.hasProperty(UserConstants.REP_MEMBERS)) {
                bestTree = groupTree;
            } else {
                if (!membersList.exists()) {
                    membersList = groupTree.addChild(UserConstants.REP_MEMBERS_LIST);
                    membersList.setProperty(JcrConstants.JCR_PRIMARYTYPE, UserConstants.NT_REP_MEMBER_REFERENCES_LIST, NAME);
                    bestTree = membersList.addChild("0");
                } else {
                    // keep node names linear
                    int i=0;
                    String name = String.valueOf(i);
                    while (membersList.hasChild(name)) {
                        name = String.valueOf(++i);
                    }
                    bestTree = membersList.addChild(name);
                }
                bestTree.setProperty(JcrConstants.JCR_PRIMARYTYPE, UserConstants.NT_REP_MEMBER_REFERENCES, NAME);
            }
            propertyBuilder = PropertyBuilder.array(Type.WEAKREFERENCE, UserConstants.REP_MEMBERS);
        } else {
            propertyBuilder = PropertyBuilder.copy(Type.WEAKREFERENCE, bestProperty);
        }
        propertyBuilder.addValue(memberContentId);
        bestTree.setProperty(propertyBuilder.getPropertyState());
        return true;
    }

    /**
     * Removes the member from the given group.
     *
     * @param groupTree group to remove the member from
     * @param memberContentId member to remove
     * @return {@code true} if the member was removed.
     */
    boolean removeMember(Tree groupTree, String memberContentId) {
        Tree membersList = groupTree.getChild(UserConstants.REP_MEMBERS_LIST);
        Iterator<Tree> trees = Iterators.concat(
                Iterators.singletonIterator(groupTree),
                membersList.getChildren().iterator()
        );
        while (trees.hasNext()) {
            Tree t = trees.next();
            PropertyState refs = t.getProperty(UserConstants.REP_MEMBERS);
            if (refs != null) {
                PropertyBuilder<String> prop = PropertyBuilder.copy(Type.WEAKREFERENCE, refs);
                if (prop.hasValue(memberContentId)) {
                    prop.removeValue(memberContentId);
                    if (prop.isEmpty()) {
                        if (t == groupTree) {
                            t.removeProperty(UserConstants.REP_MEMBERS);
                        } else {
                            t.remove();
                        }
                    } else {
                        t.setProperty(prop.getPropertyState());
                    }
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Sets the given set of members to the specified group. this method is only used by the migration code.
     *
     * @param group node builder of group
     * @param members set of content ids to set
     */
    public void setMembers(NodeBuilder group, Set<String> members) {
        group.removeProperty(UserConstants.REP_MEMBERS);
        if (group.hasChildNode(UserConstants.REP_MEMBERS)) {
            group.getChildNode(UserConstants.REP_MEMBERS).remove();
        }

        PropertyBuilder<String> prop = null;
        NodeBuilder refList = null;
        NodeBuilder node = group;

        int count = 0;
        int numNodes = 0;
        for (String ref: members) {
            if (prop == null) {
                prop = PropertyBuilder.array(Type.WEAKREFERENCE, UserConstants.REP_MEMBERS);
            }
            prop.addValue(ref);
            count++;
            if (count > membershipSizeThreshold) {
                node.setProperty(prop.getPropertyState());
                prop = null;
                if (refList == null) {
                    // create intermediate structure
                    refList = group.child(UserConstants.REP_MEMBERS_LIST);
                    refList.setProperty(JcrConstants.JCR_PRIMARYTYPE, UserConstants.NT_REP_MEMBER_REFERENCES_LIST, NAME);
                }
                node = refList.child(String.valueOf(numNodes++));
                node.setProperty(JcrConstants.JCR_PRIMARYTYPE, UserConstants.NT_REP_MEMBER_REFERENCES, NAME);
            }
        }
        if (prop != null) {
            node.setProperty(prop.getPropertyState());
        }
    }


}