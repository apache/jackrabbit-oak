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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.plugins.memory.PropertyBuilder;

import com.google.common.collect.Iterators;

import static org.apache.jackrabbit.oak.api.Type.NAME;

/**
 * @see MembershipProvider to more details.
 */
public class MembershipWriter {

    public static final int DEFAULT_MEMBERSHIP_THRESHOLD = 100;

    /**
     * size of the membership threshold after which a new overflow node is created.
     */
    private int membershipSizeThreshold = DEFAULT_MEMBERSHIP_THRESHOLD;

    public void setMembershipSizeThreshold(int membershipSizeThreshold) {
        this.membershipSizeThreshold = membershipSizeThreshold;
    }

    /**
     * Adds a new member to the given {@code groupTree}.
     *
     * @param groupTree the group to add the member to
     * @param memberContentId the id of the new member
     * @return {@code true} if the member was added
     * @throws RepositoryException if an error occurs
     */
    boolean addMember(Tree groupTree, String memberContentId) throws RepositoryException {
        Map<String, String> m = Maps.newHashMapWithExpectedSize(1);
        m.put(memberContentId, "-");
        return addMembers(groupTree, m).isEmpty();
    }

    /**
     * Adds a new member to the given {@code groupTree}.
     *
     * @param groupTree the group to add the member to
     * @param memberIds the ids of the new members as map of 'contentId':'memberId'
     * @return the set of member IDs that was not successfully processed.
     * @throws RepositoryException if an error occurs
     */
    Set<String> addMembers(@Nonnull Tree groupTree, @Nonnull Map<String, String> memberIds) throws RepositoryException {
        // check all possible rep:members properties for the new member and also find the one with the least values
        Tree membersList = groupTree.getChild(UserConstants.REP_MEMBERS_LIST);
        Iterator<Tree> trees = Iterators.concat(
                Iterators.singletonIterator(groupTree),
                membersList.getChildren().iterator()
        );

        Set<String> failed = new HashSet<String>(memberIds.size());
        int bestCount = membershipSizeThreshold;
        PropertyState bestProperty = null;
        Tree bestTree = null;

        // remove existing memberIds from the map and find best-matching tree
        // for the insertion of the new members.
        while (trees.hasNext() && !memberIds.isEmpty()) {
            Tree t = trees.next();
            PropertyState refs = t.getProperty(UserConstants.REP_MEMBERS);
            if (refs != null) {
                int numRefs = 0;
                for (String ref : refs.getValue(Type.WEAKREFERENCES)) {
                    String id = memberIds.remove(ref);
                    if (id != null) {
                        failed.add(id);
                        if (memberIds.isEmpty()) {
                            break;
                        }
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

        // update member content structure by starting inserting new member IDs
        // with the best-matching property and create new member-ref-nodes as needed.
        if (!memberIds.isEmpty()) {
            PropertyBuilder<String> propertyBuilder;
            int propCnt;
            if (bestProperty == null) {
                // we don't have a good candidate to store the new members.
                // so there are no members at all or all are full
                if (!groupTree.hasProperty(UserConstants.REP_MEMBERS)) {
                    bestTree = groupTree;
                } else {
                    bestTree = createMemberRefTree(groupTree, membersList);
                }
                propertyBuilder = PropertyBuilder.array(Type.WEAKREFERENCE, UserConstants.REP_MEMBERS);
                propCnt = 0;
            } else {
                propertyBuilder = PropertyBuilder.copy(Type.WEAKREFERENCE, bestProperty);
                propCnt = bestCount;
            }
            // if adding all new members to best-property would exceed the threshold
            // the new ids need to be distributed to different member-ref-nodes
            // for simplicity this is achieved by introducing new tree(s)
            if ((propCnt + memberIds.size()) > membershipSizeThreshold) {
                while (!memberIds.isEmpty()) {
                    Set<String> s = new HashSet<String>();
                    Iterator<String> it = memberIds.keySet().iterator();
                    while (propCnt < membershipSizeThreshold && it.hasNext()) {
                        s.add(it.next());
                        it.remove();
                        propCnt++;
                    }
                    propertyBuilder.addValues(s);
                    bestTree.setProperty(propertyBuilder.getPropertyState());

                    if (it.hasNext()) {
                        // continue filling the next (new) node + propertyBuilder pair
                        propCnt = 0;
                        bestTree = createMemberRefTree(groupTree, membersList);
                        propertyBuilder = PropertyBuilder.array(Type.WEAKREFERENCE, UserConstants.REP_MEMBERS);
                    }
                }
            } else {
                propertyBuilder.addValues(memberIds.keySet());
                bestTree.setProperty(propertyBuilder.getPropertyState());
            }
        }
        return failed;
    }

    private static Tree createMemberRefTree(@Nonnull Tree groupTree, @Nonnull Tree membersList) {
        if (!membersList.exists()) {
            membersList = groupTree.addChild(UserConstants.REP_MEMBERS_LIST);
            membersList.setProperty(JcrConstants.JCR_PRIMARYTYPE, UserConstants.NT_REP_MEMBER_REFERENCES_LIST, NAME);
        }
        Tree refTree = membersList.addChild(nextRefNodeName(membersList));
        refTree.setProperty(JcrConstants.JCR_PRIMARYTYPE, UserConstants.NT_REP_MEMBER_REFERENCES, NAME);
        return refTree;
    }

    private static String nextRefNodeName(@Nonnull Tree membersList) {
        // keep node names linear
        int i = 0;
        String name = String.valueOf(i);
        while (membersList.hasChild(name)) {
            name = String.valueOf(++i);
        }
        return name;
    }

    /**
     * Removes the member from the given group.
     *
     * @param groupTree group to remove the member from
     * @param memberContentId member to remove
     * @return {@code true} if the member was removed.
     */
    boolean removeMember(@Nonnull Tree groupTree, @Nonnull String memberContentId) {
        Map<String, String> m = Maps.newHashMapWithExpectedSize(1);
        m.put(memberContentId, "-");
        return removeMembers(groupTree, m).isEmpty();
    }

    /**
     * Removes the members from the given group.
     *
     * @param groupTree group to remove the member from
     * @param memberIds Map of 'contentId':'memberId' of all members that need to be removed.
     * @return the set of member IDs that was not successfully processed.
     */
    Set<String> removeMembers(@Nonnull Tree groupTree, @Nonnull Map<String, String> memberIds) {
        Tree membersList = groupTree.getChild(UserConstants.REP_MEMBERS_LIST);
        Iterator<Tree> trees = Iterators.concat(
                Iterators.singletonIterator(groupTree),
                membersList.getChildren().iterator()
        );
        while (trees.hasNext() && !memberIds.isEmpty()) {
            Tree t = trees.next();
            PropertyState refs = t.getProperty(UserConstants.REP_MEMBERS);
            if (refs != null) {
                PropertyBuilder<String> prop = PropertyBuilder.copy(Type.WEAKREFERENCE, refs);
                Iterator<Map.Entry<String,String>> it = memberIds.entrySet().iterator();
                while (it.hasNext() && !prop.isEmpty()) {
                    String memberContentId = it.next().getKey();
                    if (prop.hasValue(memberContentId)) {
                        prop.removeValue(memberContentId);
                        it.remove();
                    }
                }
                if (prop.isEmpty()) {
                    if (t == groupTree) {
                        t.removeProperty(UserConstants.REP_MEMBERS);
                    } else {
                        t.remove();
                    }
                } else {
                    t.setProperty(prop.getPropertyState());
                }
            }
        }
        return Sets.newHashSet(memberIds.values());
    }
}