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

import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.iterator.RangeIteratorAdapter;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.DynamicMembershipProvider;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import javax.jcr.nodetype.ConstraintViolationException;
import java.security.Principal;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * GroupImpl...
 */
class GroupImpl extends AuthorizableImpl implements Group {

    private static final Logger log = LoggerFactory.getLogger(GroupImpl.class);

    GroupImpl(String id, Tree tree, UserManagerImpl userManager) throws RepositoryException {
        super(id, tree, userManager);
    }

    //---------------------------------------------------< AuthorizableImpl >---
    @Override
    void checkValidTree(@NotNull Tree tree) {
        if (!UserUtil.isType(tree, AuthorizableType.GROUP)) {
            throw new IllegalArgumentException("Invalid group node: node type rep:Group expected.");
        }
    }

    //-------------------------------------------------------< Authorizable >---
    @Override
    public boolean isGroup() {
        return true;
    }

    @NotNull
    @Override
    public Principal getPrincipal() throws RepositoryException {
        return new GroupPrincipal(getPrincipalName(), getTree());
    }

    //--------------------------------------------------------------< Group >---
    @NotNull
    @Override
    public Iterator<Authorizable> getDeclaredMembers() throws RepositoryException {
        return getMembersMonitored(false);
    }

    @NotNull
    @Override
    public Iterator<Authorizable> getMembers() throws RepositoryException {
        return getMembersMonitored(true);
    }

    @Override
    public boolean isDeclaredMember(@NotNull Authorizable authorizable) throws RepositoryException {
        return isMember(authorizable, false);
    }

    @Override
    public boolean isMember(@NotNull Authorizable authorizable) throws RepositoryException {
        return isMember(authorizable, true);
    }

    @Override
    public boolean addMember(@NotNull Authorizable authorizable) throws RepositoryException {
        Stopwatch watch = Stopwatch.createStarted();
        boolean success = internalAddMember(authorizable);
        getMonitor().doneUpdateMembers(watch.elapsed(NANOSECONDS), 1, (success) ? 0 : 1, false);
        return success;
    }

    private boolean internalAddMember(@NotNull Authorizable authorizable) throws RepositoryException {
        if (!isValidAuthorizableImpl(authorizable)) {
            log.warn("Invalid Authorizable: {}", authorizable);
            return false;
        }

        DynamicMembershipProvider dmp = getUserManager().getDynamicMembershipProvider();
        if (dmp.coversAllMembers(this)) {
            log.debug("Attempt to add member to dynamic group {}", getID());
            return false;
        }
        AuthorizableImpl authorizableImpl = ((AuthorizableImpl) authorizable);
        if (authorizableImpl.isEveryone()) {
            log.debug("Attempt to create membership for everyone group.");
            return false;
        }

        String memberID = authorizable.getID();
        if (authorizableImpl.isGroup()) {
            if (getID().equals(memberID)) {
                String msg = "Attempt to add a group as member of itself (" + getID() + ").";
                log.debug(msg);
                return false;
            }
            if (isCyclicMembership((Group) authorizable)) {
                String msg = "Cyclic group membership detected for group " + getID() + " and member " + authorizable.getID();
                throw new ConstraintViolationException(msg);
            }
        }

        boolean success = getMembershipProvider().addMember(getTree(), authorizableImpl.getTree());

        if (success) {
            getUserManager().onGroupUpdate(this, false, authorizable);
        }

        return success;
    }

    @NotNull
    @Override
    public Set<String> addMembers(@NotNull String... memberIds) throws RepositoryException {
        return updateMembersMonitored(false, memberIds);
    }

    @Override
    public boolean removeMember(@NotNull Authorizable authorizable) throws RepositoryException {
        Stopwatch watch = Stopwatch.createStarted();
        boolean success = internalRemoveMember(authorizable);
        getMonitor().doneUpdateMembers(watch.elapsed(NANOSECONDS), 1, (success) ? 0 : 1, true);
        return success;
    }

    private boolean internalRemoveMember(@NotNull Authorizable authorizable) throws RepositoryException {
        if (!isValidAuthorizableImpl(authorizable)) {
            log.warn("Invalid Authorizable: {}", authorizable);
            return false;
        }

        DynamicMembershipProvider dmp = getUserManager().getDynamicMembershipProvider();
        if (dmp.coversAllMembers(this)) {
            log.debug("Attempt to remove member from dynamic group {}", getID());
            return false;
        }
        
        AuthorizableImpl authorizableImpl = ((AuthorizableImpl) authorizable);
        if (authorizableImpl.isEveryone()) {
            log.debug("Attempt to remove membership for everyone group.");
            return false;
        } else {
            Tree memberTree = authorizableImpl.getTree();
            boolean success = getMembershipProvider().removeMember(getTree(), memberTree);
            if (success) {
                getUserManager().onGroupUpdate(this, true, authorizable);
            }
            return success;
        }
    }

    @NotNull
    @Override
    public Set<String> removeMembers(@NotNull String... memberIds) throws RepositoryException {
        return updateMembersMonitored(true, memberIds);
    }

    //--------------------------------------------------------------------------
    @NotNull
    private Iterator<Authorizable> getMembersMonitored(boolean includeInherited) throws RepositoryException {
        Stopwatch watch = Stopwatch.createStarted();
        Iterator<Authorizable> members = getMembers(includeInherited);
        getMonitor().doneGetMembers(watch.elapsed(NANOSECONDS), !includeInherited);
        return members;
    }

    /**
     * Internal implementation of {@link #getDeclaredMembers()} and {@link #getMembers()}.
     *
     * @param includeInherited Flag indicating if only the declared or all members
     * should be returned.
     * @return Iterator of authorizables being member of this group.
     * @throws RepositoryException If an error occurs.
     */
    @NotNull
    private Iterator<Authorizable> getMembers(boolean includeInherited) throws RepositoryException {
        UserManagerImpl userMgr = getUserManager();

        DynamicMembershipProvider dmp = getUserManager().getDynamicMembershipProvider();
        Iterator<Authorizable> dynamicMembers = dmp.getMembers(this, includeInherited);
        if (dmp.coversAllMembers(this)) {
            return dynamicMembers;
        }

        // dynamic membership didn't cover all members -> extract from group-tree
        Iterator<Tree> trees = getMembershipProvider().getMembers(getTree(), includeInherited);
        if (!trees.hasNext()) {
            return dynamicMembers;
        }
        
        AuthorizableIterator members = AuthorizableIterator.create(trees, userMgr, AuthorizableType.AUTHORIZABLE);
        AuthorizableIterator allMembers = AuthorizableIterator.create(true, dynamicMembers, members);
        return new RangeIteratorAdapter(allMembers, allMembers.getSize()); 
    }

    /**
     * Internal implementation of {@link #isDeclaredMember(Authorizable)} and {@link #isMember(Authorizable)}.
     *
     * @param authorizable The authorizable to test.
     * @param includeInherited Flag indicating if only declared or all members
     * should taken into account.
     * @return {@code true} if the specified authorizable is member or declared
     * member of this group; {@code false} otherwise.
     * @throws RepositoryException If an error occurs.
     */
    private boolean isMember(@NotNull Authorizable authorizable, boolean includeInherited) throws RepositoryException {
        if (!isValidAuthorizableImpl(authorizable)) {
            return false;
        }
        if (getID().equals(authorizable.getID()) || ((AuthorizableImpl) authorizable).isEveryone()) {
            return false;
        }

        DynamicMembershipProvider dmp = getUserManager().getDynamicMembershipProvider();
        if (dmp.isMember(this, authorizable, includeInherited)) {
            return true;
        }

        // no dynamic membership -> regular membership provider needs to evaluate
        Tree authorizableTree = ((AuthorizableImpl) authorizable).getTree();
        MembershipProvider mgr = getUserManager().getMembershipProvider();
        if (includeInherited) {
            return mgr.isMember(this.getTree(), authorizableTree);
        } else {
            return mgr.isDeclaredMember(this.getTree(), authorizableTree);
        }
    }

    @NotNull
    private Set<String> updateMembersMonitored(boolean isRemove, @NotNull String... memberIds) throws RepositoryException {
        Stopwatch watch = Stopwatch.createStarted();
        Set<String> failed = updateMembers(isRemove, memberIds);
        getMonitor().doneUpdateMembers(watch.elapsed(NANOSECONDS), memberIds.length, failed.size(), isRemove);
        return failed;
    }

    /**
     * Internal method to add or remove members by ID.
     *
     * @param isRemove Boolean flag indicating if members should be added or removed.
     * @param memberIds The {@code memberIds} to be added or removed.
     * @return The sub-set of {@code memberIds} that could not be added/removed.
     * @throws javax.jcr.nodetype.ConstraintViolationException If any of the specified
     * IDs is empty string or null or if {@link org.apache.jackrabbit.oak.spi.xml.ImportBehavior#ABORT}
     * is configured and an ID cannot be resolved to an existing (or accessible)
     * authorizable.
     * @throws javax.jcr.RepositoryException If another error occurs.
     */
    @NotNull
    private Set<String> updateMembers(boolean isRemove, @NotNull String... memberIds) throws RepositoryException {
        Set<String> failedIds = Sets.newHashSet(memberIds);
        int importBehavior = UserUtil.getImportBehavior(getUserManager().getConfig());

        DynamicMembershipProvider dmp = getUserManager().getDynamicMembershipProvider();
        if (dmp.coversAllMembers(this)) {
            String msg = "Attempt to add to or remove from dynamic group {}.";
            log.debug(msg, getID());
            return failedIds;
        }

        // calculate the contentID for each memberId and remember ids that cannot be processed
        Map<String, String> updateMap = Maps.newHashMapWithExpectedSize(memberIds.length);
        MembershipProvider mp = getMembershipProvider();
        for (String memberId : memberIds) {
            if (Strings.isNullOrEmpty(memberId)) {
                throw new ConstraintViolationException("MemberId must not be null or empty.");
            }
            if (isValidMemberId(memberId, importBehavior)) {
                // memberId can be processed -> remove from failedIds and generate contentID
                failedIds.remove(memberId);
                updateMap.put(mp.getContentID(memberId), memberId);
            }
        }

        Set<String> processedIds = Sets.newHashSet(updateMap.values());
        if (!updateMap.isEmpty()) {
            Set<String> result;
            if (isRemove) {
                result = mp.removeMembers(getTree(), updateMap);
            } else {
                result = mp.addMembers(getTree(), updateMap);
            }
            failedIds.addAll(result);
            processedIds.removeAll(result);
        }

        getUserManager().onGroupUpdate(this, isRemove, false, processedIds, failedIds);
        return failedIds;
    }

    private boolean isValidMemberId(@NotNull String memberId, int importBehavior) throws RepositoryException {
        if (getID().equals(memberId)) {
            log.debug("Attempt to add or remove a group as member of itself ({}).", getID());
            return false;
        }

        if (ImportBehavior.BESTEFFORT != importBehavior) {
            Authorizable member = getUserManager().getAuthorizable(memberId);
            String msg = null;
            if (member == null) {
                msg = "Attempt to add or remove a non-existing member '" + memberId + "' with ImportBehavior = " + ImportBehavior.nameFromValue(importBehavior);
            } else if (member.isGroup()) {
                if (((AuthorizableImpl) member).isEveryone()) {
                    log.debug("Attempt to add everyone group as member.");
                    return false;
                } else if (isCyclicMembership((Group) member)) {
                    msg = "Cyclic group membership detected for group " + getID() + " and member " + member.getID();
                }
            }
            if (msg != null) {
                if (ImportBehavior.ABORT == importBehavior) {
                    throw new ConstraintViolationException(msg);
                } else {
                    // ImportBehavior.IGNORE is default in UserUtil.getImportBehavior
                    log.debug(msg);
                    return false;
                }
            }
        }
        return true;
    }

    private boolean isCyclicMembership(@NotNull Group member) throws RepositoryException {
        return member.isMember(this);
    }

    /**
     * Principal representation of this group instance.
     */
    private final class GroupPrincipal extends AbstractGroupPrincipal {

        private GroupPrincipal(String principalName, Tree groupTree) {
            super(principalName, groupTree, GroupImpl.this.getUserManager().getNamePathMapper());
        }

        @Override
        UserManager getUserManager() {
            return GroupImpl.this.getUserManager();
        }

        @Override
        boolean isEveryone() {
            return GroupImpl.this.isEveryone();
        }

        @Override
        boolean isMember(@NotNull Authorizable authorizable) throws RepositoryException {
            return GroupImpl.this.isMember(authorizable);
        }

        @NotNull
        @Override
        Iterator<Authorizable> getMembers() throws RepositoryException {
            return GroupImpl.this.getMembers();
        }
    }
}
